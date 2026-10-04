mod common;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::observability::{
    TestLogRecorder, assert_log_emitted, assert_log_emitted_timeout, assert_with_logs,
    expect_with_logs,
};

use std::sync::atomic::{AtomicBool, Ordering};

use chzzk_load::chzzk::models::LiveDetail;
use chzzk_load::chzzk::models_metadata::StreamMetadataState;
use chzzk_load::chzzk::source::MockLiveStreamSource;
use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::engine::{
    ActiveSessionState, ChannelLifecycleState, EngineOrchestrator, RestrictionReason,
};
use chzzk_load::tui::event::{AppEvent, LogEntry};
use chzzk_load::uploader::{
    BoxFuture, MockUploadBackend, ProgressCallback, UploadBackend, UploadTask, broadcast_identifier,
};

use common::mock_source::{make_close_detail, make_open_detail, make_restricted_detail};

struct ConcurrencyMockBackend {
    task2_started_tx: mpsc::Sender<()>,
    allow_task1_finish_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<()>>>,
}

impl UploadBackend for ConcurrencyMockBackend {
    fn upload_file_and_delete<'a>(
        &'a self,
        local_path: &'a Path,
        _remote_dir: &'a str,
        _on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            if file_name.contains("chan2") {
                let _ = self.task2_started_tx.send(()).await;
            } else if file_name.contains("chan1") {
                let mut rx = self.allow_task1_finish_rx.lock().await;
                let _ = rx.recv().await;
            }
            let len = tokio::fs::metadata(local_path).await?.len();
            tokio::fs::remove_file(local_path).await?;
            Ok(len)
        })
    }

    fn upload_text<'a>(
        &'a self,
        _remote_dir: &'a str,
        _file_name: &'a str,
        _content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}

struct SerialMockBackend {
    task1_in_flight: Arc<AtomicBool>,
    task1_started_tx: Option<mpsc::Sender<()>>,
    task2_unexpected_started_tx: mpsc::Sender<()>,
    allow_task1_finish_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<()>>>,
}

impl UploadBackend for SerialMockBackend {
    fn upload_file_and_delete<'a>(
        &'a self,
        local_path: &'a Path,
        _remote_dir: &'a str,
        _on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            if file_name.contains("0001") {
                if self.task1_in_flight.load(Ordering::SeqCst) {
                    let _ = self.task2_unexpected_started_tx.send(()).await;
                }
            } else if file_name.contains("0000") {
                self.task1_in_flight.store(true, Ordering::SeqCst);
                if let Some(ref tx) = self.task1_started_tx {
                    let _ = tx.send(()).await;
                }
                let mut rx = self.allow_task1_finish_rx.lock().await;
                let _ = rx.recv().await;
                self.task1_in_flight.store(false, Ordering::SeqCst);
            }
            let len = tokio::fs::metadata(local_path).await?.len();
            tokio::fs::remove_file(local_path).await?;
            Ok(len)
        })
    }

    fn upload_text<'a>(
        &'a self,
        _remote_dir: &'a str,
        _file_name: &'a str,
        _content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}

#[tokio::test]
async fn test_app_event_mpsc_channel() {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(10);
    tx.send(AppEvent::Log(LogEntry::info("Test log")))
        .await
        .unwrap();

    let received = rx.recv().await.unwrap();
    match received {
        AppEvent::Log(msg) => assert_eq!(msg, "Test log"),
        _ => panic!("Expected Log event"),
    }
}

#[tokio::test]
async fn test_all_app_event_variants_mpsc() {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(20);

    let key_event = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);

    let events = vec![
        AppEvent::Tick,
        AppEvent::Key(key_event),
        AppEvent::ChannelUpdate {
            channel_id: "chan_123".to_string(),
            channel_name: "Streamer 1".to_string(),
            is_live: true,
            title: "Live broadcast".to_string(),
        },
        AppEvent::RecordingStarted {
            channel_id: "chan_123".to_string(),
            session_title: "Session Title".to_string(),
        },
        AppEvent::ChunkSealed {
            chunk_name: "chunk_0000.ts".to_string(),
            size_bytes: 1048576,
        },
        AppEvent::UploadProgress {
            channel_id: "chan_123".to_string(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "Streamer 1".to_string(),
            uploaded_bytes: 524288,
            total_bytes: 1048576,
            speed_mb_s: 10.5,
        },
        AppEvent::UploadCompleted {
            channel_id: "chan_123".to_string(),
            chunk_name: "chunk_0000.ts".to_string(),
            reclaimed_bytes: 1048576,
        },
        AppEvent::Log(LogEntry::info("Info message")),
    ];

    for ev in &events {
        tx.send(ev.clone()).await.unwrap();
    }

    for expected in events {
        let actual = rx.recv().await.unwrap();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn test_engine_orchestrator_instantiation() {
    let settings = Settings::default();
    let mock = Arc::new(MockLiveStreamSource::new());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);
    assert!(orchestrator.active_recording_ids().is_empty());
    assert!(!orchestrator.is_recording("any_channel"));
}

#[tokio::test]
async fn test_engine_orchestrator_poll_channel_offline() {
    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("chan_offline", "OfflineStreamer")],
        ..Default::default()
    };

    let mock = Arc::new(
        MockLiveStreamSource::new()
            .with_channel_state("chan_offline", make_close_detail(Some("OfflineStreamer"))),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);
    orchestrator.poll_channels_once(&upload_tx).await;

    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::ChannelUpdate {
            channel_id,
            channel_name,
            is_live,
            title,
        } => {
            assert_eq!(channel_id, "chan_offline");
            assert_eq!(channel_name, "OfflineStreamer");
            assert!(!is_live);
            assert_eq!(title, "Offline");
        }
        other => panic!("Expected ChannelUpdate event, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_engine_orchestrator_poll_channel_error() {
    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("chan_err", "ErrorStreamer")],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.inject_channel_error("chan_err", "internal error");

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);
    orchestrator.poll_channels_once(&upload_tx).await;

    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::Log(msg) => {
            assert!(msg.starts_with("[WARN] Polling failed for chan_err"));
        }
        other => panic!("Expected Log event, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_engine_orchestrator_poll_channel_live() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_live_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_live", "LiveStreamer")],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_live",
        make_open_detail(
            "chan_live",
            "LiveStreamer",
            "Playing Games",
            12345,
            "https://mock/master.m3u8",
        ),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);

    // Poll 1: transitions to live and starts recording session
    orchestrator.poll_channels_once(&upload_tx).await;

    // ChannelUpdate event received
    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::ChannelUpdate {
            channel_id,
            channel_name,
            is_live,
            title,
        } => {
            assert_eq!(channel_id, "chan_live");
            assert_eq!(channel_name, "LiveStreamer");
            assert!(is_live);
            assert_eq!(title, "Playing Games");
        }
        other => panic!("Expected ChannelUpdate event, got: {other:?}"),
    }

    // RecordingStarted event received from the spawned session
    let event_rec = event_rx.recv().await.unwrap();
    match event_rec {
        AppEvent::RecordingStarted {
            channel_id,
            session_title,
        } => {
            assert_eq!(channel_id, "chan_live");
            assert_eq!(session_title, "Playing Games");
        }
        other => panic!("Expected RecordingStarted event, got: {other:?}"),
    }

    // Channel is marked active
    assert!(orchestrator.is_recording("chan_live"));
    assert!(
        orchestrator
            .active_recording_ids()
            .contains(&"chan_live".to_string())
    );

    // Poll 2: already recording, should emit ChannelUpdate but NOT spawn duplicate
    orchestrator.poll_channels_once(&upload_tx).await;
    let mut got_second_channel_update = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), event_rx.recv()).await
    {
        if let AppEvent::ChannelUpdate {
            channel_id,
            is_live,
            ..
        } = ev
        {
            assert_eq!(channel_id, "chan_live");
            assert!(is_live);
            got_second_channel_update = true;
            break;
        }
    }
    assert!(
        got_second_channel_update,
        "Expected second ChannelUpdate event"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_prevents_duplicate_session_race_condition() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_race_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 60,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_race", "StreamerRace")],
        ..Default::default()
    };

    let state1 = make_open_detail(
        "chan_race",
        "StreamerRace",
        "Stream A",
        21212268,
        "https://mock/master.m3u8",
    );
    let state2 = state1.clone();
    let state3 = make_close_detail(Some("StreamerRace"));
    let state4 = make_open_detail(
        "chan_race",
        "StreamerRace",
        "Stream B (New)",
        21212269,
        "https://mock/master.m3u8",
    );

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states("chan_race", vec![state1, state2, state3, state4]);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);

    // --- Poll 1: Stream goes live -> starts session 1 ---
    orchestrator.poll_channels_once(&upload_tx).await;

    // Collect ChannelUpdate and RecordingStarted for session 1
    let mut recording_started_count = 0;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            assert_eq!(channel_id, "chan_race");
            recording_started_count += 1;
        }
    }
    assert_eq!(recording_started_count, 1, "Session 1 should have started");

    // Simulate session 1 ending: finished_sessions records it
    // In actual app, this happens when FFmpeg exits
    orchestrator
        .register_finished_session("chan_race", Some(21212268))
        .await;

    // --- Poll 2: API still returns OPEN with liveId 21212268 (stale CDN cache) ---
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify NO second recording session was started!
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { .. } = ev {
            panic!(
                "BUG: A duplicate recording session was spawned for the same liveId / during cooldown!"
            );
        }
    }
    assert!(
        !orchestrator.is_recording("chan_race"),
        "Channel must not be in active_recordings"
    );

    // --- Poll 3: API transitions to CLOSE (channel offline) ---
    orchestrator.poll_channels_once(&upload_tx).await;
    let mut offline_detected = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
    {
        if let AppEvent::ChannelUpdate { is_live, .. } = ev
            && !is_live
        {
            offline_detected = true;
        }
    }
    assert!(
        offline_detected,
        "Channel should be marked offline upon CLOSE"
    );

    // --- Poll 4: Genuinely new stream starts with new liveId 21212269 ---
    orchestrator.poll_channels_once(&upload_tx).await;
    let mut session_2_started = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { session_title, .. } = ev {
            assert_eq!(session_title, "Stream B (New)");
            session_2_started = true;
        }
    }
    assert!(
        session_2_started,
        "Session 2 should start for new liveId 21212269"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_upload_consumer() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_upload_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle =
        EngineOrchestrator::spawn_upload_consumer(Some(mock_backend.clone()), event_tx, upload_rx);

    // Create a local chunk file
    let chunk_path = temp_dir.join("chunk_0000.ts");
    {
        let mut f = File::create(&chunk_path).unwrap();
        f.write_all(&vec![0u8; 1024 * 1024]).unwrap(); // 1MB
    }
    assert!(chunk_path.exists());

    // Submit upload task
    upload_tx
        .send(UploadTask {
            channel_id: "chan_test".to_string(),
            session_folder_id: "folder_abc".to_string(),
            remote_dir: "folder_abc".to_string(),
            chunk_path: chunk_path.clone(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "Streamer 1".to_string(),
        })
        .await
        .unwrap();

    // Consume events until UploadCompleted
    let mut got_completed = false;
    let mut got_clean_log = false;
    let mut recorder = TestLogRecorder::new();

    while let Some(ev) = event_rx.recv().await {
        recorder.record(&ev);
        match ev {
            AppEvent::UploadProgress {
                chunk_name,
                uploaded_bytes,
                total_bytes,
                ..
            } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(total_bytes, 1024 * 1024);
                assert!(uploaded_bytes > 0);
            }
            AppEvent::UploadCompleted {
                chunk_name,
                reclaimed_bytes,
                ..
            } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(reclaimed_bytes, 1024 * 1024);
                got_completed = true;
            }
            AppEvent::Log(msg) if msg.contains("Uploaded & deleted chunk_0000.ts") => {
                got_clean_log = true;
                break;
            }
            _ => {}
        }
    }

    assert_with_logs(
        got_completed,
        "Must receive UploadCompleted event",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        got_clean_log,
        "Must receive clean log",
        &mut event_rx,
        Some(&recorder),
    );
    // File must have been deleted locally
    assert!(!chunk_path.exists());

    let uploads = mock_backend.uploads.lock().await;
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, chunk_path);
    assert_eq!(uploads[0].1, "folder_abc");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_upload_consumer_handles_failure() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());
    mock_backend.should_fail.store(true, Ordering::SeqCst);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle =
        EngineOrchestrator::spawn_upload_consumer(Some(mock_backend), event_tx, upload_rx);

    // Create a local chunk file
    let chunk_path = temp_dir.join("chunk_fail.ts");
    {
        let mut f = File::create(&chunk_path).unwrap();
        f.write_all(b"important video content").unwrap();
    }
    assert!(chunk_path.exists());

    // Submit upload task
    upload_tx
        .send(UploadTask {
            channel_id: "chan_fail".to_string(),
            session_folder_id: "folder_xyz".to_string(),
            remote_dir: "folder_xyz".to_string(),
            chunk_path: chunk_path.clone(),
            chunk_name: "chunk_fail.ts".to_string(),
            streamer_name: "StreamerFail".to_string(),
        })
        .await
        .unwrap();

    // Expect an error log event
    let mut got_error_log = false;
    let mut recorder = TestLogRecorder::new();
    while let Some(ev) = event_rx.recv().await {
        recorder.record(&ev);
        if let AppEvent::Log(msg) = ev
            && msg.contains("Upload failed for chunk_fail.ts")
        {
            got_error_log = true;
            break;
        }
    }

    assert_with_logs(
        got_error_log,
        "Expected error log for chunk_fail.ts",
        &mut event_rx,
        Some(&recorder),
    );
    // File MUST be preserved upon failure
    assert!(chunk_path.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown() {
    use tokio_util::sync::CancellationToken;

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            poll_interval_seconds: 60,
            ..Default::default()
        },
        channels: vec![],
        ..Default::default()
    };
    let chzzk = Arc::new(MockLiveStreamSource::new());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.run());

    // Give run loop a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Trigger graceful shutdown
    cancel_token.cancel();

    // Await run_handle with timeout
    let res = tokio::time::timeout(std::time::Duration::from_secs(3), run_handle).await;
    assert!(res.is_ok(), "Engine did not shut down within timeout");

    assert_log_emitted(&mut event_rx, "Engine graceful shutdown complete.");
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown_with_active_session() {
    use tokio_util::sync::CancellationToken;

    let temp_dir = std::env::temp_dir().join(format!(
        "test_orch_shutdown_active_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_shutdown",
            "ShutdownStreamer",
        )],
        ..Default::default()
    };
    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_shutdown",
        make_open_detail(
            "chan_shutdown",
            "ShutdownStreamer",
            "Live for shutdown test",
            123456,
            "https://mock/master.m3u8",
        ),
    ));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(settings, mock, None, event_tx, cancel_token.clone())
            .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait for RecordingStarted event
    let mut recording_started = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            assert_eq!(channel_id, "chan_shutdown");
            recording_started = true;
            break;
        }
    }
    assert!(recording_started, "Recording should have started");

    // Cancel engine token while actively recording
    cancel_token.cancel();

    // In background, drain event_rx like main.rs should do during shutdown
    let drain_handle = tokio::spawn(async move {
        let mut got_ended = false;
        while let Ok(Some(ev)) =
            tokio::time::timeout(std::time::Duration::from_secs(15), event_rx.recv()).await
        {
            if let AppEvent::RecordingEnded { channel_id } = ev
                && channel_id == "chan_shutdown"
            {
                got_ended = true;
                break;
            }
        }
        got_ended
    });

    // Await run_handle with 15-second timeout (allowing for child process wait & cleanup)
    let res = tokio::time::timeout(std::time::Duration::from_secs(15), run_handle).await;
    assert!(
        res.is_ok(),
        "Engine did not shut down cleanly during active recording!"
    );

    let got_ended = drain_handle.await.unwrap_or(false);
    assert!(
        got_ended,
        "RecordingEnded event should be emitted upon shutdown"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_manual_refresh() {
    use tokio_util::sync::CancellationToken;

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            poll_interval_seconds: 3600, // 1 hour interval
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_refresh", "RefreshStreamer")],
        ..Default::default()
    };
    let mock = Arc::new(
        MockLiveStreamSource::new()
            .with_channel_state("chan_refresh", make_close_detail(Some("RefreshStreamer"))),
    );
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        mock.clone(),
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait until initial poll completes
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if mock.call_count("chan_refresh") >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        mock.call_count("chan_refresh"),
        1,
        "Initial poll must execute"
    );

    // Clear event queue
    while event_rx.try_recv().is_ok() {}

    // Trigger manual refresh
    orchestrator.trigger_refresh();

    // Second poll must happen quickly despite 1 hour sleep interval
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if mock.call_count("chan_refresh") >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        mock.call_count("chan_refresh"),
        2,
        "Manual refresh poll must execute"
    );

    // Clean up
    cancel_token.cancel();
    let _ = run_handle.await;
}

#[tokio::test]
async fn test_engine_orchestrator_concurrent_uploads() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_concurrent_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let (task2_started_tx, mut task2_started_rx) = mpsc::channel::<()>(1);
    let (allow_task1_finish_tx, allow_task1_finish_rx) = mpsc::channel::<()>(1);

    let backend: Arc<dyn UploadBackend> = Arc::new(ConcurrencyMockBackend {
        task2_started_tx,
        allow_task1_finish_rx: Arc::new(tokio::sync::Mutex::new(allow_task1_finish_rx)),
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer_with_concurrency(
        Some(backend),
        event_tx,
        upload_rx,
        2,
    );

    let chunk_path1 = temp_dir.join("chunk_chan1.ts");
    let chunk_path2 = temp_dir.join("chunk_chan2.ts");
    fs::write(&chunk_path1, b"chan1_video_data").unwrap();
    fs::write(&chunk_path2, b"chan2_video_data").unwrap();

    upload_tx
        .send(UploadTask {
            channel_id: "chan_1".to_string(),
            session_folder_id: "folder_1".to_string(),
            remote_dir: "folder_1".to_string(),
            chunk_path: chunk_path1.clone(),
            chunk_name: "chunk_chan1.ts".to_string(),
            streamer_name: "Streamer 1".to_string(),
        })
        .await
        .unwrap();

    upload_tx
        .send(UploadTask {
            channel_id: "chan_2".to_string(),
            session_folder_id: "folder_2".to_string(),
            remote_dir: "folder_2".to_string(),
            chunk_path: chunk_path2.clone(),
            chunk_name: "chunk_chan2.ts".to_string(),
            streamer_name: "Streamer 2".to_string(),
        })
        .await
        .unwrap();

    // Verify task 2 starts while task 1 is still in flight
    let task2_started =
        tokio::time::timeout(std::time::Duration::from_secs(3), task2_started_rx.recv()).await;
    assert!(
        task2_started.is_ok() && task2_started.unwrap().is_some(),
        "Task 2 did not start concurrently while Task 1 was in flight!"
    );

    // Allow task 1 to finish
    let _ = allow_task1_finish_tx.send(()).await;

    // Await both upload completions
    let mut completed_chunks = Vec::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::UploadCompleted { chunk_name, .. } = ev {
            completed_chunks.push(chunk_name);
            if completed_chunks.len() == 2 {
                break;
            }
        }
    }

    assert_eq!(completed_chunks.len(), 2);
    assert!(
        !chunk_path1.exists(),
        "chunk 1 must be deleted after upload"
    );
    assert!(
        !chunk_path2.exists(),
        "chunk 2 must be deleted after upload"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_stream_metadata_change_uploads_metadata_jsonl() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_rename_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_rename", "RenameStreamer")],
        ..Default::default()
    };

    let initial_state = make_open_detail(
        "chan_rename",
        "RenameStreamer",
        "Initial Stream Title",
        999111,
        "https://mock/master.m3u8",
    );
    let updated_state = make_open_detail(
        "chan_rename",
        "RenameStreamer",
        "Updated Stream Title? Playing Now?",
        999111,
        "https://mock/master.m3u8",
    );

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states("chan_rename", vec![initial_state, updated_state]);

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator =
        EngineOrchestrator::new(settings, mock, Some(mock_backend.clone()), event_tx);

    let initial_meta = chzzk_load::chzzk::models_metadata::StreamMetadataState {
        live_id: Some(999111),
        channel_id: "chan_rename".to_string(),
        channel_name: "RenameStreamer".to_string(),
        live_title: "Initial Stream Title".to_string(),
        ..Default::default()
    };

    // Initialize active recording state
    orchestrator.register_active_session(
        "chan_rename",
        chzzk_load::engine::ActiveSessionState::new(
            "2026-09-22_1000".to_string(),
            "RenameStreamer".to_string(),
            Some("RenameStreamer".to_string()),
            initial_meta,
        ),
    );

    // Poll 1: Channel is polled with same initial title (no metadata upload expected)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(mock_backend.texts.lock().await.is_empty());

    // Poll 2: Streamer changed title to "Updated Stream Title? Playing Now?"
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify backend received metadata.jsonl upload
    let texts = mock_backend.texts.lock().await;
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].1, "metadata.jsonl");
    let content = &texts[0].2;
    assert!(content.contains("\"INITIAL_STATE\""));
    assert!(content.contains("\"stream_offset_ms\":0"));
    assert!(content.contains("\"METADATA_CHANGED\""));
    assert!(content.contains("Initial Stream Title"));
    assert!(content.contains("Updated Stream Title? Playing Now?"));

    // Verify ChannelUpdate event had new title
    let mut got_updated_title_event = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::ChannelUpdate { title, .. } = ev
            && title == "Updated Stream Title? Playing Now?"
        {
            got_updated_title_event = true;
        }
    }
    assert!(
        got_updated_title_event,
        "Expected ChannelUpdate with Updated Stream Title"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_stream_metadata_change_updates_metadata_jsonl_file() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_history_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_rename", "RenameStreamer")],
        ..Default::default()
    };

    let initial_state = make_open_detail(
        "chan_rename",
        "RenameStreamer",
        "Initial Stream Title",
        999111,
        "https://mock/master.m3u8",
    );
    let updated_state = make_open_detail(
        "chan_rename",
        "RenameStreamer",
        "Updated Stream Title? Playing Now?",
        999111,
        "https://mock/master.m3u8",
    );

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states("chan_rename", vec![initial_state, updated_state]);

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator =
        EngineOrchestrator::new(settings, mock, Some(mock_backend.clone()), event_tx);

    let initial_meta = chzzk_load::chzzk::models_metadata::StreamMetadataState {
        live_id: Some(999111),
        channel_id: "chan_rename".to_string(),
        channel_name: "RenameStreamer".to_string(),
        live_title: "Initial Stream Title".to_string(),
        ..Default::default()
    };

    let session_folder = "[2026-09-22_1000] [RenameStreamer] RenameStreamer - Initial Stream Title";
    let session_dir = temp_dir.join(session_folder);
    fs::create_dir_all(&session_dir).unwrap();
    let initial_jsonl = format!(
        "{}\n",
        serde_json::to_string(&chzzk_load::chzzk::models_metadata::MetadataEvent {
            version: 2,
            event: chzzk_load::chzzk::models_metadata::MetadataEventType::InitialState,
            timestamp: "2026-09-22T10:00:00Z".to_string(),
            stream_offset_ms: 0,
            state: initial_meta.clone(),
        })
        .unwrap()
    );
    fs::write(session_dir.join("metadata.jsonl"), &initial_jsonl).unwrap();

    orchestrator.register_active_session(
        "chan_rename",
        chzzk_load::engine::ActiveSessionState::new(
            "2026-09-22_1000".to_string(),
            "RenameStreamer".to_string(),
            Some("RenameStreamer".to_string()),
            initial_meta,
        ),
    );

    // Poll 1: Channel is polled with same initial title (no history upload)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(mock_backend.texts.lock().await.is_empty());

    // Poll 2: Streamer changed title to "Updated Stream Title? Playing Now?"
    orchestrator.poll_channels_once(&upload_tx).await;

    let texts = mock_backend.texts.lock().await;
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].1, "metadata.jsonl");
    let content = &texts[0].2;
    assert!(content.contains("\"INITIAL_STATE\""));
    assert!(content.contains("Initial Stream Title"));
    assert!(content.contains("\"METADATA_CHANGED\""));
    assert!(content.contains("Updated Stream Title? Playing Now?"));

    // Verify local file exists and has both JSON Lines
    let local_file = fs::read_to_string(session_dir.join("metadata.jsonl")).unwrap();
    let local_lines: Vec<&str> = local_file.trim().lines().collect();
    assert_eq!(local_lines.len(), 2);
    assert!(local_lines[0].contains("\"INITIAL_STATE\""));
    assert!(local_lines[1].contains("\"METADATA_CHANGED\""));
    assert!(local_lines[1].contains("Updated Stream Title? Playing Now?"));

    let session = orchestrator
        .active_session("chan_rename")
        .expect("Session must exist");
    assert_eq!(session.metadata_history.len(), 2);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_stream_title_change_before_folder_creation() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_pre_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_pre", "PreStreamer")],
        ..Default::default()
    };

    let initial_state = make_open_detail(
        "chan_pre",
        "PreStreamer",
        "Early Title 1",
        555666,
        "https://mock/master.m3u8",
    );
    let updated_state = make_open_detail(
        "chan_pre",
        "PreStreamer",
        "Early Title 2? Pending?",
        555666,
        "https://mock/master.m3u8",
    );

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states("chan_pre", vec![initial_state, updated_state]);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);

    let initial_meta = StreamMetadataState {
        live_id: Some(555666),
        channel_id: "chan_pre".to_string(),
        channel_name: "PreStreamer".to_string(),
        live_title: "Early Title 1".to_string(),
        ..Default::default()
    };

    orchestrator.register_active_session(
        "chan_pre",
        chzzk_load::engine::ActiveSessionState::new(
            "2026-09-22_1000".to_string(),
            "PreStreamer".to_string(),
            Some("PreStreamer".to_string()),
            initial_meta,
        ),
    );

    // Poll 1: Session starts with Early Title 1 (already active, no title change)
    orchestrator.poll_channels_once(&upload_tx).await;

    let session = orchestrator
        .active_session("chan_pre")
        .expect("Session should exist");
    assert_eq!(session.current_title, "Early Title 1");

    // Poll 2: Title changes to Early Title 2
    orchestrator.poll_channels_once(&upload_tx).await;

    let session = orchestrator
        .active_session("chan_pre")
        .expect("Session should exist");
    assert_eq!(session.current_title, "Early Title 2? Pending?");
    assert_eq!(session.metadata_history.len(), 2);

    assert_log_emitted(&mut event_rx, "[chan_pre] Stream metadata changed");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_stream_category_and_watch_party_metadata_transition() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_trans_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_trans", "TransStreamer")],
        ..Default::default()
    };

    let mut s1 = make_open_detail(
        "chan_trans",
        "TransStreamer",
        "Just Chatting",
        888111,
        "https://mock/master.m3u8",
    );
    if let LiveDetail::Open(ref mut info) = s1 {
        info.metadata.category_type = Some(chzzk_load::chzzk::models_metadata::CategoryType::Talk);
        info.metadata.live_category = Some("talk".to_string());
        info.metadata.live_category_value = Some("Just Chatting".to_string());
    }

    let mut s2 = make_open_detail(
        "chan_trans",
        "TransStreamer",
        "Watch Party Asian Games!",
        888111,
        "https://mock/master.m3u8",
    );
    if let LiveDetail::Open(ref mut info) = s2 {
        info.metadata.category_type = Some(chzzk_load::chzzk::models_metadata::CategoryType::Game);
        info.metadata.live_category = Some("game".to_string());
        info.metadata.live_category_value = Some("Valorant".to_string());
        info.metadata.is_watch_party = true;
    }

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states("chan_trans", vec![s1, s2]);

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator =
        EngineOrchestrator::new(settings, mock, Some(mock_backend.clone()), event_tx);

    let initial_meta = chzzk_load::chzzk::models_metadata::StreamMetadataState {
        live_id: Some(888111),
        channel_id: "chan_trans".to_string(),
        channel_name: "TransStreamer".to_string(),
        live_title: "Just Chatting".to_string(),
        category_type: Some(chzzk_load::chzzk::models_metadata::CategoryType::Talk),
        live_category: Some("talk".to_string()),
        live_category_value: Some("Just Chatting".to_string()),
        ..Default::default()
    };

    orchestrator.register_active_session(
        "chan_trans",
        chzzk_load::engine::ActiveSessionState::new(
            "2026-09-30_1400".to_string(),
            "TransStreamer".to_string(),
            Some("TransStreamer".to_string()),
            initial_meta,
        ),
    );

    // Poll 1: Channel polled with same initial metadata
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(mock_backend.texts.lock().await.is_empty());

    // Poll 2: Channel metadata changes to Game + Watch Party
    orchestrator.poll_channels_once(&upload_tx).await;

    let texts = mock_backend.texts.lock().await;
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].1, "metadata.jsonl");
    let content = &texts[0].2;
    assert!(content.contains("\"METADATA_CHANGED\""));
    assert!(content.contains("Valorant"));
    assert!(content.contains("\"is_watch_party\":true"));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_cleanup_empty_session_dirs_removes_empty_and_preserves_non_empty() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_cleanup_empty_session_dirs_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let empty_dir_1 = temp_dir.join("ch1_20260922_100000");
    let empty_dir_2 = temp_dir.join("ch2_20260922_110000");
    let non_empty_dir = temp_dir.join("ch3_20260922_120000");
    let regular_file = temp_dir.join("notes.txt");

    fs::create_dir_all(&empty_dir_1).unwrap();
    fs::create_dir_all(&empty_dir_2).unwrap();
    fs::create_dir_all(&non_empty_dir).unwrap();
    fs::write(non_empty_dir.join("chunk_0000.ts"), b"test_stream_data").unwrap();
    fs::write(&regular_file, b"standalone file").unwrap();

    let removed = EngineOrchestrator::cleanup_empty_session_dirs(&temp_dir)
        .await
        .expect("cleanup should succeed");

    assert_eq!(removed, 2, "Expected exactly 2 empty session dirs removed");
    assert!(!empty_dir_1.exists(), "empty_dir_1 should be removed");
    assert!(!empty_dir_2.exists(), "empty_dir_2 should be removed");
    assert!(non_empty_dir.exists(), "non_empty_dir should still exist");
    assert!(
        non_empty_dir.join("chunk_0000.ts").exists(),
        "chunk inside non_empty_dir should still exist"
    );
    assert!(regular_file.exists(), "regular file should not be removed");
    assert!(
        temp_dir.exists(),
        "recordings_dir itself should still exist"
    );

    // Calling on non-existent directory should return Ok(0) safely
    let non_existent = temp_dir.join("does_not_exist");
    let removed_none = EngineOrchestrator::cleanup_empty_session_dirs(&non_existent)
        .await
        .expect("nonexistent dir should return Ok(0)");
    assert_eq!(removed_none, 0);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown_cleans_empty_session_dirs() {
    use tokio_util::sync::CancellationToken;

    let temp_dir = std::env::temp_dir().join(format!(
        "test_orch_shutdown_clean_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let session_empty = temp_dir.join("chan_empty_20260922_100000");
    let session_non_empty = temp_dir.join("chan_data_20260922_100000");

    fs::create_dir_all(&session_empty).unwrap();
    fs::create_dir_all(&session_non_empty).unwrap();
    fs::write(session_non_empty.join("chunk_0000.ts"), b"test").unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            ..Default::default()
        },
        channels: vec![],
        ..Default::default()
    };
    let chzzk = Arc::new(MockLiveStreamSource::new());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.run());

    // Give run loop a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Trigger graceful shutdown
    cancel_token.cancel();

    // Await run_handle
    let res = tokio::time::timeout(std::time::Duration::from_secs(3), run_handle).await;
    assert!(res.is_ok(), "Engine did not shut down within timeout");

    // Assert empty session folder was deleted
    assert!(
        !session_empty.exists(),
        "Empty session directory must be deleted on shutdown"
    );
    // Assert non-empty session folder remains
    assert!(
        session_non_empty.exists(),
        "Non-empty session directory must be preserved"
    );

    assert_log_emitted(&mut event_rx, "empty session folder");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_active_session_state_folder_name_sanitizes_question_marks() {
    let session = ActiveSessionState {
        start_timestamp: "2026-09-22_2200".to_string(),
        streamer_name: "Streamer?Name".to_string(),
        initial_title: "Is this live? Yes! Special: 100% <Stream>".to_string(),
        current_title: "Is this live? Yes! Special: 100% <Stream>".to_string(),
        ..Default::default()
    };

    let folder_name = session.folder_name();
    assert_eq!(
        folder_name,
        "[2026-09-22_2200] Streamer_Name - Is this live_ Yes! Special_ 100% _Stream_"
    );
    assert!(folder_name.contains("Streamer_Name"));
    assert!(folder_name.contains("Is this live_ Yes!"));
    assert!(!folder_name.contains('?'));
    assert!(!folder_name.contains(':'));
    assert!(!folder_name.contains('<'));
    assert!(!folder_name.contains('>'));
}

#[test]
fn test_active_session_state_folder_name_formatting_and_sanitization() {
    let session = ActiveSessionState {
        start_timestamp: "2026-09-22_1530".to_string(),
        streamer_name: "  Chzzk Streamer / Channel  ".to_string(),
        initial_title: "What's Next? Let's Play | Ep. 1 *Final*".to_string(),
        current_title: "What's Next? Let's Play | Ep. 1 *Final*".to_string(),
        ..Default::default()
    };

    let folder_name = session.folder_name();
    assert_eq!(
        folder_name,
        "[2026-09-22_1530] Chzzk Streamer _ Channel - What's Next_ Let's Play _ Ep. 1 _Final_"
    );
}

#[tokio::test]
async fn test_engine_orchestrator_resumes_recording_after_cooldown_for_interrupted_stream() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_resume_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 1, // 1 second cooldown
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_interrupt",
            "StreamerInterrupt",
        )],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_interrupt",
        make_open_detail(
            "chan_interrupt",
            "StreamerInterrupt",
            "Ongoing Stream After Interruption",
            888999,
            "https://mock/master.m3u8",
        ),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, mock, None, event_tx);

    // Simulate session interrupted in the past (liveId: 888999, cooldown: 1s)
    orchestrator
        .register_finished_session("chan_interrupt", Some(888999))
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Poll channels: Since cooldown (1s) has passed and stream is still OPEN, it should resume recording!
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify recording was resumed/started!
    let mut recording_started = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev
            && channel_id == "chan_interrupt"
        {
            recording_started = true;
            break;
        }
    }

    assert!(
        recording_started,
        "Engine must resume recording when stream is still OPEN after cooldown, even with same liveId!"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown_awaits_in_progress_upload() {
    use tokio_util::sync::CancellationToken;

    let temp_dir = std::env::temp_dir().join(format!(
        "test_orch_shutdown_upload_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_upload_shutdown",
            "ShutdownUploader",
        )],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_upload_shutdown",
        make_open_detail(
            "chan_upload_shutdown",
            "ShutdownUploader",
            "Live for upload shutdown test",
            998877,
            "https://mock/master.m3u8",
        ),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings,
            mock,
            Some(mock_backend.clone()),
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait for RecordingStarted event
    let mut recording_started = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            assert_eq!(channel_id, "chan_upload_shutdown");
            recording_started = true;
            break;
        }
    }
    assert!(recording_started, "Recording should have started");

    // Locate the active session folder in temp_dir and create chunk_0000.ts
    let mut session_folder = None;
    for _ in 0..20 {
        if let Ok(entries) = fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    session_folder = Some(entry.path());
                    break;
                }
            }
        }
        if session_folder.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let session_dir = expect_with_logs(
        session_folder,
        "Session directory must exist",
        &mut event_rx,
        None,
    );
    let chunk_path = session_dir.join("chunk_0000.ts");
    fs::write(&chunk_path, b"TEST_CHUNK_PAYLOAD_DATA_FOR_SHUTDOWN_TEST").unwrap();
    assert!(chunk_path.exists());

    // Cancel engine token while actively recording
    cancel_token.cancel();

    // Drain events in background and track UploadCompleted
    let drain_handle = tokio::spawn(async move {
        let mut recorder = TestLogRecorder::new();
        let mut got_completed = false;
        while let Ok(Some(ev)) =
            tokio::time::timeout(std::time::Duration::from_secs(20), event_rx.recv()).await
        {
            recorder.record(&ev);
            if let AppEvent::UploadCompleted { chunk_name, .. } = ev
                && chunk_name == "chunk_0000.ts"
            {
                got_completed = true;
                break;
            }
        }
        recorder.drain_buffered(&mut event_rx);
        (got_completed, recorder)
    });

    let res = tokio::time::timeout(std::time::Duration::from_secs(10), run_handle).await;
    assert!(
        res.is_ok(),
        "Engine run_handle timed out before completing graceful shutdown!"
    );

    assert!(
        !chunk_path.exists(),
        "Chunk file must already be uploaded and deleted when EngineOrchestrator::run completes!"
    );

    let (got_completed, recorder) = drain_handle
        .await
        .unwrap_or_else(|_| (false, TestLogRecorder::new()));
    assert!(
        got_completed,
        "UploadCompleted event for chunk_0000.ts must be emitted during graceful shutdown! {}",
        recorder.summary()
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_serializes_uploads_per_channel() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_serial_chan_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let (task2_unexpected_started_tx, mut task2_unexpected_started_rx) = mpsc::channel::<()>(1);
    let (allow_task1_finish_tx, allow_task1_finish_rx) = mpsc::channel::<()>(1);

    let backend: Arc<dyn UploadBackend> = Arc::new(SerialMockBackend {
        task1_in_flight: Arc::new(AtomicBool::new(false)),
        task1_started_tx: None,
        task2_unexpected_started_tx,
        allow_task1_finish_rx: Arc::new(tokio::sync::Mutex::new(allow_task1_finish_rx)),
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    // Concurrency is set to 3, but both tasks belong to the SAME channel
    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer_with_concurrency(
        Some(backend),
        event_tx,
        upload_rx,
        3,
    );

    let chunk_path1 = temp_dir.join("chunk_0000.ts");
    let chunk_path2 = temp_dir.join("chunk_0001.ts");
    fs::write(&chunk_path1, b"chunk0_video_data").unwrap();
    fs::write(&chunk_path2, b"chunk1_video_data").unwrap();

    upload_tx
        .send(UploadTask {
            channel_id: "chan_same".to_string(),
            session_folder_id: "folder_same".to_string(),
            remote_dir: "folder_same".to_string(),
            chunk_path: chunk_path1.clone(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "SameStreamer".to_string(),
        })
        .await
        .unwrap();

    upload_tx
        .send(UploadTask {
            channel_id: "chan_same".to_string(),
            session_folder_id: "folder_same".to_string(),
            remote_dir: "folder_same".to_string(),
            chunk_path: chunk_path2.clone(),
            chunk_name: "chunk_0001.ts".to_string(),
            streamer_name: "SameStreamer".to_string(),
        })
        .await
        .unwrap();

    // Check if task 2 was prematurely started while task 1 is in-flight.
    let task2_started_prematurely = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        task2_unexpected_started_rx.recv(),
    )
    .await;

    assert!(
        task2_started_prematurely.is_err(),
        "Task 2 for the same channel must NOT be uploaded concurrently while Task 1 is still in flight! Chunks of the same stream must be serialized to prevent upload speed slowdown and disk accumulation."
    );

    // Allow task 1 to finish
    let _ = allow_task1_finish_tx.send(()).await;

    // Both chunks should eventually complete in sequential order
    let mut completed_chunks = Vec::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::UploadCompleted { chunk_name, .. } = ev {
            completed_chunks.push(chunk_name);
            if completed_chunks.len() == 2 {
                break;
            }
        }
    }

    assert_eq!(completed_chunks, vec!["chunk_0000.ts", "chunk_0001.ts"]);
    assert!(
        !chunk_path1.exists(),
        "chunk 0 must be deleted after upload"
    );
    assert!(
        !chunk_path2.exists(),
        "chunk 1 must be deleted after upload"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown_serializes_final_chunk_after_in_progress_upload()
 {
    use tokio_util::sync::CancellationToken;

    let temp_dir = std::env::temp_dir().join(format!(
        "test_orch_shutdown_serial_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let (task2_unexpected_started_tx, mut task2_unexpected_started_rx) = mpsc::channel::<()>(1);
    let (chunk0_in_flight_tx, mut chunk0_in_flight_rx) = mpsc::channel::<()>(1);
    let (allow_chunk0_finish_tx, allow_chunk0_finish_rx) = mpsc::channel::<()>(1);

    let backend: Arc<dyn UploadBackend> = Arc::new(SerialMockBackend {
        task1_in_flight: Arc::new(AtomicBool::new(false)),
        task1_started_tx: Some(chunk0_in_flight_tx),
        task2_unexpected_started_tx,
        allow_task1_finish_rx: Arc::new(tokio::sync::Mutex::new(allow_chunk0_finish_rx)),
    });

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_shutdown_serial",
            "ShutdownSerialStreamer",
        )],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_shutdown_serial",
        make_open_detail(
            "chan_shutdown_serial",
            "ShutdownSerialStreamer",
            "Live for shutdown serialization test",
            887766,
            "https://mock/master.m3u8",
        ),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings,
            mock,
            Some(backend),
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait for RecordingStarted event
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            assert_eq!(channel_id, "chan_shutdown_serial");
            break;
        }
    }

    // Locate active session folder
    let mut session_folder = None;
    for _ in 0..20 {
        if let Ok(entries) = fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    session_folder = Some(entry.path());
                    break;
                }
            }
        }
        if session_folder.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let session_dir = expect_with_logs(
        session_folder,
        "Session directory must exist",
        &mut event_rx,
        None,
    );
    let chunk_path0 = session_dir.join("chunk_0000.ts");
    let chunk_path1 = session_dir.join("chunk_0001.ts");

    // Write chunk_0000.ts and chunk_0001.ts to trigger N+1 sealing of chunk_0000.ts
    fs::write(&chunk_path0, b"TEST_CHUNK_0_DATA").unwrap();
    fs::write(&chunk_path1, b"TEST_CHUNK_1_DATA").unwrap();

    // Wait until chunk 0 starts uploading and reaches in-flight state
    let in_flight = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        chunk0_in_flight_rx.recv(),
    )
    .await;
    assert!(
        in_flight.is_ok(),
        "chunk_0000.ts should have started uploading and reached in-flight"
    );

    // Cancel while chunk 0 is actively uploading and chunk 1 is pending as final chunk
    cancel_token.cancel();

    // Verify chunk 1 is NOT started prematurely while chunk 0 is still in flight.
    let task2_started_prematurely = tokio::time::timeout(
        std::time::Duration::from_millis(1500),
        task2_unexpected_started_rx.recv(),
    )
    .await;

    assert!(
        task2_started_prematurely.is_err(),
        "chunk_0001.ts must NOT start uploading concurrently while chunk_0000.ts is still in-flight on termination!"
    );

    // Allow chunk 0 to finish
    let _ = allow_chunk0_finish_tx.send(()).await;

    // Await run_handle
    let res = tokio::time::timeout(std::time::Duration::from_secs(10), run_handle).await;
    assert!(res.is_ok(), "Engine run_handle should complete cleanly");

    assert!(
        !chunk_path0.exists(),
        "chunk 0 must be deleted after upload"
    );
    assert!(
        !chunk_path1.exists(),
        "chunk 1 must be deleted after upload"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_two_concurrent_live_streams() {
    use std::collections::HashSet;
    use tokio_util::sync::CancellationToken;

    let mock = Arc::new(
        MockLiveStreamSource::new()
            .with_channel_state(
                "chan_multi_1",
                make_open_detail(
                    "chan_multi_1",
                    "Streamer_1",
                    "Concurrent Stream chan_multi_1",
                    112233,
                    "https://mock/master.m3u8",
                ),
            )
            .with_channel_state(
                "chan_multi_2",
                make_open_detail(
                    "chan_multi_2",
                    "Streamer_2",
                    "Concurrent Stream chan_multi_2",
                    112233,
                    "https://mock/master.m3u8",
                ),
            ),
    );

    let temp_dir = std::env::temp_dir().join(format!("test_orch_multi_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            record_chat: false,
            ..Default::default()
        },
        channels: vec![
            ChannelConfig::with_alias("chan_multi_1", "Streamer_1"),
            ChannelConfig::with_alias("chan_multi_2", "Streamer_2"),
        ],
        ..Default::default()
    };

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings,
            mock,
            Some(mock_backend.clone()),
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait for RecordingStarted for both channels
    let mut started = HashSet::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            started.insert(channel_id);
            if started.len() == 2 {
                break;
            }
        }
    }
    assert_eq!(
        started.len(),
        2,
        "Both channels should have started recording sessions"
    );

    // Locate both session folders
    let mut session_dir_1 = None;
    let mut session_dir_2 = None;
    for _ in 0..20 {
        if let Ok(entries) = fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    let path = entry.path();
                    let name = path.file_name().unwrap().to_str().unwrap();
                    if name.contains("chan_multi_1") || name.starts_with("chan_multi_1_") {
                        session_dir_1 = Some(path.clone());
                    } else if name.contains("chan_multi_2") || name.starts_with("chan_multi_2_") {
                        session_dir_2 = Some(path.clone());
                    }
                }
            }
        }
        if session_dir_1.is_some() && session_dir_2.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let dir1 = expect_with_logs(
        session_dir_1,
        "Session dir 1 must exist",
        &mut event_rx,
        None,
    );
    let dir2 = expect_with_logs(
        session_dir_2,
        "Session dir 2 must exist",
        &mut event_rx,
        None,
    );

    // Write chunk_0000.ts and chunk_0001.ts to BOTH session folders
    let d1_c0 = dir1.join("chunk_0000.ts");
    let d1_c1 = dir1.join("chunk_0001.ts");
    fs::write(&d1_c0, b"CHUNK_1_0_DATA").unwrap();
    fs::write(&d1_c1, b"CHUNK_1_1_DATA").unwrap();

    let d2_c0 = dir2.join("chunk_0000.ts");
    let d2_c1 = dir2.join("chunk_0001.ts");
    fs::write(&d2_c0, b"CHUNK_2_0_DATA").unwrap();
    fs::write(&d2_c1, b"CHUNK_2_1_DATA").unwrap();

    // Check if chunk_0000.ts from BOTH channels completes upload
    let mut recorder = TestLogRecorder::new();
    let mut uploaded_channels = HashSet::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv()).await
    {
        recorder.record(&ev);
        if let AppEvent::UploadCompleted {
            channel_id,
            chunk_name,
            ..
        } = ev
            && chunk_name == "chunk_0000.ts"
        {
            uploaded_channels.insert(channel_id);
            if uploaded_channels.len() == 2 {
                break;
            }
        }
    }

    assert_with_logs(
        uploaded_channels.contains("chan_multi_1"),
        "chan_multi_1 chunk 0 should be uploaded",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        uploaded_channels.contains("chan_multi_2"),
        "chan_multi_2 chunk 0 should be uploaded",
        &mut event_rx,
        Some(&recorder),
    );

    let uploads = mock_backend.uploads.lock().await;
    assert!(
        uploads
            .iter()
            .any(
                |(p, _)| p.file_name().and_then(|n| n.to_str()) == Some("chunk_0000.ts")
                    && p.to_string_lossy().contains("chan_multi_1")
            ),
        "chan_multi_1 chunk_0000.ts must be recorded in mock_backend uploads"
    );
    assert!(
        uploads
            .iter()
            .any(
                |(p, _)| p.file_name().and_then(|n| n.to_str()) == Some("chunk_0000.ts")
                    && p.to_string_lossy().contains("chan_multi_2")
            ),
        "chan_multi_2 chunk_0000.ts must be recorded in mock_backend uploads"
    );
    assert!(
        !d1_c0.exists(),
        "chan_multi_1 chunk_0000.ts must be deleted locally"
    );
    assert!(
        !d2_c0.exists(),
        "chan_multi_2 chunk_0000.ts must be deleted locally"
    );

    cancel_token.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle).await;
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_recovers_and_uploads_pending_chunks() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_retry_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_retry", "StreamerRetry")],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_retry",
        make_open_detail(
            "chan_retry",
            "StreamerRetry",
            "Retry Stream",
            88881,
            "https://mock/master.m3u8",
        ),
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings,
            mock,
            Some(mock_backend.clone()),
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait for RecordingStarted
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), event_rx.recv()).await
    {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev
            && channel_id == "chan_retry"
        {
            break;
        }
    }

    // Locate session folder
    let mut session_dir = None;
    for _ in 0..20 {
        if let Ok(entries) = fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    let path = entry.path();
                    let name = path.file_name().unwrap().to_str().unwrap();
                    if name.contains("StreamerRetry") || name.starts_with("chan_retry_") {
                        session_dir = Some(path.clone());
                        break;
                    }
                }
            }
        }
        if session_dir.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let dir = expect_with_logs(session_dir, "Session dir must exist", &mut event_rx, None);

    // Write chunk_0000.ts and chunk_0001.ts
    let c0 = dir.join("chunk_0000.ts");
    let c1 = dir.join("chunk_0001.ts");
    fs::write(&c0, b"CHUNK_0_DATA").unwrap();
    fs::write(&c1, b"CHUNK_1_DATA").unwrap();

    // Check if chunk_0000.ts completes upload
    let mut chunk_0_uploaded = false;
    let mut recorder = TestLogRecorder::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) =
            tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
        {
            recorder.record(&ev);
            if let AppEvent::UploadCompleted { chunk_name, .. } = ev
                && chunk_name == "chunk_0000.ts"
            {
                chunk_0_uploaded = true;
                break;
            }
        }
    }

    assert_with_logs(
        chunk_0_uploaded,
        "chunk_0000.ts must be uploaded",
        &mut event_rx,
        Some(&recorder),
    );

    let uploads = mock_backend.uploads.lock().await;
    assert!(
        uploads
            .iter()
            .any(|(p, _)| p.file_name().and_then(|n| n.to_str()) == Some("chunk_0000.ts")),
        "chunk_0000.ts must be recorded in mock_backend uploads"
    );
    assert!(!c0.exists(), "chunk_0000.ts must be deleted locally");

    cancel_token.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle).await;
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_poll_channel_restricted_stream_sets_live_and_logs_once() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_restr_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_restricted",
            "RestrictedStreamer",
        )],
        ..Default::default()
    };

    let chzzk = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_restricted",
        make_restricted_detail(
            "chan_restricted",
            "RestrictedStreamer",
            "[19+] Midnight Broadcast",
            Some(5001),
            true,
        ),
    ));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1
    orchestrator.poll_channels_once(&upload_tx).await;
    // Poll 2
    orchestrator.poll_channels_once(&upload_tx).await;
    // Poll 3
    orchestrator.poll_channels_once(&upload_tx).await;

    let mut error_log_count = 0;
    let mut channel_updates = Vec::new();
    let mut saw_recording_started = false;

    while let Ok(event) = event_rx.try_recv() {
        match event {
            AppEvent::Log(entry) => {
                if entry
                    .message
                    .contains("Recording unavailable for channel chan_restricted")
                {
                    error_log_count += 1;
                }
            }
            AppEvent::ChannelUpdate { is_live, title, .. } => {
                channel_updates.push((is_live, title));
            }
            AppEvent::RecordingStarted { .. } => {
                saw_recording_started = true;
            }
            _ => {}
        }
    }

    assert_eq!(
        error_log_count, 1,
        "Expected exactly 1 error log across 3 polls"
    );
    assert!(
        !saw_recording_started,
        "Must not start recording session for restricted stream"
    );
    assert!(
        !channel_updates.is_empty(),
        "Must receive ChannelUpdate events"
    );
    for (is_live, title) in channel_updates {
        assert!(
            is_live,
            "Restricted stream must be marked is_live: true in TUI"
        );
        assert_eq!(title, "[19+] Midnight Broadcast");
    }
    assert!(orchestrator.is_restricted("chan_restricted"));
    assert!(orchestrator.active_recording_ids().is_empty());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_restricted_stream_recovers_to_recordable() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_recov_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_recover", "RecoverStreamer")],
        ..Default::default()
    };

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_recover",
        vec![
            // Poll 1: Restricted
            make_restricted_detail(
                "chan_recover",
                "RecoverStreamer",
                "Watch Party (Restricted)",
                Some(6001),
                false,
            ),
            // Poll 2: Becomes recordable
            make_open_detail(
                "chan_recover",
                "RecoverStreamer",
                "Watch Party (Public)",
                6001,
                "https://mock/master.m3u8",
            ),
        ],
    );
    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Restricted
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_recover"));
    assert!(orchestrator.active_recording_ids().is_empty());

    // Poll 2: Now recordable
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_recover"));
    assert!(orchestrator.is_recording("chan_recover"));

    let mut saw_started = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(event)) =
            tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
        {
            if let AppEvent::RecordingStarted { channel_id, .. } = event
                && channel_id == "chan_recover"
            {
                saw_started = true;
                break;
            }
        } else {
            break;
        }
    }
    assert!(
        saw_started,
        "RecordingStarted must be emitted once stream becomes recordable"
    );

    orchestrator.cancel();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_normal_to_restricted_to_normal_transitions_into_new_session_folder()
 {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_trans_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_bin = get_mock_ffmpeg_bin();

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_trans",
        vec![
            // Poll 1: Normal stream
            make_open_detail(
                "chan_trans",
                "TransStreamer",
                "Public Stream Part 1",
                888777,
                "https://mock/master.m3u8",
            ),
            // Poll 2: Transitions to Restricted (19+ adult without auth)
            make_restricted_detail(
                "chan_trans",
                "TransStreamer",
                "[19+] Restricted Stream Part 2",
                Some(888777),
                true,
            ),
            // Poll 3: Returns back to Normal (Public stream again)
            make_open_detail(
                "chan_trans",
                "TransStreamer",
                "Public Stream Part 3 (Resumed)",
                888777,
                "https://mock/master.m3u8",
            ),
        ],
    );

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 30, // Normal 30s cooldown
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_trans", "TransStreamer")],
        ..Default::default()
    };

    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx)
        .with_ffmpeg_bin(mock_bin.to_string_lossy());

    // --- Poll 1: Normal stream starts recording ---
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator.is_recording("chan_trans"),
        "Poll 1: Must be in active_recordings"
    );

    // Wait for session 1 directory to be created on disk
    let mut entries_after_poll1: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
    while tokio::time::Instant::now() < deadline {
        entries_after_poll1 = fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains("TransStreamer") || name.starts_with("chan_trans_"))
            .collect();
        if entries_after_poll1.len() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert_with_logs(
        entries_after_poll1.len() == 1,
        &format!("Must create exactly 1 session folder for Poll 1: {entries_after_poll1:?}"),
        &mut event_rx,
        None,
    );
    let folder_poll1 = entries_after_poll1[0].clone();
    fs::write(
        temp_dir.join(&folder_poll1).join("chunk_0000.ts"),
        b"test segment content",
    )
    .unwrap();

    // Sleep a moment to ensure timestamp clock advances
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // --- Poll 2: Stream transitions to Restricted ---
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        !orchestrator.is_recording("chan_trans"),
        "Poll 2: Must be removed from active_recordings on restriction"
    );
    assert!(
        orchestrator.is_restricted("chan_trans"),
        "Poll 2: Must be added to restricted_channels"
    );

    // Verify Session 1 FFmpeg process received termination and exited cleanly on restriction
    let mut session1_exited = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(AppEvent::RecordingEnded { ref channel_id })) =
            tokio::time::timeout(std::time::Duration::from_millis(100), event_rx.recv()).await
            && channel_id == "chan_trans"
        {
            session1_exited = true;
            break;
        }
    }
    assert_with_logs(
        session1_exited,
        "Session 1 FFmpeg process must be gracefully terminated upon entering restricted state!",
        &mut event_rx,
        None,
    );

    // Simulate restriction latch and cooldown that occurs upon session conclusion
    orchestrator
        .register_finished_session("chan_trans", Some(888777))
        .await;

    // Sleep a moment to ensure timestamp clock advances
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // --- Poll 3: Stream transitions back to Normal (same liveId) ---
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator.is_recording("chan_trans"),
        "Poll 3: Must be recording again after returning to normal"
    );
    assert!(
        !orchestrator.is_restricted("chan_trans"),
        "Poll 3: Must no longer be in restricted_channels"
    );

    // Wait for session 2 directory to be created on disk
    let mut entries_after_poll3: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
    while tokio::time::Instant::now() < deadline {
        entries_after_poll3 = fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains("TransStreamer") || name.starts_with("chan_trans_"))
            .collect();
        if entries_after_poll3.len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert_with_logs(
        entries_after_poll3.len() >= 2,
        &format!(
            "Must create a new distinct session folder for Poll 3, got: {entries_after_poll3:?}"
        ),
        &mut event_rx,
        None,
    );
    assert!(
        entries_after_poll3.iter().any(|f| f != &folder_poll1),
        "New session folder must be distinct from folder 1 '{folder_poll1}', got: {entries_after_poll3:?}"
    );

    orchestrator.cancel();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_restricted_stream_resets_on_offline() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_rst_off_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_rst_off", "StreamerRst")],
        ..Default::default()
    };

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_rst_off",
        vec![
            // Poll 1: Restricted Stream A
            make_restricted_detail("chan_rst_off", "StreamerRst", "Stream A", Some(7001), true),
            // Poll 2: Channel goes CLOSE (offline)
            make_close_detail(Some("StreamerRst")),
            // Poll 3: Restricted Stream B starts
            make_restricted_detail("chan_rst_off", "StreamerRst", "Stream B", Some(7002), true),
        ],
    );

    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Restricted A
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_rst_off"));

    // Poll 2: Offline
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_rst_off"));

    // Poll 3: Restricted B
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_rst_off"));

    let mut error_log_count = 0;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = event
            && entry
                .message
                .contains("Recording unavailable for channel chan_rst_off")
        {
            error_log_count += 1;
        }
    }
    assert_eq!(
        error_log_count, 2,
        "Expected 1 error log for Stream A and 1 for Stream B after offline reset"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_empty_session_folder_deleted_after_broadcast_ends_and_uploads_finish() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_session_clean_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let channel_id = "chan_post_broadcast";
    let session_dir = temp_dir.join(format!("{channel_id}_20260927_100000"));
    fs::create_dir_all(&session_dir).unwrap();

    let chunk_path = session_dir.join("chunk_0000.ts");
    fs::write(&chunk_path, vec![0u8; 1024 * 1024]).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::default());

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);
    let drain_notify = Arc::new(tokio::sync::Notify::new());

    let custodian = chzzk_load::engine::SessionCustodian::new(event_tx.clone());
    custodian.register_draining(&session_dir, channel_id, "Streamer Clean");

    let _consumer_handle = chzzk_load::uploader::UploadWorker::spawn_with_options(
        Some(mock_backend.clone()),
        event_tx,
        upload_rx,
        1,
        chzzk_load::uploader::DlqConfig::default(),
        Some(drain_notify.clone()),
    );

    let custodian_clone = custodian.clone();
    let notify_clone = drain_notify.clone();
    let purge_task = tokio::spawn(async move {
        notify_clone.notified().await;
        custodian_clone.try_purge_drained().await;
    });

    upload_tx
        .send(UploadTask {
            channel_id: channel_id.to_string(),
            session_folder_id: "folder_clean_123".to_string(),
            remote_dir: "folder_clean_123".to_string(),
            chunk_path: chunk_path.clone(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "Streamer Clean".to_string(),
        })
        .await
        .unwrap();

    assert_log_emitted_timeout(
        &mut event_rx,
        "Cleaned up empty session folder",
        std::time::Duration::from_secs(5),
    )
    .await;

    let _ = purge_task.await;

    assert!(
        !chunk_path.exists(),
        "Chunk file must be deleted upon upload confirmation"
    );
    assert!(
        !session_dir.exists(),
        "Empty stream session folder must be deleted after broadcast ends and all cleanup tasks are finished"
    );

    let uploads = mock_backend.uploads.lock().await;
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, chunk_path);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_cleanup_empty_session_dirs_excluding_active_preserves_active_session() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_cleanup_excluding_active_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let active_chan = "ch_active";
    let ended_chan = "ch_ended";

    let active_dir = temp_dir.join(format!("{active_chan}_20260927_100000"));
    let ended_dir = temp_dir.join(format!("{ended_chan}_20260927_090000"));
    let non_empty_dir = temp_dir.join("ch_other_20260927_080000");

    fs::create_dir_all(&active_dir).unwrap();
    fs::create_dir_all(&ended_dir).unwrap();
    fs::create_dir_all(&non_empty_dir).unwrap();
    fs::write(non_empty_dir.join("chunk_0000.ts"), b"data").unwrap();

    let mut active_set = std::collections::HashSet::new();
    active_set.insert(active_chan.to_string());

    let removed = EngineOrchestrator::cleanup_empty_session_dirs_excluding(&temp_dir, &active_set)
        .await
        .expect("cleanup should succeed");

    assert_eq!(removed, 1, "Only ended_dir should be removed");
    assert!(!ended_dir.exists(), "ended_dir should be deleted");
    assert!(
        active_dir.exists(),
        "active_dir must be preserved even if empty"
    );
    assert!(non_empty_dir.exists(), "non_empty_dir must be preserved");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_running_orchestrator_cleans_empty_session_folder_after_broadcast_ends() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_live_clean_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let empty_session_dir = temp_dir.join("chan_ended_20260927_100000");
    fs::create_dir_all(&empty_session_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_ended", "EndedStreamer")],
        ..Default::default()
    };
    let chzzk = Arc::new(
        MockLiveStreamSource::new()
            .with_channel_state("chan_ended", make_close_detail(Some("EndedStreamer"))),
    );
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    // Poll channel once while orchestrator is running
    orchestrator.poll_channels_once(&upload_tx).await;

    assert!(
        !empty_session_dir.exists(),
        "Empty session directory must be deleted after stream ends during normal polling"
    );

    assert_log_emitted(&mut event_rx, "empty session folder");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_recording_session_cleans_empty_folder_when_no_chunks_saved() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_session_clean_no_chunks_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let chzzk = Arc::new(MockLiveStreamSource::new());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let info = chzzk_load::chzzk::models::LiveStreamInfo {
        channel_id: "chan_empty_session".to_string(),
        live_id: Some(12345),
        streamer_name: "EmptyStreamer".to_string(),
        title: "Empty Stream Title".to_string(),
        hls_url: "http://127.0.0.1:9999/nonexistent.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_empty_session".to_string(), info, upload_tx);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        cancel_clone.cancel();
    });

    let mut got_clean_log = false;
    let mut got_ended = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(10), event_rx.recv()).await
    {
        match ev {
            AppEvent::Log(entry) if entry.message.contains("Cleaned up empty session folder") => {
                got_clean_log = true;
            }
            AppEvent::RecordingEnded { ref channel_id } if channel_id == "chan_empty_session" => {
                got_ended = true;
            }
            _ => {}
        }
        if got_clean_log && got_ended {
            break;
        }
    }

    // Give session directory cleanup a moment to finalize
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Check temp_dir contents: there should be no empty session folder remaining
    let mut entries = tokio::fs::read_dir(&temp_dir).await.unwrap();
    let mut remaining_session_dirs = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            remaining_session_dirs += 1;
        }
    }

    assert_eq!(
        remaining_session_dirs, 0,
        "Empty stream session folder must be deleted when no chunks were saved"
    );
    assert!(
        got_clean_log,
        "Expected clean log message for empty session folder deletion"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_handles_ffmpeg_key_403_forbidden_stream() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_ffmpeg_key_403_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let mock_bin = get_mock_ffmpeg_bin();

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_sports",
        vec![
            // Poll 2: OPEN broadcast (API cache still OPEN)
            make_open_detail(
                "chan_sports",
                "SportsStreamer",
                "Sports Broadcast (Encrypted)",
                21326414,
                "https://test.com/hls_key_error.m3u8",
            ),
            // Poll 3: CLOSE (offline)
            make_close_detail(Some("SportsStreamer")),
        ],
    );

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_sports", "SportsStreamer")],
        ..Default::default()
    };
    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings,
            chzzk,
            None,
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(mock_bin.to_string_lossy()),
    );

    let info = chzzk_load::chzzk::models::LiveStreamInfo {
        channel_id: "chan_sports".to_string(),
        live_id: Some(21326414),
        streamer_name: "SportsStreamer".to_string(),
        title: "Sports Broadcast (Encrypted)".to_string(),
        hls_url: "https://test.com/hls_key_error.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_sports".to_string(), info, upload_tx.clone());

    // Collect events emitted during FFmpeg 403 handling
    let mut got_error_log = false;
    let mut got_ended = false;
    let mut got_live_channel_update = false;
    let mut ffmpeg_logs = Vec::new();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(300), event_rx.recv()).await {
            Ok(Some(ev)) => match ev {
                AppEvent::Log(entry) => {
                    if entry.message.contains("requires valid Naver credentials") {
                        got_error_log = true;
                    }
                    if entry.message.starts_with("[FFMPEG]") {
                        ffmpeg_logs.push(entry.message);
                    }
                }
                AppEvent::RecordingEnded { ref channel_id } if channel_id == "chan_sports" => {
                    got_ended = true;
                }
                AppEvent::ChannelUpdate {
                    ref channel_id,
                    is_live,
                    ..
                } if channel_id == "chan_sports" && is_live => {
                    got_live_channel_update = true;
                }
                _ => {}
            },
            _ => {
                if got_error_log && got_ended && got_live_channel_update {
                    break;
                }
            }
        }
    }

    assert_with_logs(
        got_error_log,
        "Must emit single error log for restricted stream credentials",
        &mut event_rx,
        None,
    );
    assert_with_logs(
        got_ended,
        "Must emit RecordingEnded event",
        &mut event_rx,
        None,
    );
    assert_with_logs(
        got_live_channel_update,
        "Must set channel status to LIVE",
        &mut event_rx,
        None,
    );
    assert_with_logs(
        ffmpeg_logs.is_empty(),
        &format!(
            "FFmpeg 403 and segment skipping errors must NOT be forwarded to logs (zero log spam), got: {ffmpeg_logs:?}"
        ),
        &mut event_rx,
        None,
    );

    // Verify channel is removed from recording, and marked restricted
    assert!(
        !orchestrator.is_recording("chan_sports"),
        "Channel must not be in active_recordings after 403 detection"
    );
    assert!(
        orchestrator.is_restricted("chan_sports"),
        "Channel must be in restricted_channels after 403 detection"
    );
    assert!(
        matches!(
            orchestrator.channel_state("chan_sports"),
            ChannelLifecycleState::Restricted {
                live_id: Some(21326414),
                reason: RestrictionReason::KeyForbidden
            }
        ),
        "channel_state must track liveId 21326414 and RestrictionReason::KeyForbidden"
    );

    // Poll 2: Next polling cycle for the same broadcast
    orchestrator.poll_channels_once(&upload_tx).await;
    // Should NOT spawn another recording session
    assert!(
        !orchestrator.is_recording("chan_sports"),
        "Channel must not spawn another session for the same restricted liveId"
    );
    assert!(
        orchestrator.is_restricted("chan_sports"),
        "Channel must remain in restricted_channels"
    );

    // Poll 3: Channel goes offline (CLOSE)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        !orchestrator.is_restricted("chan_sports"),
        "Channel must be cleared from restricted_channels on CLOSE"
    );
    assert!(
        matches!(
            orchestrator.channel_state("chan_sports"),
            ChannelLifecycleState::Idle
        ),
        "Channel must be cleared from restricted_live_ids on CLOSE"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_broadcast_identifier_resolution() {
    use std::path::PathBuf;

    assert_eq!(broadcast_identifier("Streamer", "chan_123"), "Streamer");
    assert_eq!(broadcast_identifier("  Streamer  ", "chan_123"), "Streamer");
    assert_eq!(broadcast_identifier("", "chan_123"), "chan_123");
    assert_eq!(broadcast_identifier("   ", "chan_123"), "chan_123");

    let task1 = UploadTask {
        channel_id: "chan_1".to_string(),
        session_folder_id: "s1".to_string(),
        remote_dir: "s1".to_string(),
        chunk_path: PathBuf::from("chunk_0000.ts"),
        chunk_name: "chunk_0000.ts".to_string(),
        streamer_name: "StreamerOne".to_string(),
    };
    assert_eq!(task1.broadcast_identifier(), "StreamerOne");

    let task2 = UploadTask {
        channel_id: "chan_2".to_string(),
        session_folder_id: "s2".to_string(),
        remote_dir: "s2".to_string(),
        chunk_path: PathBuf::from("chunk_0000.ts"),
        chunk_name: "chunk_0000.ts".to_string(),
        streamer_name: "   ".to_string(),
    };
    assert_eq!(task2.broadcast_identifier(), "chan_2");
}

#[tokio::test]
async fn test_upload_consumer_logs_broadcast_identifier_on_success_and_failure() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_broadcast_id_logs_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_path = temp_dir.join("chunk_0001.ts");
    fs::write(&chunk_path, vec![0u8; 1024 * 1024]).unwrap();

    let chat_path = temp_dir.join("chat_0001.jsonl");
    fs::write(&chat_path, b"{\"test\":true}\n").unwrap();

    let chunk_path_fallback = temp_dir.join("chunk_0002.ts");
    fs::write(&chunk_path_fallback, vec![0u8; 512 * 1024]).unwrap();

    let mock_backend = Arc::new(MockUploadBackend::new());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    EngineOrchestrator::spawn_upload_consumer_with_concurrency(
        Some(mock_backend.clone()),
        event_tx.clone(),
        upload_rx,
        2,
    );

    // Send video chunk task with streamer_name
    upload_tx
        .send(UploadTask {
            channel_id: "chan_streamer_1".to_string(),
            session_folder_id: "session_1".to_string(),
            remote_dir: "session_1".to_string(),
            chunk_path: chunk_path.clone(),
            chunk_name: "chunk_0001.ts".to_string(),
            streamer_name: "StreamerOne".to_string(),
        })
        .await
        .unwrap();

    // Send chat chunk task with streamer_name
    upload_tx
        .send(UploadTask {
            channel_id: "chan_streamer_1".to_string(),
            session_folder_id: "session_1".to_string(),
            remote_dir: "session_1".to_string(),
            chunk_path: chat_path.clone(),
            chunk_name: "chat_0001.jsonl".to_string(),
            streamer_name: "StreamerOne".to_string(),
        })
        .await
        .unwrap();

    // Send video chunk task with whitespace streamer_name (should fallback to channel_id)
    upload_tx
        .send(UploadTask {
            channel_id: "chan_fallback_id".to_string(),
            session_folder_id: "session_2".to_string(),
            remote_dir: "session_2".to_string(),
            chunk_path: chunk_path_fallback.clone(),
            chunk_name: "chunk_0002.ts".to_string(),
            streamer_name: "   ".to_string(),
        })
        .await
        .unwrap();

    // Drop upload_tx and event_tx to signal completion
    drop(upload_tx);
    drop(event_tx);

    let mut recorder = TestLogRecorder::new();
    let mut got_video_clean_log = false;
    let mut got_chat_clean_log = false;
    let mut got_fallback_clean_log = false;

    while let Some(event) = event_rx.recv().await {
        recorder.record(&event);
        if let AppEvent::Log(msg) = event {
            if msg.contains("[StreamerOne] Uploaded & deleted chunk_0001.ts") {
                got_video_clean_log = true;
            }
            if msg.contains("[StreamerOne] Uploaded & deleted chat_0001.jsonl") {
                got_chat_clean_log = true;
            }
            if msg.contains("[chan_fallback_id] Uploaded & deleted chunk_0002.ts") {
                got_fallback_clean_log = true;
            }
        }
    }

    assert_with_logs(
        got_video_clean_log,
        "Must log [StreamerOne] on video chunk upload & delete",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        got_chat_clean_log,
        "Must log [StreamerOne] on chat chunk upload & delete",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        got_fallback_clean_log,
        "Must log [chan_fallback_id] fallback when streamer name is whitespace",
        &mut event_rx,
        Some(&recorder),
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_engine_folder_naming_with_alias() {
    let state = ActiveSessionState::new(
        "2026-09-30_1100".to_string(),
        "StreamerName".to_string(),
        Some("CustomAlias".to_string()),
        chzzk_load::chzzk::models_metadata::StreamMetadataState {
            channel_name: "StreamerName".to_string(),
            live_title: "Gaming Stream".to_string(),
            ..Default::default()
        },
    );
    assert_eq!(
        state.folder_name(),
        "[2026-09-30_1100] [CustomAlias] StreamerName - Gaming Stream"
    );
}

#[test]
fn test_engine_folder_naming_without_alias() {
    let state = ActiveSessionState::new(
        "2026-09-30_1100".to_string(),
        "StreamerName".to_string(),
        None,
        chzzk_load::chzzk::models_metadata::StreamMetadataState {
            channel_name: "StreamerName".to_string(),
            live_title: "Gaming Stream".to_string(),
            ..Default::default()
        },
    );
    assert_eq!(
        state.folder_name(),
        "[2026-09-30_1100] StreamerName - Gaming Stream"
    );
}

#[test]
fn test_engine_folder_naming_empty_alias_fallback() {
    let state = ActiveSessionState::new(
        "2026-09-30_1100".to_string(),
        "StreamerName".to_string(),
        Some("   ".to_string()),
        chzzk_load::chzzk::models_metadata::StreamMetadataState {
            channel_name: "StreamerName".to_string(),
            live_title: "Gaming Stream".to_string(),
            ..Default::default()
        },
    );
    assert_eq!(
        state.folder_name(),
        "[2026-09-30_1100] StreamerName - Gaming Stream"
    );
}

#[test]
fn test_engine_folder_naming_sanitizes_illegal_and_trailing_chars() {
    let state = ActiveSessionState::new(
        "2026-09-30_1100".to_string(),
        "Streamer/Name...".to_string(),
        Some("Alias:Special ".to_string()),
        chzzk_load::chzzk::models_metadata::StreamMetadataState {
            channel_name: "Streamer/Name...".to_string(),
            live_title: "Gaming Stream? Playing Now... ".to_string(),
            ..Default::default()
        },
    );
    assert_eq!(
        state.folder_name(),
        "[2026-09-30_1100] [Alias_Special] Streamer_Name - Gaming Stream_ Playing Now"
    );
}

#[tokio::test]
async fn test_engine_orchestrator_channel_update_uses_alias() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(32);
    let (upload_tx, _upload_rx) = tokio::sync::mpsc::channel(32);

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("chan_alias", "MyAlias")],
        ..Default::default()
    };

    let client = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_alias",
        make_open_detail(
            "chan_alias",
            "OfficialKoreanName",
            "Live Stream",
            9999,
            "https://dummy.m3u8",
        ),
    ));
    let orchestrator = EngineOrchestrator::new(settings, client, None, event_tx);

    orchestrator.poll_channels_once(&upload_tx).await;

    let mut received_update = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::ChannelUpdate {
            channel_id,
            channel_name,
            is_live,
            title,
        } = event
        {
            assert_eq!(channel_id, "chan_alias");
            // Must use MyAlias, NOT OfficialKoreanName
            assert_eq!(channel_name, "MyAlias");
            assert!(is_live);
            assert_eq!(title, "Live Stream");
            received_update = true;
        }
    }
    assert!(received_update, "Expected ChannelUpdate event");

    let session = orchestrator
        .active_session("chan_alias")
        .expect("Session must exist");
    assert_eq!(
        session.folder_name(),
        format!(
            "[{}] [MyAlias] OfficialKoreanName - Live Stream",
            session.start_timestamp
        )
    );
}
