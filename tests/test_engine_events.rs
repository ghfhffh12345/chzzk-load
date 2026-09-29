use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tiny_http::{Header, Response, Server, StatusCode};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use std::sync::atomic::{AtomicBool, Ordering};

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::engine::{ActiveSessionState, EngineOrchestrator};
use chzzk_load::tui::event::{AppEvent, LogEntry};
use chzzk_load::uploader::{
    BoxFuture, MockUploadBackend, ProgressCallback, UploadBackend, UploadTask,
};

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
    let chzzk = ChzzkClient::new(&settings.chzzk);
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);
    let active = orchestrator.active_recordings();
    let active_guard = active.lock().await;
    assert!(active_guard.is_empty());
}

#[tokio::test]
async fn test_engine_orchestrator_poll_channel_offline() {
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "CLOSE",
                    "liveTitle": null,
                    "channel": {
                        "channelId": "chan_offline",
                        "channelName": "OfflineStreamer"
                    },
                    "livePlaybackJson": null
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_offline".to_string(),
            name: "OfflineStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);
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
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let response =
                Response::from_string("internal error").with_status_code(StatusCode(500));
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_err".to_string(),
            name: "ErrorStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);
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
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // First poll
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveTitle": "Playing Games",
                    "channel": {
                        "channelId": "chan_live",
                        "channelName": "LiveStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }

        // Second poll
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveTitle": "Playing Games",
                    "channel": {
                        "channelId": "chan_live",
                        "channelName": "LiveStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_live_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_live".to_string(),
            name: "LiveStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

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
    {
        let active = orchestrator.active_recordings();
        let guard = active.lock().await;
        assert!(guard.contains("chan_live"));
    }

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
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let req_count_server = request_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = req_count_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = match count {
                // Poll 1: Channel is live with liveId 21212268
                0 => {
                    r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "OPEN",
                        "liveTitle": "Stream A",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#
                }
                // Poll 2: Stream ended, but CDN/cache still reports OPEN with same liveId 21212268!
                1 => {
                    r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "OPEN",
                        "liveTitle": "Stream A",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#
                }
                // Poll 3: CDN cache finally clears to CLOSE
                2 => {
                    r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "CLOSE",
                        "liveTitle": null,
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": null
                    }
                }"#
                }
                // Poll 4: Genuinely new stream starts with new liveId 21212269
                _ => {
                    r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212269,
                        "status": "OPEN",
                        "liveTitle": "Stream B (New)",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#
                }
            };
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_race_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 60,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_race".to_string(),
            name: "StreamerRace".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

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

    // Simulate session 1 ending: active_recordings removes channel, finished_sessions records it
    // In actual app, this happens when FFmpeg exits
    {
        let active = orchestrator.active_recordings();
        active.lock().await.remove("chan_race");
        // Also register finished session directly if helper or method exists
        orchestrator
            .register_finished_session("chan_race", Some(21212268))
            .await;
    }

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
    {
        let active = orchestrator.active_recordings();
        assert!(
            !active.lock().await.contains("chan_race"),
            "Channel must not be in active_recordings"
        );
    }

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

    while let Some(ev) = event_rx.recv().await {
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

    assert!(got_completed);
    assert!(got_clean_log);
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
    while let Some(ev) = event_rx.recv().await {
        if let AppEvent::Log(msg) = ev
            && msg.contains("Upload failed for chunk_fail.ts")
        {
            got_error_log = true;
            break;
        }
    }

    assert!(got_error_log);
    // File MUST be preserved upon failure
    assert!(chunk_path.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_process_sealed_chunk_no_drive_saves_locally() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_chunk_no_drive_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_path = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk_path, b"dummy video bytes").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        "Streamer - Title",
        "chan_local",
        "Streamer",
        &upload_tx,
        &event_tx,
        false,
    )
    .await;

    assert!(upload_rx.try_recv().is_err());

    let mut got_chunk_sealed = false;
    let mut got_saved_locally_log = false;

    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::ChunkSealed {
                chunk_name,
                size_bytes,
            } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(size_bytes, 17);
                got_chunk_sealed = true;
            }
            AppEvent::Log(msg) if msg == "[REC] chunk_0000.ts sealed (saved locally)." => {
                got_saved_locally_log = true;
            }
            _ => {}
        }
    }

    assert!(got_chunk_sealed);
    assert!(got_saved_locally_log);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_process_sealed_chunk_retry_drive_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_chunk_retry_ok_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_path = temp_dir.join("chunk_0001.ts");
    fs::write(&chunk_path, b"test chunk content").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        "Subfolder",
        "chan_sub",
        "StreamerSub",
        &upload_tx,
        &event_tx,
        true,
    )
    .await;

    // Verify task in upload_tx
    let task = upload_rx.recv().await.expect("Expected UploadTask");
    assert_eq!(task.remote_dir, "Subfolder");
    assert_eq!(task.chunk_name, "chunk_0001.ts");
    assert_eq!(task.channel_id, "chan_sub");
    assert_eq!(task.streamer_name, "StreamerSub");

    let mut got_pushed_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg == "[REC] chunk_0001.ts sealed. Pushed to cloud upload queue."
        {
            got_pushed_log = true;
        }
    }

    assert!(got_pushed_log);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_process_sealed_chunk_retry_drive_failure() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_chunk_retry_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_path = temp_dir.join("chunk_0002.ts");
    fs::write(&chunk_path, b"test video data").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);
    drop(upload_rx);

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        "Subfolder",
        "chan_fail_test",
        "StreamerFail",
        &upload_tx,
        &event_tx,
        true,
    )
    .await;

    let mut got_saved_locally_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg == "[REC] chunk_0002.ts sealed (saved locally)."
        {
            got_saved_locally_log = true;
        }
    }

    assert!(got_saved_locally_log);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_process_sealed_chunk_existing_folder_id_skips_retry() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_chunk_existing_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_path = temp_dir.join("chunk_0003.ts");
    fs::write(&chunk_path, b"another chunk data").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        "Subfolder",
        "chan_exist",
        "StreamerExist",
        &upload_tx,
        &event_tx,
        true,
    )
    .await;

    let task = upload_rx.recv().await.expect("Expected UploadTask");
    assert_eq!(task.remote_dir, "Subfolder");
    assert_eq!(task.chunk_name, "chunk_0003.ts");

    let mut got_pushed_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg == "[REC] chunk_0003.ts sealed. Pushed to cloud upload queue."
        {
            got_pushed_log = true;
        }
    }
    assert!(got_pushed_log);

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
    let chzzk = ChzzkClient::new(&settings.chzzk);
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

    let mut got_shutdown_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg.contains("Engine graceful shutdown complete.")
        {
            got_shutdown_log = true;
        }
    }
    assert!(got_shutdown_log, "Expected shutdown completion log");
}

#[tokio::test]
async fn test_engine_orchestrator_graceful_shutdown_with_active_session() {
    use tokio_util::sync::CancellationToken;

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 123456,
                    "liveTitle": "Live for shutdown test",
                    "channel": {
                        "channelId": "chan_shutdown",
                        "channelName": "ShutdownStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

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
        channels: vec![ChannelConfig {
            id: "chan_shutdown".to_string(),
            name: "ShutdownStreamer".to_string(),
        }],
        ..Default::default()
    };
    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let (req_tx, mut req_rx) = mpsc::channel::<()>(10);
    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let _ = req_tx.try_send(());
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "CLOSE",
                    "liveTitle": null,
                    "channel": {
                        "channelId": "chan_refresh",
                        "channelName": "RefreshStreamer"
                    },
                    "livePlaybackJson": null
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            poll_interval_seconds: 3600, // 1 hour interval
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_refresh".to_string(),
            name: "RefreshStreamer".to_string(),
        }],
        ..Default::default()
    };
    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // First request should happen immediately on startup
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), req_rx.recv())
        .await
        .expect("Timeout waiting for initial poll");

    // Clear event queue
    while event_rx.try_recv().is_ok() {}

    // Trigger manual refresh
    orchestrator.trigger_refresh();

    // Second request must happen quickly despite 1 hour sleep interval
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), req_rx.recv())
        .await
        .expect("Timeout waiting for manual refresh poll");

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
async fn test_engine_orchestrator_stream_title_change_renames_drive_folder() {
    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // Poll 1: Initial title "Initial Stream Title"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 999111,
                    "liveTitle": "Initial Stream Title",
                    "channel": {
                        "channelId": "chan_rename",
                        "channelName": "RenameStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }

        // Poll 2: Streamer changes title to "Updated Stream Title? Playing Now?"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 999111,
                    "liveTitle": "Updated Stream Title? Playing Now?",
                    "channel": {
                        "channelId": "chan_rename",
                        "channelName": "RenameStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_rename_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_rename".to_string(),
            name: "RenameStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator =
        EngineOrchestrator::new(settings, chzzk, Some(mock_backend.clone()), event_tx);

    // Initialize active recording state
    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_rename".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_rename".to_string(),
            chzzk_load::engine::ActiveSessionState::new(
                "2026-09-22_1000".to_string(),
                "RenameStreamer".to_string(),
                "Initial Stream Title".to_string(),
            ),
        );
    }

    // Poll 1: Channel is polled with same initial title (no title history upload expected)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(mock_backend.texts.lock().await.is_empty());

    // Poll 2: Streamer changed title to "Updated Stream Title? Playing Now?"
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify backend received title_history.txt upload
    let texts = mock_backend.texts.lock().await;
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].1, "title_history.txt");
    assert!(texts[0].2.contains("Initial Stream Title"));
    assert!(texts[0].2.contains("Updated Stream Title? Playing Now?"));

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
async fn test_engine_orchestrator_stream_title_change_updates_title_history_file() {
    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // Poll 1: Initial title "Initial Stream Title"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 999111,
                    "liveTitle": "Initial Stream Title",
                    "channel": {
                        "channelId": "chan_rename",
                        "channelName": "RenameStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }

        // Poll 2: Streamer changes title to "Updated Stream Title"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 999111,
                    "liveTitle": "Updated Stream Title? Playing Now?",
                    "channel": {
                        "channelId": "chan_rename",
                        "channelName": "RenameStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_history_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_rename".to_string(),
            name: "RenameStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));

    let mock_backend = Arc::new(MockUploadBackend::default());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator =
        EngineOrchestrator::new(settings, chzzk, Some(mock_backend.clone()), event_tx);

    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_rename".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_rename".to_string(),
            chzzk_load::engine::ActiveSessionState {
                start_timestamp: "2026-09-22_1000".to_string(),
                streamer_name: "RenameStreamer".to_string(),
                initial_title: "Initial Stream Title".to_string(),
                current_title: "Initial Stream Title".to_string(),
                title_history: vec![(
                    "2026-09-22 10:00:00".to_string(),
                    "Initial Stream Title".to_string(),
                )],
            },
        );
    }

    // Poll 1: Channel is polled with same initial title (no history upload)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(mock_backend.texts.lock().await.is_empty());

    // Poll 2: Streamer changed title to "Updated Stream Title"
    orchestrator.poll_channels_once(&upload_tx).await;

    let texts = mock_backend.texts.lock().await;
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].1, "title_history.txt");
    let content = &texts[0].2;
    assert!(content.contains("[2026-09-22 10:00:00] Initial Stream Title"));
    assert!(content.contains("Updated Stream Title? Playing Now?"));

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_rename").unwrap();
        assert_eq!(session.title_history.len(), 2);
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_stream_title_change_before_folder_creation() {
    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // Poll 1: Initial title "Early Title 1"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 555666,
                    "liveTitle": "Early Title 1",
                    "channel": {
                        "channelId": "chan_pre",
                        "channelName": "PreStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }

        // Poll 2: Title changes to "Early Title 2"
        if let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 555666,
                    "liveTitle": "Early Title 2? Pending?",
                    "channel": {
                        "channelId": "chan_pre",
                        "channelName": "PreStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_pre_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_pre".to_string(),
            name: "PreStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_pre".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_pre".to_string(),
            chzzk_load::engine::ActiveSessionState::new(
                "2026-09-22_1000".to_string(),
                "PreStreamer".to_string(),
                "Early Title 1".to_string(),
            ),
        );
    }

    // Poll 1: Session starts with Early Title 1 (already active, no title change)
    orchestrator.poll_channels_once(&upload_tx).await;

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_pre").expect("Session should exist");
        assert_eq!(session.current_title, "Early Title 1");
    }

    // Poll 2: Title changes to Early Title 2
    orchestrator.poll_channels_once(&upload_tx).await;

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_pre").expect("Session should exist");
        assert_eq!(session.current_title, "Early Title 2? Pending?");
    }

    let mut got_title_change_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg.contains("Stream title changed for chan_pre")
            && msg.contains("Early Title 2? Pending?")
        {
            got_title_change_log = true;
        }
    }
    assert!(got_title_change_log, "Expected title change log message");

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
    let chzzk = ChzzkClient::new(&settings.chzzk);
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

    let mut got_clean_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg.contains("Cleaned up")
            && msg.contains("empty session folder")
        {
            got_clean_log = true;
        }
    }
    assert!(
        got_clean_log,
        "Expected log message indicating cleanup of empty session folder"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_active_session_state_folder_name_preserves_question_marks() {
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
        "[2026-09-22_2200] Streamer?Name - Is this live? Yes! Special_ 100% _Stream_"
    );
    assert!(folder_name.contains("Streamer?Name"));
    assert!(folder_name.contains("Is this live? Yes!"));
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
        "[2026-09-22_1530] Chzzk Streamer _ Channel - What's Next? Let's Play _ Ep. 1 _Final_"
    );
}

#[tokio::test]
async fn test_engine_orchestrator_resumes_recording_after_cooldown_for_interrupted_stream() {
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 888999,
                    "status": "OPEN",
                    "liveTitle": "Ongoing Stream After Interruption",
                    "channel": { "channelId": "chan_interrupt", "channelName": "StreamerInterrupt" },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_resume_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 1, // 1 second cooldown
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_interrupt".to_string(),
            name: "StreamerInterrupt".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Simulate session interrupted in the past (liveId: 888999, finished_at: 2 seconds ago > 1s cooldown)
    {
        let sessions = orchestrator.finished_sessions();
        let mut finished = sessions.lock().await;
        finished.insert(
            "chan_interrupt".to_string(),
            chzzk_load::engine::FinishedSession {
                live_id: Some(888999),
                finished_at: std::time::Instant::now() - std::time::Duration::from_secs(2),
            },
        );
    }

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

    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 998877,
                    "liveTitle": "Live for upload shutdown test",
                    "channel": {
                        "channelId": "chan_upload_shutdown",
                        "channelName": "ShutdownUploader"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

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
        channels: vec![ChannelConfig {
            id: "chan_upload_shutdown".to_string(),
            name: "ShutdownUploader".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(mock_backend.clone()),
        event_tx,
        cancel_token.clone(),
    ));

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
    let session_dir = session_folder.expect("Session directory must exist");
    let chunk_path = session_dir.join("chunk_0000.ts");
    fs::write(&chunk_path, b"TEST_CHUNK_PAYLOAD_DATA_FOR_SHUTDOWN_TEST").unwrap();
    assert!(chunk_path.exists());

    // Cancel engine token while actively recording
    cancel_token.cancel();

    // Drain events in background and track UploadCompleted
    let drain_handle = tokio::spawn(async move {
        let mut got_completed = false;
        while let Ok(Some(ev)) =
            tokio::time::timeout(std::time::Duration::from_secs(20), event_rx.recv()).await
        {
            if let AppEvent::UploadCompleted { chunk_name, .. } = ev
                && chunk_name == "chunk_0000.ts"
            {
                got_completed = true;
            }
        }
        got_completed
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

    let got_completed = drain_handle.await.unwrap_or(false);
    assert!(
        got_completed,
        "UploadCompleted event for chunk_0000.ts must be emitted during graceful shutdown!"
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

    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 887766,
                    "liveTitle": "Live for shutdown serialization test",
                    "channel": {
                        "channelId": "chan_shutdown_serial",
                        "channelName": "ShutdownSerialStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

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
        channels: vec![ChannelConfig {
            id: "chan_shutdown_serial".to_string(),
            name: "ShutdownSerialStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(backend),
        event_tx,
        cancel_token.clone(),
    ));

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
    let session_dir = session_folder.expect("Session directory must exist");
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

    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = chzzk_server.recv() {
            let url = request.url().to_string();
            let channel_id = if url.contains("chan_multi_1") {
                "chan_multi_1"
            } else if url.contains("chan_multi_2") {
                "chan_multi_2"
            } else {
                "unknown"
            };

            let mock_body = format!(
                r#"{{
                    "code": 200,
                    "message": null,
                    "content": {{
                        "status": "OPEN",
                        "liveId": 112233,
                        "liveTitle": "Concurrent Stream {channel_id}",
                        "channel": {{
                            "channelId": "{channel_id}",
                            "channelName": "Streamer_{channel_id}"
                        }},
                        "livePlaybackJson": "{{\"media\":[{{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}}]}}]}}"
                    }}
                }}"#
            );

            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

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
            ChannelConfig {
                id: "chan_multi_1".to_string(),
                name: "Streamer_1".to_string(),
            },
            ChannelConfig {
                id: "chan_multi_2".to_string(),
                name: "Streamer_2".to_string(),
            },
        ],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(mock_backend.clone()),
        event_tx,
        cancel_token.clone(),
    ));

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
                    if name.starts_with("chan_multi_1_") {
                        session_dir_1 = Some(path.clone());
                    } else if name.starts_with("chan_multi_2_") {
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

    let dir1 = session_dir_1.expect("Session dir 1 must exist");
    let dir2 = session_dir_2.expect("Session dir 2 must exist");

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
    let mut uploaded_channels = HashSet::new();
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv()).await
    {
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

    assert!(
        uploaded_channels.contains("chan_multi_1"),
        "chan_multi_1 chunk 0 should be uploaded"
    );
    assert!(
        uploaded_channels.contains("chan_multi_2"),
        "chan_multi_2 chunk 0 should be uploaded"
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
async fn test_engine_orchestrator_recovers_and_uploads_pending_chunks_after_drive_rate_limit() {
    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(req) = chzzk_server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 88881,
                    "status": "OPEN",
                    "liveTitle": "Retry Stream",
                    "channel": { "channelId": "chan_retry", "channelName": "StreamerRetry" },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = req.respond(response);
        }
    });

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
        channels: vec![ChannelConfig {
            id: "chan_retry".to_string(),
            name: "StreamerRetry".to_string(),
        }],
        ..Default::default()
    };

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{chzzk_port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(mock_backend.clone()),
        event_tx,
        cancel_token.clone(),
    ));

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
                    if name.starts_with("chan_retry_") {
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

    let dir = session_dir.expect("Session dir must exist");

    // Write chunk_0000.ts and chunk_0001.ts
    let c0 = dir.join("chunk_0000.ts");
    let c1 = dir.join("chunk_0001.ts");
    fs::write(&c0, b"CHUNK_0_DATA").unwrap();
    fs::write(&c1, b"CHUNK_1_DATA").unwrap();

    // Check if chunk_0000.ts completes upload
    let mut chunk_0_uploaded = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(AppEvent::UploadCompleted { chunk_name, .. })) =
            tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await
            && chunk_name == "chunk_0000.ts"
        {
            chunk_0_uploaded = true;
            break;
        }
    }

    assert!(chunk_0_uploaded, "chunk_0000.ts must be uploaded");

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
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 5001,
                    "status": "OPEN",
                    "liveTitle": "[19+] Midnight Broadcast",
                    "channel": { "channelId": "chan_restricted", "channelName": "RestrictedStreamer" },
                    "livePlaybackJson": null,
                    "adult": true
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_restr_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_restricted".to_string(),
            name: "RestrictedStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
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
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_restricted")
    );
    assert!(orchestrator.active_recordings().lock().await.is_empty());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_restricted_stream_recovers_to_recordable() {
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = if count == 0 {
                // Poll 1: Restricted
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 6001,
                        "status": "OPEN",
                        "liveTitle": "Watch Party (Restricted)",
                        "channel": { "channelId": "chan_recover", "channelName": "RecoverStreamer" },
                        "livePlaybackJson": null,
                        "adult": false
                    }
                }"#
            } else {
                // Poll 2: Becomes recordable
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 6001,
                        "status": "OPEN",
                        "liveTitle": "Watch Party (Public)",
                        "channel": { "channelId": "chan_recover", "channelName": "RecoverStreamer" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}",
                        "adult": false
                    }
                }"#
            };
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_recov_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_recover".to_string(),
            name: "RecoverStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Restricted
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_recover")
    );
    assert!(orchestrator.active_recordings().lock().await.is_empty());

    // Poll 2: Now recordable
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        !orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_recover")
    );
    assert!(
        orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_recover")
    );

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

    let mock_bin = temp_dir.join(if cfg!(windows) {
        "mock_ffmpeg.exe"
    } else {
        "mock_ffmpeg"
    });
    let src_path = temp_dir.join("mock_ffmpeg.rs");
    fs::write(
        &src_path,
        r#"
use std::io::Read;
fn main() {
    let exe = std::env::current_exe().unwrap();
    let out_dir = exe.parent().unwrap();
    let pid = std::process::id();
    let start_file = out_dir.join(format!("ffmpeg_start_{pid}.txt"));
    let exit_file = out_dir.join(format!("ffmpeg_exit_{pid}.txt"));
    let _ = std::fs::write(&start_file, "running");
    let mut stdin = std::io::stdin();
    let mut buf = [0u8; 128];
    while let Ok(n) = stdin.read(&mut buf) {
        if n == 0 || buf[..n].contains(&b'q') {
            break;
        }
    }
    let _ = std::fs::write(&exit_file, "exited");
}
"#,
    )
    .unwrap();

    let compile_status = std::process::Command::new("rustc")
        .arg(&src_path)
        .arg("-o")
        .arg(&mock_bin)
        .status()
        .expect("Failed to compile mock_ffmpeg");
    assert!(compile_status.success(), "mock_ffmpeg compilation failed");

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = match count {
                0 => {
                    // Poll 1: Normal stream
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": 888777,
                            "status": "OPEN",
                            "liveTitle": "Public Stream Part 1",
                            "channel": { "channelId": "chan_trans", "channelName": "TransStreamer" },
                            "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}",
                            "adult": false
                        }
                    }"#
                }
                1 => {
                    // Poll 2: Transitions to Restricted (19+ adult without auth)
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": 888777,
                            "status": "OPEN",
                            "liveTitle": "[19+] Restricted Stream Part 2",
                            "channel": { "channelId": "chan_trans", "channelName": "TransStreamer" },
                            "livePlaybackJson": null,
                            "adult": true
                        }
                    }"#
                }
                _ => {
                    // Poll 3: Returns back to Normal (Public stream again)
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": 888777,
                            "status": "OPEN",
                            "liveTitle": "Public Stream Part 3 (Resumed)",
                            "channel": { "channelId": "chan_trans", "channelName": "TransStreamer" },
                            "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}",
                            "adult": false
                        }
                    }"#
                }
            };
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            stream_cooldown_seconds: 30, // Normal 30s cooldown
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_trans".to_string(),
            name: "TransStreamer".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx)
        .with_ffmpeg_bin(mock_bin.to_string_lossy());

    // --- Poll 1: Normal stream starts recording ---
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_trans"),
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
            .filter(|name| name.starts_with("chan_trans_"))
            .collect();
        if entries_after_poll1.len() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert_eq!(
        entries_after_poll1.len(),
        1,
        "Must create exactly 1 session folder for Poll 1: {entries_after_poll1:?}"
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
        !orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_trans"),
        "Poll 2: Must be removed from active_recordings on restriction"
    );
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_trans"),
        "Poll 2: Must be added to restricted_channels"
    );

    // Verify Session 1 FFmpeg process received termination and exited cleanly on restriction
    let mut session1_exited = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let count = fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("ffmpeg_exit_"))
            .count();
        if count >= 1 {
            session1_exited = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        session1_exited,
        "Session 1 FFmpeg process must be gracefully terminated upon entering restricted state!"
    );

    // Simulate restriction latch and cooldown that occurs upon session conclusion
    {
        let r_ids_arc = orchestrator.restricted_live_ids();
        let mut r_ids = r_ids_arc.lock().await;
        r_ids.insert("chan_trans".to_string(), 888777);
    }
    {
        let finished_arc = orchestrator.finished_sessions();
        let mut finished = finished_arc.lock().await;
        finished.insert(
            "chan_trans".to_string(),
            chzzk_load::engine::FinishedSession {
                live_id: Some(888777),
                finished_at: std::time::Instant::now(),
            },
        );
    }

    // Sleep a moment to ensure timestamp clock advances
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // --- Poll 3: Stream transitions back to Normal (same liveId) ---
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_trans"),
        "Poll 3: Must be recording again after returning to normal"
    );
    assert!(
        !orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_trans"),
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
            .filter(|name| name.starts_with("chan_trans_"))
            .collect();
        if entries_after_poll3.len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    assert!(
        entries_after_poll3.len() >= 2,
        "Must create a new distinct session folder for Poll 3, got: {entries_after_poll3:?}"
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
    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = match count {
                0 => {
                    // Poll 1: Restricted Stream A
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": 7001,
                            "status": "OPEN",
                            "liveTitle": "Stream A",
                            "channel": { "channelId": "chan_rst_off", "channelName": "StreamerRst" },
                            "livePlaybackJson": null,
                            "adult": true
                        }
                    }"#
                }
                1 => {
                    // Poll 2: Channel goes CLOSE (offline)
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": null,
                            "status": "CLOSE",
                            "liveTitle": null,
                            "channel": { "channelId": "chan_rst_off", "channelName": "StreamerRst" },
                            "livePlaybackJson": null
                        }
                    }"#
                }
                _ => {
                    // Poll 3: Restricted Stream B starts
                    r#"{
                        "code": 200,
                        "message": null,
                        "content": {
                            "liveId": 7002,
                            "status": "OPEN",
                            "liveTitle": "Stream B",
                            "channel": { "channelId": "chan_rst_off", "channelName": "StreamerRst" },
                            "livePlaybackJson": null,
                            "adult": true
                        }
                    }"#
                }
            };
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_rst_off_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_rst_off".to_string(),
            name: "StreamerRst".to_string(),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Restricted A
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_rst_off")
    );

    // Poll 2: Offline
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        !orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_rst_off")
    );

    // Poll 3: Restricted B
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_rst_off")
    );

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

    let _consumer_handle =
        EngineOrchestrator::spawn_upload_consumer(Some(mock_backend.clone()), event_tx, upload_rx);

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

    let mut got_clean_log = false;
    while let Ok(Some(ev)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv()).await
    {
        if let AppEvent::Log(entry) = ev
            && entry.message.contains("Cleaned up empty session folder")
        {
            got_clean_log = true;
            break;
        }
    }

    assert!(
        !chunk_path.exists(),
        "Chunk file must be deleted upon upload confirmation"
    );
    assert!(
        !session_dir.exists(),
        "Empty stream session folder must be deleted after broadcast ends and all cleanup tasks are finished"
    );
    assert!(
        got_clean_log,
        "Expected log message indicating session folder cleanup"
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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    // Channel goes CLOSE
    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "CLOSE",
                    "channel": {
                        "channelId": "chan_ended",
                        "channelName": "EndedStreamer"
                    }
                }
            }"#;
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let empty_session_dir = temp_dir.join("chan_ended_20260927_100000");
    fs::create_dir_all(&empty_session_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            poll_interval_seconds: 60,
            ..Default::default()
        },
        channels: vec![chzzk_load::config::ChannelConfig {
            id: "chan_ended".to_string(),
            name: "EndedStreamer".to_string(),
        }],
        ..Default::default()
    };
    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
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

    let mut got_clean_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev
            && entry.message.contains("Cleaned up")
            && entry.message.contains("empty session folder")
        {
            got_clean_log = true;
            break;
        }
    }
    assert!(
        got_clean_log,
        "Expected clean log message for deleted empty session folder"
    );

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
    let chzzk = ChzzkClient::new(&settings.chzzk);
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

    let mock_bin = temp_dir.join(if cfg!(windows) {
        "mock_ffmpeg.exe"
    } else {
        "mock_ffmpeg"
    });
    let src_path = temp_dir.join("mock_ffmpeg.rs");
    std::fs::write(&src_path, r#"
use std::io::Write;
fn main() {
    let stderr = std::io::stderr();
    let mut handle = stderr.lock();
    let _ = writeln!(handle, "[https @ 0xaaaaebe99930] HTTP error 403 Forbidden");
    let _ = writeln!(handle, "[in#0 @ 0xaaaaebdbabe0] Unable to open key file https://api.chzzk.naver.com/service/v1/encryption/lives/21326414/aes_key, Server returned 403 Forbidden (access denied)");
    let _ = writeln!(handle, "[in#0 @ 0xaaaaebdbabe0] Failed to open segment 4896 of playlist 0");
    let _ = writeln!(handle, "[in#0 @ 0xaaaaebdbabe0] Segment 4896 of playlist 0 failed too many times, skipping");
    let _ = handle.flush();
    std::thread::sleep(std::time::Duration::from_secs(60));
}
"#).unwrap();
    let compile_status = std::process::Command::new("rustc")
        .arg(&src_path)
        .arg("-o")
        .arg(&mock_bin)
        .status()
        .expect("Failed to compile mock_ffmpeg");
    assert!(compile_status.success(), "mock_ffmpeg compilation failed");

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = if count == 0 {
                // Poll 2: OPEN broadcast (API cache still OPEN)
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21326414,
                        "status": "OPEN",
                        "liveTitle": "Sports Broadcast (Encrypted)",
                        "channel": { "channelId": "chan_sports", "channelName": "SportsStreamer" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                        "adult": false
                    }
                }"#
            } else {
                // Poll 3: CLOSE (offline)
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21326414,
                        "status": "CLOSE",
                        "liveTitle": "Sports Broadcast (Concluded)",
                        "channel": { "channelId": "chan_sports", "channelName": "SportsStreamer" },
                        "livePlaybackJson": null,
                        "adult": false
                    }
                }"#
            };
            let response = Response::from_string(mock_body).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_sports".to_string(),
            name: "SportsStreamer".to_string(),
        }],
        ..Default::default()
    };
    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
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
        hls_url: "https://test.com/hls.m3u8".to_string(),
        chat_channel_id: None,
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    {
        orchestrator
            .active_recordings()
            .lock()
            .await
            .insert("chan_sports".to_string());
    }
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

    assert!(
        got_error_log,
        "Must emit single error log for restricted stream credentials"
    );
    assert!(got_ended, "Must emit RecordingEnded event");
    assert!(got_live_channel_update, "Must set channel status to LIVE");
    assert!(
        ffmpeg_logs.is_empty(),
        "FFmpeg 403 and segment skipping errors must NOT be forwarded to logs (zero log spam), got: {ffmpeg_logs:?}"
    );

    // Verify channel is removed from active_recordings, and marked in restricted_channels and restricted_live_ids
    assert!(
        !orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_sports"),
        "Channel must not be in active_recordings after 403 detection"
    );
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_sports"),
        "Channel must be in restricted_channels after 403 detection"
    );
    assert_eq!(
        orchestrator
            .restricted_live_ids()
            .lock()
            .await
            .get("chan_sports")
            .copied(),
        Some(21326414),
        "restricted_live_ids must track liveId 21326414"
    );

    // Poll 2: Next polling cycle for the same broadcast
    orchestrator.poll_channels_once(&upload_tx).await;
    // Should NOT spawn another recording session
    assert!(
        !orchestrator
            .active_recordings()
            .lock()
            .await
            .contains("chan_sports"),
        "Channel must not spawn another session for the same restricted liveId"
    );
    assert!(
        orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_sports"),
        "Channel must remain in restricted_channels"
    );

    // Poll 3: Channel goes offline (CLOSE)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(
        !orchestrator
            .restricted_channels()
            .lock()
            .await
            .contains("chan_sports"),
        "Channel must be cleared from restricted_channels on CLOSE"
    );
    assert!(
        !orchestrator
            .restricted_live_ids()
            .lock()
            .await
            .contains_key("chan_sports"),
        "Channel must be cleared from restricted_live_ids on CLOSE"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
