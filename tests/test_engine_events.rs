use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Response, Server, StatusCode};
use tokio::sync::mpsc;

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::drive::auth::{DriveAuth, StoredToken};
use chzzk_load::drive::client::DriveClient;
use chzzk_load::engine::{ActiveSessionState, EngineOrchestrator};
use chzzk_load::tui::event::{AppEvent, LogEntry};
use chzzk_load::uploader::UploadTask;

async fn create_mock_drive_auth(temp_dir: &Path) -> Arc<DriveAuth> {
    let cred_path = temp_dir.join("mock_credentials.json");
    let token_path = temp_dir.join("mock_token.json");

    let cred_json = r#"{
        "installed": {
            "client_id": "mock_client_id",
            "client_secret": "mock_client_secret",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token"
        }
    }"#;
    fs::write(&cred_path, cred_json).unwrap();

    let future_expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    let token_data = StoredToken {
        access_token: "mock_test_token_xyz".to_string(),
        refresh_token: Some("mock_refresh_xyz".to_string()),
        expires_at_epoch_sec: future_expiry,
    };
    fs::write(
        &token_path,
        serde_json::to_string_pretty(&token_data).unwrap(),
    )
    .unwrap();

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("Failed to initialize mock DriveAuth");

    Arc::new(auth)
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

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));

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
        other => panic!("Expected ChannelUpdate event, got: {:?}", other),
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

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);
    orchestrator.poll_channels_once(&upload_tx).await;

    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::Log(msg) => {
            assert!(msg.starts_with("[WARN] Polling failed for chan_err"));
        }
        other => panic!("Expected Log event, got: {:?}", other),
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

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));

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
        other => panic!("Expected ChannelUpdate event, got: {:?}", other),
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
        other => panic!("Expected RecordingStarted event, got: {:?}", other),
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

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));

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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    let upload_session_url = format!("http://127.0.0.1:{}/resumable_upload_session", port);
    let session_url_clone = upload_session_url.clone();

    std::thread::spawn(move || {
        // 1. Init resumable upload request
        if let Ok(req) = server.recv() {
            assert_eq!(req.method().as_str(), "POST");
            let response = Response::from_string("").with_header(
                Header::from_bytes(&b"Location"[..], session_url_clone.as_bytes()).unwrap(),
            );
            let _ = req.respond(response);
        }

        // 2. Stream byte chunk upload
        if let Ok(req) = server.recv() {
            assert_eq!(req.method().as_str(), "PUT");
            let response_body = serde_json::json!({
                "id": "file_uploaded_test_123",
                "name": "chunk_0000.ts"
            });
            let response = Response::from_string(response_body.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = req.respond(response);
        }
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle =
        EngineOrchestrator::spawn_upload_consumer(Some(client), event_tx, upload_rx);

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
            AppEvent::Log(msg) if msg.contains("[CLEAN] Uploaded & deleted chunk_0000.ts") => {
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

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_upload_consumer_handles_failure() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        if let Ok(req) = server.recv() {
            let response = Response::from_string("server error").with_status_code(StatusCode(500));
            let _ = req.respond(response);
        }
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle =
        EngineOrchestrator::spawn_upload_consumer(Some(client), event_tx, upload_rx);

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
            && msg.contains("[ERROR] Upload failed for chunk_fail.ts")
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
    let mut session_folder_id: Option<String> = None;

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        &mut session_folder_id,
        None,
        "ChzzkRecordings",
        "Streamer - Title",
        "chan_local",
        "Streamer",
        &upload_tx,
        &event_tx,
    )
    .await;

    assert!(session_folder_id.is_none());
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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    // Spawn server responding to root folder query and subfolder query
    std::thread::spawn(move || {
        // 1. Root folder query
        if let Ok(req) = server.recv() {
            let body = serde_json::json!({
                "files": [{"id": "root_123", "name": "ChzzkRecordings"}]
            });
            let response = Response::from_string(body.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = req.respond(response);
        }

        // 2. Subfolder query
        if let Ok(req) = server.recv() {
            let body = serde_json::json!({
                "files": [{"id": "sub_456", "name": "Subfolder"}]
            });
            let response = Response::from_string(body.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = req.respond(response);
        }
    });

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    let chunk_path = temp_dir.join("chunk_0001.ts");
    fs::write(&chunk_path, b"test chunk content").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);
    let mut session_folder_id: Option<String> = None;

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        &mut session_folder_id,
        Some(&drive),
        "ChzzkRecordings",
        "Subfolder",
        "chan_sub",
        "StreamerSub",
        &upload_tx,
        &event_tx,
    )
    .await;

    assert_eq!(session_folder_id, Some("sub_456".to_string()));

    // Verify task in upload_tx
    let task = upload_rx.recv().await.expect("Expected UploadTask");
    assert_eq!(task.session_folder_id, "sub_456");
    assert_eq!(task.chunk_name, "chunk_0001.ts");

    let mut got_pushed_log = false;
    let mut got_drive_ready_log = false;

    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev {
            if msg == "[REC] chunk_0001.ts sealed. Pushed to Drive upload queue." {
                got_pushed_log = true;
            }
            if msg.contains("[DRIVE] Session folder ready:") {
                got_drive_ready_log = true;
            }
        }
    }

    assert!(got_drive_ready_log);
    assert!(got_pushed_log);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_process_sealed_chunk_retry_drive_failure() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_chunk_retry_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(req) = server.recv() {
            let response =
                Response::from_string("internal error").with_status_code(StatusCode(500));
            let _ = req.respond(response);
        }
    });

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    let chunk_path = temp_dir.join("chunk_0002.ts");
    fs::write(&chunk_path, b"test video data").unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);
    let mut session_folder_id: Option<String> = None;

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        &mut session_folder_id,
        Some(&drive),
        "ChzzkRecordings",
        "Subfolder",
        "chan_fail_test",
        "StreamerFail",
        &upload_tx,
        &event_tx,
    )
    .await;

    assert!(session_folder_id.is_none());
    assert!(upload_rx.try_recv().is_err());

    let mut got_saved_locally_log = false;
    let mut got_warn_log = false;

    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev {
            if msg == "[REC] chunk_0002.ts sealed (saved locally)." {
                got_saved_locally_log = true;
            }
            if msg.contains("[WARN] Failed to access Drive root folder:") {
                got_warn_log = true;
            }
        }
    }

    assert!(got_warn_log);
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
    let mut session_folder_id = Some("already_ready_folder_999".to_string());

    EngineOrchestrator::process_sealed_chunk(
        &chunk_path,
        &mut session_folder_id,
        None,
        "ChzzkRecordings",
        "Subfolder",
        "chan_exist",
        "StreamerExist",
        &upload_tx,
        &event_tx,
    )
    .await;

    assert_eq!(
        session_folder_id,
        Some("already_ready_folder_999".to_string())
    );

    let task = upload_rx.recv().await.expect("Expected UploadTask");
    assert_eq!(task.session_folder_id, "already_ready_folder_999");
    assert_eq!(task.chunk_name, "chunk_0003.ts");

    let mut got_pushed_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg == "[REC] chunk_0003.ts sealed. Pushed to Drive upload queue."
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
    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));
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
    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));
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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    let (task2_started_tx, mut task2_started_rx) = mpsc::channel::<()>(1);
    let (allow_task1_finish_tx, mut allow_task1_finish_rx) = mpsc::channel::<()>(1);

    std::thread::spawn(move || {
        let mut put_reqs = Vec::new();
        // 4 requests expected: 2 POST inits + 2 PUT uploads
        for _ in 0..4 {
            if let Ok(mut req) = server.recv() {
                if req.method().as_str() == "POST" {
                    let mut body = String::new();
                    req.as_reader().read_to_string(&mut body).unwrap();
                    let chunk_id = if body.contains("chunk_chan1.ts") {
                        "1"
                    } else {
                        "2"
                    };
                    if chunk_id == "2" {
                        let _ = task2_started_tx.try_send(());
                    }
                    let session_url =
                        format!("http://127.0.0.1:{}/resumable_session_{}", port, chunk_id);
                    let response = Response::empty(200).with_header(
                        Header::from_bytes(&b"Location"[..], session_url.as_bytes()).unwrap(),
                    );
                    let _ = req.respond(response);
                } else if req.method().as_str() == "PUT" {
                    if req.url().contains("resumable_session_1") {
                        // Hold PUT for chunk 1 until chunk 2 has started!
                        put_reqs.push(req);
                    } else {
                        // Chunk 2 PUT
                        let mock_body = serde_json::json!({
                            "id": "file_2",
                            "name": "chunk_chan2.ts"
                        });
                        let response = Response::from_string(mock_body.to_string()).with_header(
                            Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                                .unwrap(),
                        );
                        let _ = req.respond(response);
                    }
                }
            }
        }

        // Wait for signal that task 2 was verified running concurrently before releasing task 1
        let _ = allow_task1_finish_rx.blocking_recv();
        for req in put_reqs {
            let mock_body = serde_json::json!({
                "id": "file_1",
                "name": "chunk_chan1.ts"
            });
            let response = Response::from_string(mock_body.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = req.respond(response);
        }
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer_with_concurrency(
        Some(client),
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
            chunk_path: chunk_path2.clone(),
            chunk_name: "chunk_chan2.ts".to_string(),
            streamer_name: "Streamer 2".to_string(),
        })
        .await
        .unwrap();

    // Verify task 2 starts POST request while task 1 is still in flight (holding PUT 1)!
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

    let drive_server = Server::http("127.0.0.1:0").unwrap();
    let drive_port = drive_server.server_addr().to_ip().unwrap().port();

    let renamed_new_name = Arc::new(std::sync::Mutex::new(None));
    let renamed_clone = renamed_new_name.clone();

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

    std::thread::spawn(move || {
        // Handle Drive requests: Expect a PATCH to /drive/v3/files/session_folder_777 and any title_history calls
        while let Ok(mut req) = drive_server.recv() {
            let method = req.method().as_str().to_string();
            let url = req.url().to_string();

            if method == "PATCH" && url.contains("/drive/v3/files/session_folder_777") {
                let mut body = String::new();
                req.as_reader().read_to_string(&mut body).unwrap();
                let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
                let name = parsed["name"].as_str().unwrap().to_string();
                *renamed_clone.lock().unwrap() = Some(name.clone());

                let resp_body = serde_json::json!({
                    "id": "session_folder_777",
                    "name": name
                });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if method == "GET" && url.contains("/drive/v3/files") {
                let resp_body = serde_json::json!({ "files": [] });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if (method == "POST" && url.contains("/drive/v3/files"))
                || (method == "PATCH" && url.contains("/upload/drive/v3/files"))
            {
                let resp_body =
                    serde_json::json!({ "id": "hist_123", "name": "title_history.txt" });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else {
                let response = Response::from_string(r#"{"error":"Not handled"}"#)
                    .with_status_code(StatusCode(400));
                let _ = req.respond(response);
            }
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
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", chzzk_port));

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", drive_port),
        format!("http://127.0.0.1:{}", drive_port),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, Some(drive), event_tx);

    // Initialize active recording state with existing session folder
    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_rename".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_rename".to_string(),
            chzzk_load::engine::ActiveSessionState {
                start_timestamp: "2026-09-22_1000".to_string(),
                streamer_name: "RenameStreamer".to_string(),
                current_title: "Initial Stream Title".to_string(),
                session_folder_id: Some("session_folder_777".to_string()),
                ..Default::default()
            },
        );
    }

    // Poll 1: Channel is polled with same initial title (no rename expected)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(renamed_new_name.lock().unwrap().is_none());

    // Poll 2: Streamer changed title to "Updated Stream Title" (triggers Drive folder rename)
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify Drive rename request was made
    let renamed = renamed_new_name.lock().unwrap().clone();
    assert!(
        renamed.is_some(),
        "Drive PATCH rename request was not received!"
    );
    let new_folder_name = renamed.unwrap();
    assert!(
        new_folder_name.contains("RenameStreamer - Updated Stream Title? Playing Now?"),
        "Expected folder name to contain 'RenameStreamer - Updated Stream Title? Playing Now?', got: {}",
        new_folder_name
    );

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

    let drive_server = Server::http("127.0.0.1:0").unwrap();
    let drive_port = drive_server.server_addr().to_ip().unwrap().port();

    let uploaded_history_content = Arc::new(std::sync::Mutex::new(None));
    let history_clone = uploaded_history_content.clone();

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

    std::thread::spawn(move || {
        while let Ok(mut req) = drive_server.recv() {
            let method = req.method().as_str().to_string();
            let url = req.url().to_string();

            if method == "PATCH" && url.contains("/drive/v3/files/session_folder_777") {
                // Folder rename
                let resp_body = serde_json::json!({
                    "id": "session_folder_777",
                    "name": "Renamed"
                });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if method == "GET"
                && url.contains("/drive/v3/files")
                && url.contains("title_history.txt")
            {
                // Check if title_history.txt exists
                let resp_body = serde_json::json!({ "files": [] });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if method == "POST" && url.contains("/drive/v3/files") {
                // Create title_history.txt metadata
                let resp_body = serde_json::json!({
                    "id": "title_history_file_555",
                    "name": "title_history.txt"
                });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if method == "PATCH"
                && url.contains("/upload/drive/v3/files/title_history_file_555")
            {
                // Media upload PATCH
                let mut body = String::new();
                req.as_reader().read_to_string(&mut body).unwrap();
                *history_clone.lock().unwrap() = Some(body.clone());

                let resp_body = serde_json::json!({
                    "id": "title_history_file_555",
                    "name": "title_history.txt"
                });
                let response = Response::from_string(resp_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
                break;
            } else {
                let response = Response::from_string(r#"{"error":"Not handled"}"#)
                    .with_status_code(StatusCode(400));
                let _ = req.respond(response);
            }
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
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", chzzk_port));

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", drive_port),
        format!("http://127.0.0.1:{}", drive_port),
    );

    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, Some(drive), event_tx);

    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_rename".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_rename".to_string(),
            chzzk_load::engine::ActiveSessionState {
                start_timestamp: "2026-09-22_1000".to_string(),
                streamer_name: "RenameStreamer".to_string(),
                current_title: "Initial Stream Title".to_string(),
                session_folder_id: Some("session_folder_777".to_string()),
                title_history: vec![(
                    "2026-09-22 10:00:00".to_string(),
                    "Initial Stream Title".to_string(),
                )],
                title_history_file_id: None,
            },
        );
    }

    // Poll 1: Channel is polled with same initial title (no rename or history upload)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(uploaded_history_content.lock().unwrap().is_none());

    // Poll 2: Streamer changed title to "Updated Stream Title" (triggers Drive upload of title_history.txt)
    orchestrator.poll_channels_once(&upload_tx).await;

    let content_opt = uploaded_history_content.lock().unwrap().clone();
    assert!(
        content_opt.is_some(),
        "title_history.txt upload was not received by Drive mock!"
    );
    let content = content_opt.unwrap();
    assert!(content.contains("[2026-09-22 10:00:00] Initial Stream Title"));
    assert!(content.contains("Updated Stream Title? Playing Now?"));

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_rename").unwrap();
        assert_eq!(
            session.title_history_file_id,
            Some("title_history_file_555".to_string())
        );
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

        // Poll 2: Title changes to "Early Title 2" before folder is created
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
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", chzzk_port));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Initialize active recording state before Drive folder is created
    {
        let active = orchestrator.active_recordings();
        active.lock().await.insert("chan_pre".to_string());

        let sessions = orchestrator.active_sessions();
        sessions.lock().await.insert(
            "chan_pre".to_string(),
            chzzk_load::engine::ActiveSessionState {
                start_timestamp: "2026-09-22_1000".to_string(),
                streamer_name: "PreStreamer".to_string(),
                current_title: "Early Title 1".to_string(),
                session_folder_id: None,
                ..Default::default()
            },
        );
    }

    // Poll 1: Session starts with Early Title 1 (already active, no title change)
    orchestrator.poll_channels_once(&upload_tx).await;

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_pre").expect("Session should exist");
        assert_eq!(session.current_title, "Early Title 1");
        assert!(session.session_folder_id.is_none());
    }

    // Poll 2: Title changes to Early Title 2 before folder creation
    orchestrator.poll_channels_once(&upload_tx).await;

    {
        let sessions = orchestrator.active_sessions();
        let guard = sessions.lock().await;
        let session = guard.get("chan_pre").expect("Session should exist");
        assert_eq!(session.current_title, "Early Title 2? Pending?");
        assert!(session.session_folder_id.is_none());
    }

    let mut got_pending_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(msg) = ev
            && msg.contains("Pending folder name updated")
            && msg.contains("Early Title 2? Pending?")
        {
            got_pending_log = true;
        }
    }
    assert!(got_pending_log, "Expected pending folder name log message");

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
        current_title: "Is this live? Yes! Special: 100% <Stream>".to_string(),
        session_folder_id: None,
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
        current_title: "What's Next? Let's Play | Ep. 1 *Final*".to_string(),
        session_folder_id: Some("folder_123".to_string()),
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

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", port));

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

    let drive_server = Server::http("127.0.0.1:0").unwrap();
    let drive_port = drive_server.server_addr().to_ip().unwrap().port();

    let upload_url = format!("http://127.0.0.1:{}/resumable_chunk_upload", drive_port);
    let upload_url_clone = upload_url.clone();

    std::thread::spawn(move || {
        while let Ok(req) = drive_server.recv() {
            let path = req.url().to_string();
            if req.method().as_str() == "GET" && path.contains("/drive/v3/files") {
                let response = Response::from_string(
                    serde_json::json!({
                        "files": [{ "id": "mock_folder_111", "name": "Chzzk_Recordings" }]
                    })
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "POST" && path.contains("uploadType=resumable") {
                let response = Response::empty(200).with_header(
                    Header::from_bytes(&b"Location"[..], upload_url_clone.as_bytes()).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "PUT" && path.contains("resumable_chunk_upload") {
                // Simulate an upload that takes 11 seconds (exceeding old 10s timeout)
                std::thread::sleep(std::time::Duration::from_millis(11000));
                let response = Response::from_string(
                    serde_json::json!({
                        "id": "uploaded_chunk_id",
                        "name": "chunk_0000.ts"
                    })
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "POST" && path.contains("/drive/v3/files") {
                let response = Response::from_string(
                    serde_json::json!({
                        "id": "mock_subfolder_222",
                        "name": "mock_subfolder"
                    })
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else {
                let response = Response::empty(200);
                let _ = req.respond(response);
            }
        }
    });

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive_client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", drive_port),
        format!("http://127.0.0.1:{}", drive_port),
    );

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
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", chzzk_port));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(drive_client),
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

    // Await run_handle with 20-second timeout (allowing 11s upload + cleanup)
    let res = tokio::time::timeout(std::time::Duration::from_secs(20), run_handle).await;
    assert!(
        res.is_ok(),
        "Engine run_handle timed out before completing graceful shutdown!"
    );

    // CRITICAL: At the exact moment EngineOrchestrator::run() finishes, the chunk upload
    // MUST have completed and the chunk must already be deleted from local disk!
    // In the broken code, EngineOrchestrator::run() prematurely exits at 10 seconds,
    // leaving the chunk still on disk and still uploading when the engine shuts down.
    assert!(
        !chunk_path.exists(),
        "Chunk file must already be uploaded and deleted when EngineOrchestrator::run completes! (Engine shut down prematurely while upload was still in progress)"
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

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    let (task2_unexpected_started_tx, mut task2_unexpected_started_rx) = mpsc::channel::<()>(1);
    let (allow_task1_finish_tx, mut allow_task1_finish_rx) = mpsc::channel::<()>(1);

    let put_task1_slot: Arc<std::sync::Mutex<Option<tiny_http::Request>>> =
        Arc::new(std::sync::Mutex::new(None));
    let put_task1_slot_clone = put_task1_slot.clone();

    std::thread::spawn(move || {
        let _ = allow_task1_finish_rx.blocking_recv();
        for _ in 0..100 {
            let maybe_req = put_task1_slot_clone.lock().unwrap().take();
            if let Some(r) = maybe_req {
                let mock_body = serde_json::json!({
                    "id": "file_task1",
                    "name": "chunk_0000.ts"
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = r.respond(response);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    std::thread::spawn(move || {
        // Process requests from client
        while let Ok(mut req) = server.recv() {
            if req.method().as_str() == "POST" {
                let mut body = String::new();
                req.as_reader().read_to_string(&mut body).unwrap();
                let is_task2 = body.contains("chunk_0001.ts");
                if is_task2 {
                    // Task 2 started while Task 1 is still in flight!
                    let _ = task2_unexpected_started_tx.try_send(());
                }

                let chunk_id = if is_task2 { "2" } else { "1" };
                let session_url = format!("http://127.0.0.1:{}/serial_session_{}", port, chunk_id);
                let response = Response::empty(200).with_header(
                    Header::from_bytes(&b"Location"[..], session_url.as_bytes()).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "PUT" {
                if req.url().contains("serial_session_1") {
                    // Task 1 PUT arrived; hold it in slot without blocking the server loop
                    *put_task1_slot.lock().unwrap() = Some(req);
                } else if req.url().contains("serial_session_2") {
                    let mock_body = serde_json::json!({
                        "id": "file_task2",
                        "name": "chunk_0001.ts"
                    });
                    let response = Response::from_string(mock_body.to_string()).with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    );
                    let _ = req.respond(response);
                    break;
                }
            }
        }
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    // Concurrency is set to 3, but both tasks belong to the SAME channel
    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer_with_concurrency(
        Some(client),
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
            chunk_path: chunk_path2.clone(),
            chunk_name: "chunk_0001.ts".to_string(),
            streamer_name: "SameStreamer".to_string(),
        })
        .await
        .unwrap();

    // Check if task 2 was prematurely started while task 1 is in-flight.
    // If it started, task2_unexpected_started_rx will receive a signal within 500ms.
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

    let drive_server = Server::http("127.0.0.1:0").unwrap();
    let drive_port = drive_server.server_addr().to_ip().unwrap().port();

    let (task2_unexpected_started_tx, mut task2_unexpected_started_rx) = mpsc::channel::<()>(1);
    let (chunk0_in_flight_tx, mut chunk0_in_flight_rx) = mpsc::channel::<()>(1);
    let (allow_chunk0_finish_tx, mut allow_chunk0_finish_rx) = mpsc::channel::<()>(1);

    let put_chunk0_slot: Arc<std::sync::Mutex<Option<tiny_http::Request>>> =
        Arc::new(std::sync::Mutex::new(None));
    let put_chunk0_slot_clone = put_chunk0_slot.clone();

    std::thread::spawn(move || {
        let _ = allow_chunk0_finish_rx.blocking_recv();
        for _ in 0..100 {
            let maybe_req = put_chunk0_slot_clone.lock().unwrap().take();
            if let Some(r) = maybe_req {
                let mock_body = serde_json::json!({
                    "id": "uploaded_chunk0_id",
                    "name": "chunk_0000.ts"
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = r.respond(response);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    let chunk0_active = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let chunk0_active_server = chunk0_active.clone();

    std::thread::spawn(move || {
        while let Ok(mut req) = drive_server.recv() {
            let path = req.url().to_string();
            if req.method().as_str() == "GET" && path.contains("/drive/v3/files") {
                let response = Response::from_string(
                    serde_json::json!({
                        "files": [{ "id": "mock_folder_111", "name": "Chzzk_Recordings" }]
                    })
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "POST" && path.contains("uploadType=resumable") {
                let mut body = String::new();
                req.as_reader().read_to_string(&mut body).unwrap();
                let is_task2 = body.contains("chunk_0001.ts");
                if is_task2 && chunk0_active_server.load(std::sync::atomic::Ordering::SeqCst) {
                    let _ = task2_unexpected_started_tx.try_send(());
                }

                let session_id = if is_task2 {
                    "session_chunk_1"
                } else {
                    "session_chunk_0"
                };
                let session_url = format!("http://127.0.0.1:{}/{}", drive_port, session_id);
                let response = Response::empty(200).with_header(
                    Header::from_bytes(&b"Location"[..], session_url.as_bytes()).unwrap(),
                );
                let _ = req.respond(response);
            } else if req.method().as_str() == "PUT" {
                if path.contains("session_chunk_0") {
                    chunk0_active_server.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _ = chunk0_in_flight_tx.try_send(());
                    *put_chunk0_slot.lock().unwrap() = Some(req);
                } else if path.contains("session_chunk_1") {
                    if chunk0_active_server.load(std::sync::atomic::Ordering::SeqCst) {
                        let _ = task2_unexpected_started_tx.try_send(());
                    }
                    let response = Response::from_string(
                        serde_json::json!({
                            "id": "uploaded_chunk1_id",
                            "name": "chunk_0001.ts"
                        })
                        .to_string(),
                    )
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    );
                    let _ = req.respond(response);
                }
            } else if req.method().as_str() == "POST" && path.contains("/drive/v3/files") {
                let response = Response::from_string(
                    serde_json::json!({
                        "id": "mock_subfolder_222",
                        "name": "mock_subfolder"
                    })
                    .to_string(),
                )
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = req.respond(response);
            } else {
                let response = Response::empty(200);
                let _ = req.respond(response);
            }
        }
    });

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive_client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", drive_port),
        format!("http://127.0.0.1:{}", drive_port),
    );

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
        ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{}", chzzk_port));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(drive_client),
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

    // Wait until chunk 0 starts uploading and reaches in-flight state (PUT request held)
    let in_flight = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        chunk0_in_flight_rx.recv(),
    )
    .await;
    assert!(
        in_flight.is_ok(),
        "chunk_0000.ts should have started uploading and reached in-flight PUT"
    );

    // Cancel while chunk 0 is actively uploading and chunk 1 is pending as final chunk
    cancel_token.cancel();

    // Verify chunk 1 is NOT started prematurely while chunk 0 is still in flight.
    // FFmpeg stop takes up to 3.5s to exit cleanly and seal the final chunk.
    let task2_started_prematurely = tokio::time::timeout(
        std::time::Duration::from_millis(4000),
        task2_unexpected_started_rx.recv(),
    )
    .await;

    assert!(
        task2_started_prematurely.is_err(),
        "chunk_0001.ts must NOT start uploading concurrently while chunk_0000.ts is still in-flight on termination!"
    );

    // Allow chunk 0 to finish
    chunk0_active.store(false, std::sync::atomic::Ordering::SeqCst);
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
