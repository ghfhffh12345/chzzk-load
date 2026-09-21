use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tiny_http::{Header, Response, Server, StatusCode};
use tokio::sync::mpsc;

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::drive::auth::{DriveAuth, StoredToken};
use chzzk_load::drive::client::DriveClient;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::event::AppEvent;
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
    fs::write(&token_path, serde_json::to_string_pretty(&token_data).unwrap()).unwrap();

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("Failed to initialize mock DriveAuth");

    Arc::new(auth)
}

#[tokio::test]
async fn test_app_event_mpsc_channel() {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(10);
    tx.send(AppEvent::Log("Test log".to_string())).await.unwrap();

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
            chunk_name: "chunk_0000.ts".to_string(),
            uploaded_bytes: 524288,
            total_bytes: 1048576,
            speed_mb_s: 10.5,
        },
        AppEvent::UploadCompleted {
            chunk_name: "chunk_0000.ts".to_string(),
            reclaimed_bytes: 1048576,
        },
        AppEvent::Log("Info message".to_string()),
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
            let response = Response::from_string(mock_body)
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_base_url(format!("http://127.0.0.1:{}", port));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(10);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);
    orchestrator.poll_channels_once(&upload_tx).await;

    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::ChannelUpdate { channel_id, channel_name, is_live, title } => {
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
            let response = Response::from_string("internal error")
                .with_status_code(StatusCode(500));
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

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_base_url(format!("http://127.0.0.1:{}", port));

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
            let response = Response::from_string(mock_body)
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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
            let response = Response::from_string(mock_body)
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_base_url(format!("http://127.0.0.1:{}", port));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: transitions to live and starts recording session
    orchestrator.poll_channels_once(&upload_tx).await;

    // ChannelUpdate event received
    let event = event_rx.recv().await.unwrap();
    match event {
        AppEvent::ChannelUpdate { channel_id, channel_name, is_live, title } => {
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
        AppEvent::RecordingStarted { channel_id, session_title } => {
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
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_secs(2), event_rx.recv()).await {
        if let AppEvent::ChannelUpdate { channel_id, is_live, .. } = ev {
            assert_eq!(channel_id, "chan_live");
            assert!(is_live);
            got_second_channel_update = true;
            break;
        }
    }
    assert!(got_second_channel_update, "Expected second ChannelUpdate event");

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
                0 => r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "OPEN",
                        "liveTitle": "Stream A",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#,
                // Poll 2: Stream ended, but CDN/cache still reports OPEN with same liveId 21212268!
                1 => r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "OPEN",
                        "liveTitle": "Stream A",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#,
                // Poll 3: CDN cache finally clears to CLOSE
                2 => r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212268,
                        "status": "CLOSE",
                        "liveTitle": null,
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": null
                    }
                }"#,
                // Poll 4: Genuinely new stream starts with new liveId 21212269
                _ => r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21212269,
                        "status": "OPEN",
                        "liveTitle": "Stream B (New)",
                        "channel": { "channelId": "chan_race", "channelName": "StreamerRace" },
                        "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[]}]}"
                    }
                }"#,
            };
            let response = Response::from_string(mock_body)
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_base_url(format!("http://127.0.0.1:{}", port));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(50);
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // --- Poll 1: Stream goes live -> starts session 1 ---
    orchestrator.poll_channels_once(&upload_tx).await;

    // Collect ChannelUpdate and RecordingStarted for session 1
    let mut recording_started_count = 0;
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await {
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
        orchestrator.register_finished_session("chan_race", Some(21212268)).await;
    }

    // --- Poll 2: API still returns OPEN with liveId 21212268 (stale CDN cache) ---
    orchestrator.poll_channels_once(&upload_tx).await;

    // Verify NO second recording session was started!
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await {
        if let AppEvent::RecordingStarted { .. } = ev {
            panic!("BUG: A duplicate recording session was spawned for the same liveId / during cooldown!");
        }
    }
    {
        let active = orchestrator.active_recordings();
        assert!(!active.lock().await.contains("chan_race"), "Channel must not be in active_recordings");
    }

    // --- Poll 3: API transitions to CLOSE (channel offline) ---
    orchestrator.poll_channels_once(&upload_tx).await;
    let mut offline_detected = false;
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await {
        if let AppEvent::ChannelUpdate { is_live, .. } = ev
            && !is_live
        {
            offline_detected = true;
        }
    }
    assert!(offline_detected, "Channel should be marked offline upon CLOSE");

    // --- Poll 4: Genuinely new stream starts with new liveId 21212269 ---
    orchestrator.poll_channels_once(&upload_tx).await;
    let mut session_2_started = false;
    while let Ok(Some(ev)) = tokio::time::timeout(std::time::Duration::from_millis(500), event_rx.recv()).await {
        if let AppEvent::RecordingStarted { session_title, .. } = ev {
            assert_eq!(session_title, "Stream B (New)");
            session_2_started = true;
        }
    }
    assert!(session_2_started, "Session 2 should start for new liveId 21212269");

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
            let response = Response::from_string("")
                .with_header(Header::from_bytes(&b"Location"[..], session_url_clone.as_bytes()).unwrap());
            let _ = req.respond(response);
        }

        // 2. Stream byte chunk upload
        if let Ok(req) = server.recv() {
            assert_eq!(req.method().as_str(), "PUT");
            let response_body = serde_json::json!({
                "id": "file_uploaded_test_123",
                "name": "chunk_0000.ts"
            });
            let response = Response::from_string(response_body.to_string())
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
            let _ = req.respond(response);
        }
    });

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);

    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer(
        Some(client),
        event_tx,
        upload_rx,
    );

    // Create a local chunk file
    let chunk_path = temp_dir.join("chunk_0000.ts");
    {
        let mut f = File::create(&chunk_path).unwrap();
        f.write_all(&vec![0u8; 1024 * 1024]).unwrap(); // 1MB
    }
    assert!(chunk_path.exists());

    // Submit upload task
    upload_tx.send(UploadTask {
        session_folder_id: "folder_abc".to_string(),
        chunk_path: chunk_path.clone(),
        chunk_name: "chunk_0000.ts".to_string(),
    }).await.unwrap();

    // Consume events until UploadCompleted
    let mut got_completed = false;
    let mut got_clean_log = false;

    while let Some(ev) = event_rx.recv().await {
        match ev {
            AppEvent::UploadProgress { chunk_name, uploaded_bytes, total_bytes, .. } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(total_bytes, 1024 * 1024);
                assert!(uploaded_bytes > 0);
            }
            AppEvent::UploadCompleted { chunk_name, reclaimed_bytes } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(reclaimed_bytes, 1024 * 1024);
                got_completed = true;
            }
            AppEvent::Log(msg)
                if msg.contains("[CLEAN] Uploaded & deleted chunk_0000.ts") =>
            {
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

    let _consumer_handle = EngineOrchestrator::spawn_upload_consumer(
        Some(client),
        event_tx,
        upload_rx,
    );

    // Create a local chunk file
    let chunk_path = temp_dir.join("chunk_fail.ts");
    {
        let mut f = File::create(&chunk_path).unwrap();
        f.write_all(b"important video content").unwrap();
    }
    assert!(chunk_path.exists());

    // Submit upload task
    upload_tx.send(UploadTask {
        session_folder_id: "folder_xyz".to_string(),
        chunk_path: chunk_path.clone(),
        chunk_name: "chunk_fail.ts".to_string(),
    }).await.unwrap();

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
    let temp_dir = std::env::temp_dir().join(format!("test_chunk_no_drive_{}", rand::random::<u32>()));
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
            AppEvent::ChunkSealed { chunk_name, size_bytes } => {
                assert_eq!(chunk_name, "chunk_0000.ts");
                assert_eq!(size_bytes, 17);
                got_chunk_sealed = true;
            }
            AppEvent::Log(msg)
                if msg == "[REC] chunk_0000.ts sealed (saved locally)." =>
            {
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
    let temp_dir = std::env::temp_dir().join(format!("test_chunk_retry_ok_{}", rand::random::<u32>()));
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
            let response = Response::from_string(body.to_string())
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
            let _ = req.respond(response);
        }

        // 2. Subfolder query
        if let Ok(req) = server.recv() {
            let body = serde_json::json!({
                "files": [{"id": "sub_456", "name": "Subfolder"}]
            });
            let response = Response::from_string(body.to_string())
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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
    let temp_dir = std::env::temp_dir().join(format!("test_chunk_retry_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(req) = server.recv() {
            let response = Response::from_string("internal error").with_status_code(StatusCode(500));
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
    let temp_dir = std::env::temp_dir().join(format!("test_chunk_existing_{}", rand::random::<u32>()));
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
        &upload_tx,
        &event_tx,
    )
    .await;

    assert_eq!(session_folder_id, Some("already_ready_folder_999".to_string()));

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
            let response = Response::from_string(mock_body)
                .with_header(Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
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
    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_base_url(format!("http://127.0.0.1:{}", port));
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


