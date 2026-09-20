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
