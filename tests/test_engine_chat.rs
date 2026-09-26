use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Response, Server};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::chzzk::models::LiveStreamInfo;
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

async fn spawn_mock_chat_ws_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                    use futures_util::{SinkExt, StreamExt};
                    // Read CONNECT packet
                    if let Some(Ok(_)) = ws.next().await {
                        let resp = serde_json::json!({
                            "cmd": 10100,
                            "bdy": { "sid": "mock_test_session" }
                        });
                        let _ = ws
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                resp.to_string().into(),
                            ))
                            .await;

                        // Drain until client disconnects
                        while let Some(Ok(_)) = ws.next().await {}
                    }
                }
            });
        }
    });

    (ws_url, handle)
}

#[tokio::test]
async fn test_engine_orchestrator_chat_lifecycle_with_cancel() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_cancel_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let (ws_url, _ws_handle) = spawn_mock_chat_ws_server().await;

    let token_requested = Arc::new(AtomicBool::new(false));
    let token_req_clone = token_requested.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            if request.url().contains("/v1/chats/access-token") {
                token_req_clone.store(true, Ordering::SeqCst);
                let mock_body = serde_json::json!({
                    "code": 200,
                    "message": null,
                    "content": {
                        "accessToken": "mock_access_token_123"
                    }
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(response);
            } else {
                let response = Response::empty(404);
                let _ = request.respond(response);
            }
        }
    });

    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;
    settings.general.chat_flush_interval_seconds = 1;
    settings.channels = vec![ChannelConfig {
        id: "chan_chat_test".to_string(),
        name: "ChatStreamer".to_string(),
    }];

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_game_base_url(format!("http://127.0.0.1:{port}"))
        .with_chat_ws_url(ws_url);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    );

    let info = LiveStreamInfo {
        channel_id: "chan_chat_test".to_string(),
        live_id: Some(99991),
        streamer_name: "ChatStreamer".to_string(),
        title: "Chat Stream Title".to_string(),
        hls_url: "http://127.0.0.1:9999/dummy.m3u8".to_string(),
        chat_channel_id: Some("chat_ch_123".to_string()),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_chat_test".to_string(), info, upload_tx);

    let cancel_clone = cancel_token.clone();
    let token_req_for_cancel = token_requested.clone();
    tokio::spawn(async move {
        for _ in 0..100 {
            if token_req_for_cancel.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel_clone.cancel();
    });

    let mut saw_recording_started = false;
    let mut saw_recording_ended = false;
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(ev) = event_rx.recv() => {
                match ev {
                    AppEvent::RecordingStarted { channel_id, .. } if channel_id == "chan_chat_test" => {
                        saw_recording_started = true;
                    }
                    AppEvent::RecordingEnded { channel_id } if channel_id == "chan_chat_test" => {
                        saw_recording_ended = true;
                        break;
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => break,
        }
    }

    assert!(saw_recording_started, "Expected AppEvent::RecordingStarted");
    assert!(
        saw_recording_ended,
        "Expected AppEvent::RecordingEnded within timeout"
    );
    assert!(
        token_requested.load(Ordering::SeqCst),
        "Expected chat access token to be requested"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_disabled_does_not_request_token() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_disabled_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let token_requested = Arc::new(AtomicBool::new(false));
    let token_req_clone = token_requested.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            if request.url().contains("/v1/chats/access-token") {
                token_req_clone.store(true, Ordering::SeqCst);
                let mock_body = serde_json::json!({
                    "code": 200,
                    "message": null,
                    "content": {
                        "accessToken": "mock_access_token_123"
                    }
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(response);
            }
        }
    });

    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = false; // Chat disabled!
    settings.channels = vec![ChannelConfig {
        id: "chan_no_chat".to_string(),
        name: "NoChatStreamer".to_string(),
    }];

    let chzzk =
        ChzzkClient::new(&settings.chzzk).with_game_base_url(format!("http://127.0.0.1:{port}"));

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    );

    let info = LiveStreamInfo {
        channel_id: "chan_no_chat".to_string(),
        live_id: Some(99992),
        streamer_name: "NoChatStreamer".to_string(),
        title: "No Chat Title".to_string(),
        hls_url: "http://127.0.0.1:9999/dummy.m3u8".to_string(),
        chat_channel_id: Some("chat_ch_999".to_string()),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_no_chat".to_string(), info, upload_tx);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv()).await {
        if let AppEvent::RecordingEnded { .. } = ev {
            break;
        }
    }

    assert!(
        !token_requested.load(Ordering::SeqCst),
        "Chat token must NOT be requested when record_chat is false"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_preserves_local_file_when_no_drive() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_nodrive_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let (ws_url, _ws_handle) = spawn_mock_chat_ws_server().await;

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            if request.url().contains("/v1/chats/access-token") {
                let mock_body = serde_json::json!({
                    "code": 200,
                    "message": null,
                    "content": {
                        "accessToken": "mock_access_token_123"
                    }
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(response);
            }
        }
    });

    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_game_base_url(format!("http://127.0.0.1:{port}"))
        .with_chat_ws_url(ws_url);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None, // No Drive
        event_tx,
        cancel_token.clone(),
    );

    let info = LiveStreamInfo {
        channel_id: "chan_local_chat".to_string(),
        live_id: Some(99993),
        streamer_name: "LocalChatStreamer".to_string(),
        title: "Local Chat Title".to_string(),
        hls_url: "http://127.0.0.1:9999/dummy.m3u8".to_string(),
        chat_channel_id: Some("chat_ch_local".to_string()),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_local_chat".to_string(), info, upload_tx);

    // Wait for the session dir to be created, then create a mock chat.jsonl to simulate captured chat
    let mut session_dir_opt = None;
    for _ in 0..100 {
        if let Ok(mut entries) = tokio::fs::read_dir(&temp_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry
                    .file_type()
                    .await
                    .map(|ft| ft.is_dir())
                    .unwrap_or(false)
                {
                    let p = entry.path();
                    let _ = fs::write(p.join("chat.jsonl"), b"{\"content\":\"hello\"}\n");
                    session_dir_opt = Some(p);
                    break;
                }
            }
        }
        if session_dir_opt.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let session_dir = session_dir_opt.expect("Session directory must be created");

    // Cancel session
    cancel_token.cancel();

    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv()).await {
        if let AppEvent::RecordingEnded { .. } = ev {
            break;
        }
    }

    assert!(
        session_dir.join("chat.jsonl").exists(),
        "chat.jsonl must remain saved locally when Google Drive is disabled"
    );
    assert!(
        session_dir.exists(),
        "Session directory containing chat.jsonl must not be cleaned up as empty"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_uploads_and_deletes_when_drive_enabled() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_drive_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    let drive_server = Server::http("127.0.0.1:0").unwrap();
    let drive_port = drive_server.server_addr().to_ip().unwrap().port();

    let (ws_url, _ws_handle) = spawn_mock_chat_ws_server().await;

    std::thread::spawn(move || {
        while let Ok(request) = chzzk_server.recv() {
            if request.url().contains("/v1/chats/access-token") {
                let mock_body = serde_json::json!({
                    "code": 200,
                    "message": null,
                    "content": {
                        "accessToken": "mock_access_token_123"
                    }
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(response);
            }
        }
    });

    let uploaded_chat = Arc::new(AtomicBool::new(false));
    let uploaded_chat_clone = uploaded_chat.clone();

    std::thread::spawn(move || {
        while let Ok(mut request) = drive_server.recv() {
            let url = request.url().to_string();
            let method = request.method().as_str().to_string();

            if method == "GET" && url.contains("/drive/v3/files") {
                // Folder query
                let mock_resp = serde_json::json!({ "files": [{ "id": "folder_root_123", "name": "chzzk_records" }] });
                let resp = Response::from_string(mock_resp.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(resp);
            } else if method == "POST"
                && url.contains("/upload/drive/v3/files?uploadType=resumable")
            {
                // Resumable upload init for chat.jsonl
                let mut body = String::new();
                let _ = request.as_reader().read_to_string(&mut body);
                let session_url = format!("http://127.0.0.1:{drive_port}/resumable_chat_session");
                let resp = Response::empty(200).with_header(
                    Header::from_bytes(&b"Location"[..], session_url.as_bytes()).unwrap(),
                );
                let _ = request.respond(resp);
            } else if method == "PUT" && url.contains("/resumable_chat_session") {
                // Upload PUT
                uploaded_chat_clone.store(true, Ordering::SeqCst);
                let mock_resp = serde_json::json!({
                    "id": "drive_chat_file_999",
                    "name": "chat.jsonl"
                });
                let resp = Response::from_string(mock_resp.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(resp);
            } else if method == "POST" && url.contains("/drive/v3/files") {
                // Create folder
                let mock_resp = serde_json::json!({ "id": "subfolder_456", "name": "subfolder" });
                let resp = Response::from_string(mock_resp.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(resp);
            } else if method == "PATCH" && url.contains("/upload/drive/v3/files") {
                // Text file upload (e.g. title_history.txt)
                let mock_resp =
                    serde_json::json!({ "id": "title_file_789", "name": "title_history.txt" });
                let resp = Response::from_string(mock_resp.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(resp);
            } else {
                let _ = request.respond(Response::empty(200));
            }
        }
    });

    let auth = create_mock_drive_auth(&temp_dir).await;
    let drive_client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{drive_port}"),
        format!("http://127.0.0.1:{drive_port}"),
    );

    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;
    settings.google_drive.root_folder_name = "chzzk_records".to_string();

    let chzzk = ChzzkClient::new(&settings.chzzk)
        .with_game_base_url(format!("http://127.0.0.1:{chzzk_port}"))
        .with_chat_ws_url(ws_url);

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(drive_client),
        event_tx,
        cancel_token.clone(),
    );

    let info = LiveStreamInfo {
        channel_id: "chan_drive_chat".to_string(),
        live_id: Some(99994),
        streamer_name: "DriveChatStreamer".to_string(),
        title: "Drive Chat Title".to_string(),
        hls_url: "http://127.0.0.1:9999/dummy.m3u8".to_string(),
        chat_channel_id: Some("chat_ch_drive".to_string()),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_drive_chat".to_string(), info, upload_tx);

    // Condition-based wait for session directory to be created
    let mut session_dir_opt = None;
    for _ in 0..100 {
        if let Ok(mut entries) = tokio::fs::read_dir(&temp_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry
                    .file_type()
                    .await
                    .map(|ft| ft.is_dir())
                    .unwrap_or(false)
                {
                    session_dir_opt = Some(entry.path());
                    break;
                }
            }
        }
        if session_dir_opt.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let session_dir = session_dir_opt.expect("Session directory must be created within timeout");
    let chat_file_path = session_dir.join("chat.jsonl");
    fs::write(&chat_file_path, b"{\"content\":\"stream chat message\"}\n")
        .expect("Failed to write mock chat.jsonl");

    cancel_token.cancel();

    let mut saw_drive_uploaded_log = false;
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv()).await {
        match ev {
            AppEvent::Log(ref entry)
                if entry.contains("Uploaded 'chat.jsonl' for chan_drive_chat") =>
            {
                saw_drive_uploaded_log = true;
            }
            AppEvent::RecordingEnded { ref channel_id } if channel_id == "chan_drive_chat" => {
                break;
            }
            _ => {}
        }
    }

    assert!(
        uploaded_chat.load(Ordering::SeqCst),
        "chat.jsonl must be uploaded to Google Drive"
    );
    assert!(
        saw_drive_uploaded_log,
        "Must emit Drive log for chat.jsonl upload"
    );

    assert!(
        !chat_file_path.exists(),
        "chat.jsonl must be deleted locally upon confirmed upload to maintain strictly bounded disk footprint"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
