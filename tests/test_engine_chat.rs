mod common;

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tiny_http::{Header, Response, Server};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::chzzk::models::LiveStreamInfo;
use chzzk_load::chzzk::source::MockLiveStreamSource;
use chzzk_load::config::{ChannelConfig, GeneralConfig, Settings};
use chzzk_load::engine::{ChannelLifecycleRegistry, EngineOrchestrator, RecordingSession};
use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::{MockUploadBackend, UploadTask};
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::observability::{TestLogRecorder, assert_with_logs, expect_with_logs};

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
    let mock_bin = get_mock_ffmpeg_bin();
    let hls_url = "http://127.0.0.1:0/dummy.m3u8".to_string();

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
    settings.channels = vec![ChannelConfig::with_alias("chan_chat_test", "ChatStreamer")];

    let chzzk = Arc::new(
        ChzzkClient::new(&settings.chzzk)
            .with_game_base_url(format!("http://127.0.0.1:{port}"))
            .with_chat_ws_url(ws_url),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    )
    .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let info = LiveStreamInfo {
        channel_id: "chan_chat_test".to_string(),
        live_id: Some(99991),
        streamer_name: "ChatStreamer".to_string(),
        title: "Chat Stream Title".to_string(),
        hls_url,
        chat_channel_id: Some("chat_ch_123".to_string()),
        metadata: Default::default(),
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

    let mut recorder = TestLogRecorder::new();
    let mut saw_recording_started = false;
    let mut saw_recording_ended = false;
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(ev) = event_rx.recv() => {
                recorder.record(&ev);
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

    assert_with_logs(
        saw_recording_started,
        "Expected AppEvent::RecordingStarted",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        saw_recording_ended,
        "Expected AppEvent::RecordingEnded within timeout",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        token_requested.load(Ordering::SeqCst),
        "Expected chat access token to be requested",
        &mut event_rx,
        Some(&recorder),
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

    let mock_bin = get_mock_ffmpeg_bin();
    let hls_url = "http://127.0.0.1:0/dummy.m3u8".to_string();

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
    settings.channels = vec![ChannelConfig::with_alias("chan_no_chat", "NoChatStreamer")];

    let chzzk = Arc::new(
        ChzzkClient::new(&settings.chzzk).with_game_base_url(format!("http://127.0.0.1:{port}")),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None,
        event_tx,
        cancel_token.clone(),
    )
    .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let info = LiveStreamInfo {
        channel_id: "chan_no_chat".to_string(),
        live_id: Some(99992),
        streamer_name: "NoChatStreamer".to_string(),
        title: "No Chat Title".to_string(),
        hls_url,
        chat_channel_id: Some("chat_ch_999".to_string()),
        metadata: Default::default(),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_no_chat".to_string(), info, upload_tx);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    let mut recorder = TestLogRecorder::new();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv()).await {
        recorder.record(&ev);
        if let AppEvent::RecordingEnded { .. } = ev {
            break;
        }
    }

    assert_with_logs(
        !token_requested.load(Ordering::SeqCst),
        "Chat token must NOT be requested when record_chat is false",
        &mut event_rx,
        Some(&recorder),
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_preserves_local_file_when_backend_disabled() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_nobackend_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let (ws_url, _ws_handle) = spawn_mock_chat_ws_server().await;
    let mock_bin = get_mock_ffmpeg_bin();
    let hls_url = "http://127.0.0.1:0/dummy.m3u8".to_string();

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

    let chzzk = Arc::new(
        ChzzkClient::new(&settings.chzzk)
            .with_game_base_url(format!("http://127.0.0.1:{port}"))
            .with_chat_ws_url(ws_url),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        None, // No upload backend
        event_tx,
        cancel_token.clone(),
    )
    .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let info = LiveStreamInfo {
        channel_id: "chan_local_chat".to_string(),
        live_id: Some(99993),
        streamer_name: "LocalChatStreamer".to_string(),
        title: "Local Chat Title".to_string(),
        hls_url,
        chat_channel_id: Some("chat_ch_local".to_string()),
        metadata: Default::default(),
    };

    let (upload_tx, _upload_rx) = mpsc::channel::<UploadTask>(10);
    orchestrator.spawn_recording_session("chan_local_chat".to_string(), info, upload_tx);

    // Wait for the session dir to be created, then create a mock chat_0000.jsonl to simulate captured chat
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
                    let _ = fs::write(p.join("chat_0000.jsonl"), b"{\"content\":\"hello\"}\n");
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

    let session_dir = expect_with_logs(
        session_dir_opt,
        "Session directory must be created",
        &mut event_rx,
        None,
    );

    // Cancel session
    cancel_token.cancel();

    let mut recorder = TestLogRecorder::new();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv()).await {
        recorder.record(&ev);
        if let AppEvent::RecordingEnded { .. } = ev {
            break;
        }
    }

    assert_with_logs(
        session_dir.join("chat_0000.jsonl").exists(),
        "chat_0000.jsonl must remain saved locally when upload backend is disabled",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        session_dir.exists(),
        "Session directory containing chat_0000.jsonl must not be cleaned up as empty",
        &mut event_rx,
        Some(&recorder),
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_uploads_and_deletes_when_backend_enabled() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_backend_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chzzk_server = Server::http("127.0.0.1:0").unwrap();
    let chzzk_port = chzzk_server.server_addr().to_ip().unwrap().port();

    let (ws_url, _ws_handle) = spawn_mock_chat_ws_server().await;
    let mock_bin = get_mock_ffmpeg_bin();
    let hls_url = "http://127.0.0.1:0/dummy.m3u8".to_string();

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

    let mock_backend = Arc::new(MockUploadBackend::default());

    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;

    let chzzk = Arc::new(
        ChzzkClient::new(&settings.chzzk)
            .with_game_base_url(format!("http://127.0.0.1:{chzzk_port}"))
            .with_chat_ws_url(ws_url),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(mock_backend.clone()),
        event_tx.clone(),
        cancel_token.clone(),
    )
    .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let info = LiveStreamInfo {
        channel_id: "chan_backend_chat".to_string(),
        live_id: Some(99994),
        streamer_name: "BackendChatStreamer".to_string(),
        title: "Backend Chat Title".to_string(),
        hls_url,
        chat_channel_id: Some("chat_ch_backend".to_string()),
        metadata: Default::default(),
    };

    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);
    let consumer_handle = EngineOrchestrator::spawn_upload_consumer(
        Some(mock_backend.clone()),
        event_tx.clone(),
        upload_rx,
    );
    orchestrator.spawn_recording_session("chan_backend_chat".to_string(), info, upload_tx.clone());

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
                    let p = entry.path();
                    let _ = fs::write(
                        p.join("chat_0000.jsonl"),
                        b"{\"content\":\"stream chat message\"}\n",
                    );
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

    let session_dir = expect_with_logs(
        session_dir_opt,
        "Session directory must be created within timeout",
        &mut event_rx,
        None,
    );
    let chat_file_path = session_dir.join("chat_0000.jsonl");

    let session_folder = session_dir
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    upload_tx
        .send(UploadTask {
            channel_id: "chan_backend_chat".to_string(),
            session_folder_id: session_folder.clone(),
            remote_dir: session_folder,
            chunk_path: chat_file_path.clone(),
            chunk_name: "chat_0000.jsonl".to_string(),
            streamer_name: "BackendChatStreamer".to_string(),
        })
        .await
        .unwrap();

    cancel_token.cancel();

    let mut recorder = TestLogRecorder::new();
    let mut saw_uploaded_chunk = false;
    let mut saw_recording_ended = false;
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(ev) = event_rx.recv() => {
                recorder.record(&ev);
                if matches!(ev, AppEvent::UploadCompleted { ref chunk_name, .. } if chunk_name == "chat_0000.jsonl") {
                    saw_uploaded_chunk = true;
                }
                if matches!(ev, AppEvent::RecordingEnded { ref channel_id } if channel_id == "chan_backend_chat") {
                    saw_recording_ended = true;
                }
                if saw_uploaded_chunk && saw_recording_ended {
                    break;
                }
            }
            _ = &mut timeout => break,
        }
    }

    assert_with_logs(
        saw_uploaded_chunk,
        "Must observe AppEvent::UploadCompleted for chat chunk",
        &mut event_rx,
        Some(&recorder),
    );
    assert_with_logs(
        saw_recording_ended,
        "Must observe AppEvent::RecordingEnded within timeout",
        &mut event_rx,
        Some(&recorder),
    );

    let uploads = mock_backend.uploads.lock().await;
    assert!(
        uploads
            .iter()
            .any(|(path, _)| path.file_name().and_then(|n| n.to_str()) == Some("chat_0000.jsonl")),
        "chat_0000.jsonl must be uploaded via MockUploadBackend"
    );

    assert!(
        !chat_file_path.exists(),
        "chat_0000.jsonl must be deleted locally upon confirmed upload to maintain strictly bounded disk footprint"
    );

    consumer_handle.abort();
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_engine_orchestrator_chat_incremental_upload_and_delete() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_eng_chat_inc_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{addr}");

    let ws_handle = tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        use futures_util::{SinkExt, StreamExt};
        if let Some(Ok(_)) = ws.next().await {
            let resp = serde_json::json!({
                "cmd": 10100,
                "bdy": { "sid": "mock_session" }
            });
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    resp.to_string().into(),
                ))
                .await;

            let chat_pkt = serde_json::json!({
                "cmd": 93101,
                "bdy": [{
                    "msg": "Live stream chat chunk 0",
                    "msgTime": 1727268158000u64,
                    "msgTypeCode": 1,
                    "profile": "{\"nickname\":\"Viewer\"}",
                    "extras": "{}"
                }]
            });
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    chat_pkt.to_string().into(),
                ))
                .await;

            while let Some(Ok(_)) = ws.next().await {}
        }
    });

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            if request.url().contains("/v1/chats/access-token") {
                let mock_body = serde_json::json!({
                    "code": 200,
                    "message": null,
                    "content": { "accessToken": "token_inc_test" }
                });
                let response = Response::from_string(mock_body.to_string()).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                );
                let _ = request.respond(response);
            } else {
                let _ = request.respond(Response::empty(404));
            }
        }
    });

    let mock_bin = get_mock_ffmpeg_bin();
    let hls_url = "http://127.0.0.1:0/dummy.m3u8".to_string();

    let mock_backend = Arc::new(MockUploadBackend::new());
    let mut settings = Settings::default();
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;
    settings.general.chunk_duration_seconds = 1;
    settings.general.chat_flush_interval_seconds = 1;
    settings.channels = vec![ChannelConfig::with_alias("chan_chat_inc", "IncStreamer")];

    let chzzk = Arc::new(
        ChzzkClient::new(&settings.chzzk)
            .with_game_base_url(format!("http://127.0.0.1:{port}"))
            .with_chat_ws_url(ws_url),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = EngineOrchestrator::with_cancel_token(
        settings,
        chzzk,
        Some(mock_backend.clone()),
        event_tx.clone(),
        cancel_token.clone(),
    )
    .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);
    let consumer_handle = EngineOrchestrator::spawn_upload_consumer(
        Some(mock_backend.clone()),
        event_tx.clone(),
        upload_rx,
    );

    let info = LiveStreamInfo {
        channel_id: "chan_chat_inc".to_string(),
        live_id: Some(99995),
        streamer_name: "IncStreamer".to_string(),
        title: "Inc Stream Title".to_string(),
        hls_url,
        chat_channel_id: Some("chat_ch_inc".to_string()),
        metadata: Default::default(),
    };

    orchestrator.spawn_recording_session("chan_chat_inc".to_string(), info, upload_tx);

    let mut recorder = TestLogRecorder::new();
    let mut saw_chunk_uploaded = false;
    let mut saw_recording_ended = false;
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(ev) = event_rx.recv() => {
                recorder.record(&ev);
                if matches!(ev, AppEvent::UploadCompleted { ref chunk_name, .. } if chunk_name.starts_with("chat_") && chunk_name.ends_with(".jsonl")) {
                    saw_chunk_uploaded = true;
                    cancel_token.cancel();
                }
                if matches!(ev, AppEvent::RecordingEnded { ref channel_id } if channel_id == "chan_chat_inc") {
                    saw_recording_ended = true;
                }
                if saw_chunk_uploaded && saw_recording_ended {
                    break;
                }
            }
            _ = &mut timeout => {
                cancel_token.cancel();
                break;
            }
        }
    }

    let uploads = mock_backend.uploads.lock().await;
    assert_with_logs(
        uploads.iter().any(|(path, _)| {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            name.starts_with("chat_") && name.ends_with(".jsonl")
        }),
        "Expected at least one chat_*.jsonl uploaded to MockUploadBackend",
        &mut event_rx,
        Some(&recorder),
    );

    assert_with_logs(
        saw_chunk_uploaded,
        "Must observe AppEvent::UploadCompleted for chat chunk",
        &mut event_rx,
        Some(&recorder),
    );

    consumer_handle.abort();
    let _ = ws_handle.await;
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_recording_session_resolves_chat_token_and_ws_url_from_source() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_chat_token_{}", rand::random::<u32>()));
    let _ = fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: true,
            chunk_duration_seconds: 60,
            chat_flush_interval_seconds: 5,
            stream_cooldown_seconds: 1,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_chat".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let mock = Arc::new(
        MockLiveStreamSource::new()
            .with_chat_token("chat_chan_chat", "mock_secret_token_xyz")
            .with_chat_ws_url("wss://custom-ws.example.com"),
    );

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);
    let registry = ChannelLifecycleRegistry::new();
    let cancel_token = CancellationToken::new();

    let info = LiveStreamInfo {
        channel_id: "chan_chat".to_string(),
        live_id: Some(77777),
        streamer_name: "ChatStreamer".to_string(),
        title: "Chat Test Stream".to_string(),
        hls_url: "https://mock.stream/live.m3u8".to_string(),
        chat_channel_id: Some("chat_chan_chat".to_string()),
        metadata: Default::default(),
    };

    let session_handle = RecordingSession::spawn(
        "chan_chat".to_string(),
        info,
        upload_tx,
        settings,
        None,
        mock.clone(),
        event_tx,
        registry,
        cancel_token.clone(),
        Some(get_mock_ffmpeg_bin().to_string_lossy().to_string()),
    );

    // Wait for chat token resolution log event
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_token_log = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(AppEvent::Log(entry))) =
            tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await
        {
            if entry
                .message
                .contains("Retrieved chat access token for channel chan_chat")
            {
                saw_token_log = true;
                break;
            }
        }
    }

    cancel_token.cancel();
    let _ = session_handle.await;

    assert!(saw_token_log, "Must log chat token retrieval");
    assert_eq!(mock.chat_token_call_count("chat_chan_chat"), 1);
    assert_eq!(
        mock.chat_ws_url_call_count(),
        1,
        "Recording session must query chat_ws_url from the intake source"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
