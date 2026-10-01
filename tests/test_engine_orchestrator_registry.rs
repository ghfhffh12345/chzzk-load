mod common;

use std::sync::Arc;
use tokio::sync::mpsc;

use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::{ChannelConfig, ChzzkConfig, GeneralConfig, Settings};
use chzzk_load::engine::{ChannelLifecycleState, EngineOrchestrator};
use chzzk_load::tui::event::AppEvent;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;

#[tokio::test]
async fn test_orchestrator_typed_query_seam_idle() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan1".to_string(),
            alias: Some("StreamerOne".to_string()),
        }],
        ..Default::default()
    };
    let chzzk = ChzzkClient::new(&ChzzkConfig::default());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Initial state must be Idle and observable through typed methods
    assert!(!orchestrator.is_recording("chan1"));
    assert!(!orchestrator.is_restricted("chan1"));
    assert!(orchestrator.active_session("chan1").is_none());
    assert!(orchestrator.active_recording_ids().is_empty());
    assert!(matches!(
        orchestrator.channel_state("chan1"),
        ChannelLifecycleState::Idle
    ));
    assert!(matches!(
        orchestrator.registry().channel_state("chan1"),
        ChannelLifecycleState::Idle
    ));

    // Register finished session transitions idle/recording channel to cooldown
    orchestrator
        .register_finished_session("chan1", Some(12345))
        .await;
    assert!(matches!(
        orchestrator.channel_state("chan1"),
        ChannelLifecycleState::Cooldown {
            live_id: Some(12345),
            ..
        }
    ));

    // Register active session transitions channel to recording
    let session = chzzk_load::engine::ActiveSessionState::new(
        "2026-10-01_1200".to_string(),
        "StreamerOne".to_string(),
        Some("StreamerOne".to_string()),
        Default::default(),
    );
    orchestrator.register_active_session("chan1", session);
    assert!(orchestrator.is_recording("chan1"));
    assert_eq!(orchestrator.active_recording_ids(), vec!["chan1"]);
}

#[tokio::test]
async fn test_orchestrator_poll_delegates_to_evaluate_poll_and_starts_recording() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 12345,
                    "liveTitle": "Rust Stream",
                    "channel": {
                        "channelId": "chan_live",
                        "channelName": "LiveStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_reg_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_live".to_string(),
            alias: Some("StreamerAlias".to_string()),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    orchestrator.poll_channels_once(&upload_tx).await;

    // Registry must now track channel as Recording
    assert!(orchestrator.is_recording("chan_live"));
    assert_eq!(orchestrator.active_recording_ids(), vec!["chan_live"]);
    assert!(matches!(
        orchestrator.channel_state("chan_live"),
        ChannelLifecycleState::Recording { .. }
    ));
    assert!(orchestrator.active_session("chan_live").is_some());
    let session = orchestrator.active_session("chan_live").unwrap();
    assert_eq!(session.streamer_name, "LiveStreamer");
    assert_eq!(session.alias, Some("StreamerAlias".to_string()));
    assert_eq!(session.current_metadata.live_title, "Rust Stream");

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_poll_metadata_change_updates_registry_and_persists() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // Poll 1: Initial Title
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 55555,
                    "liveTitle": "Initial Title",
                    "channel": {
                        "channelId": "chan_meta",
                        "channelName": "MetaStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }

        // Poll 2: Updated Title
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 55555,
                    "liveTitle": "Updated Title",
                    "channel": {
                        "channelId": "chan_meta",
                        "channelName": "MetaStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://mock/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"1080p\",\"path\":\"https://mock/1080p.m3u8\"}]}]}"
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let temp_dir = std::env::temp_dir().join(format!("test_orch_meta_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_meta".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Starts recording
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_meta"));
    assert_eq!(
        orchestrator
            .active_session("chan_meta")
            .unwrap()
            .current_metadata
            .live_title,
        "Initial Title"
    );

    // Poll 2: Metadata change
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_meta"));
    let updated_session = orchestrator.active_session("chan_meta").unwrap();
    assert_eq!(updated_session.current_metadata.live_title, "Updated Title");

    // Verify metadata event was emitted or logged
    let mut got_channel_update = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::ChannelUpdate { title, is_live, .. } = event {
            if is_live && title == "Updated Title" {
                got_channel_update = true;
            }
        }
    }
    assert!(
        got_channel_update,
        "Must emit ChannelUpdate with updated title"
    );

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_handles_api_restricted_stream_via_registry() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let c = count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = if c == 0 {
                // Poll 1: Restricted (19+ adult)
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "status": "OPEN",
                        "liveId": 99999,
                        "liveTitle": "Adult Only Stream",
                        "channel": {
                            "channelId": "chan_adult",
                            "channelName": "AdultStreamer"
                        },
                        "livePlaybackJson": null,
                        "adult": true
                    }
                }"#
            } else {
                // Poll 2: Offline (CLOSE)
                r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "status": "CLOSE",
                        "liveId": 99999,
                        "liveTitle": null,
                        "channel": {
                            "channelId": "chan_adult",
                            "channelName": "AdultStreamer"
                        },
                        "livePlaybackJson": null,
                        "adult": false
                    }
                }"#
            };
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_adult".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx);

    // Poll 1: Restricted
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_adult"));
    assert!(!orchestrator.is_recording("chan_adult"));
    assert!(matches!(
        orchestrator.channel_state("chan_adult"),
        ChannelLifecycleState::Restricted {
            reason: chzzk_load::engine::RestrictionReason::AgeRestricted,
            ..
        }
    ));

    // Verify error log was emitted
    let mut got_error_log = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = event {
            if entry.message.contains("19+ age-restricted") {
                got_error_log = true;
            }
        }
    }
    assert!(got_error_log, "Must log 19+ age restriction error");

    // Poll 2: Offline resets to Idle
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_adult"));
    assert!(!orchestrator.is_recording("chan_adult"));
    assert!(matches!(
        orchestrator.channel_state("chan_adult"),
        ChannelLifecycleState::Idle
    ));
}

#[tokio::test]
async fn test_orchestrator_handles_ffmpeg_403_forbidden_via_registry() {
    let temp_dir = std::env::temp_dir().join(format!("test_orch_403_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let mock_bin = get_mock_ffmpeg_bin();

    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = poll_count.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let count = poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mock_body = if count == 0 {
                // Poll 2: OPEN broadcast (API cache still OPEN with same liveId)
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
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_sports".to_string(),
            alias: Some("SportsStreamer".to_string()),
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx)
        .with_ffmpeg_bin(mock_bin.to_string_lossy());

    let info = chzzk_load::chzzk::models::LiveStreamInfo {
        channel_id: "chan_sports".to_string(),
        live_id: Some(21326414),
        streamer_name: "SportsStreamer".to_string(),
        title: "Sports Broadcast (Encrypted)".to_string(),
        hls_url: "https://test.com/hls_key_error.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    };

    // Spawn session directly
    orchestrator.spawn_recording_session("chan_sports".to_string(), info, upload_tx.clone());

    // Await RecordingEnded event from 403 abort
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut got_ended = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_millis(300), event_rx.recv()).await {
            Ok(Some(AppEvent::RecordingEnded { channel_id })) if channel_id == "chan_sports" => {
                got_ended = true;
                break;
            }
            _ => {}
        }
    }
    assert!(got_ended, "Must emit RecordingEnded upon 403 abort");

    // Observable typed state checks: channel transitioned to Restricted cleanly!
    assert!(orchestrator.is_restricted("chan_sports"));
    assert!(!orchestrator.is_recording("chan_sports"));
    assert!(matches!(
        orchestrator.channel_state("chan_sports"),
        ChannelLifecycleState::Restricted {
            live_id: Some(21326414),
            reason: chzzk_load::engine::RestrictionReason::KeyForbidden,
        }
    ));

    // Poll 2: Next poll for same liveId should remain Restricted and NOT spawn new recording
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_sports"));
    assert!(!orchestrator.is_recording("chan_sports"));

    // Poll 3: CLOSE transitions to Idle
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_sports"));
    assert!(matches!(
        orchestrator.channel_state("chan_sports"),
        ChannelLifecycleState::Idle
    ));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_graceful_shutdown_drains_registry_sessions() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_shutdown_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 88888,
                    "status": "OPEN",
                    "liveTitle": "Shutdown Test Stream",
                    "channel": { "channelId": "chan_shutdown", "channelName": "ShutdownStreamer" },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                    "adult": false
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            poll_interval_seconds: 1,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_shutdown".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let chzzk = ChzzkClient::new(&settings.chzzk).with_base_url(format!("http://127.0.0.1:{port}"));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);

    let orchestrator = Arc::new(EngineOrchestrator::new(settings, chzzk, None, event_tx));
    let orch_clone = orchestrator.clone();

    let run_handle = tokio::spawn(async move {
        orch_clone.run().await;
    });

    // Wait until recording starts
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if orchestrator.is_recording("chan_shutdown") {
            break;
        }
        let _ = tokio::time::timeout(std::time::Duration::from_millis(50), event_rx.recv()).await;
    }
    assert!(orchestrator.is_recording("chan_shutdown"));

    // Cancel engine orchestrator (triggers cancel_all on registry)
    orchestrator.cancel();

    // Await run_handle with timeout to ensure clean shutdown draining
    let shutdown_res = tokio::time::timeout(std::time::Duration::from_secs(5), run_handle).await;
    assert!(
        shutdown_res.is_ok(),
        "Engine run loop must terminate cleanly within timeout"
    );
    assert!(
        orchestrator.active_recording_ids().is_empty(),
        "All active recordings must be drained from the registry upon shutdown"
    );
    assert!(
        !orchestrator.is_recording("chan_shutdown"),
        "Channel must not be in recording state after shutdown"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}
