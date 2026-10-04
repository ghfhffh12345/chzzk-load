mod common;

use std::sync::Arc;
use tokio::sync::mpsc;

use chzzk_load::chzzk::models::{LiveDetail, LiveStreamInfo};
use chzzk_load::chzzk::source::MockLiveStreamSource;
use chzzk_load::config::{ChannelConfig, GeneralConfig, Settings};
use chzzk_load::engine::{ChannelLifecycleState, EngineOrchestrator};
use chzzk_load::tui::event::AppEvent;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::mock_source::{make_close_detail, make_open_detail, make_restricted_detail};

#[tokio::test]
async fn test_orchestrator_typed_query_seam_idle() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan1".to_string(),
            alias: Some("StreamerOne".to_string()),
        }],
        ..Default::default()
    };
    let chzzk = Arc::new(MockLiveStreamSource::new());
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
        chzzk_load::chzzk::models_metadata::StreamMetadataState::default(),
    );
    orchestrator.register_active_session("chan1", session);
    assert!(orchestrator.is_recording("chan1"));
    assert_eq!(orchestrator.active_recording_ids(), vec!["chan1"]);
}

#[tokio::test]
async fn test_orchestrator_poll_delegates_to_evaluate_poll_and_starts_recording() {
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

    let live_detail = make_open_detail(
        "chan_live",
        "LiveStreamer",
        "Rust Stream",
        12345,
        "https://mock/master.m3u8",
    );

    let chzzk = Arc::new(MockLiveStreamSource::new().with_channel_state("chan_live", live_detail));
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx)
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy());

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

    let source = MockLiveStreamSource::new();
    let make_info = |title: &str, tags: Vec<String>| {
        LiveDetail::Open(LiveStreamInfo {
            channel_id: "chan_meta".to_string(),
            live_id: Some(55555),
            streamer_name: "MetaStreamer".to_string(),
            title: title.to_string(),
            hls_url: "https://mock/master.m3u8".to_string(),
            chat_channel_id: None,
            metadata: chzzk_load::chzzk::models_metadata::StreamMetadataState {
                live_title: title.to_string(),
                live_id: Some(55555),
                tags,
                ..Default::default()
            },
        })
    };
    source.enqueue_channel_states(
        "chan_meta",
        vec![
            make_info("Initial Title", vec![]),
            make_info("Updated Title", vec![]),
            make_info("Updated Title", vec!["gaming".to_string()]),
            make_info("Updated Title", vec!["gaming".to_string()]),
        ],
    );

    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, chzzk, None, event_tx)
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy());

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
    while event_rx.try_recv().is_ok() {}

    // Poll 2: Metadata change (title changed)
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_meta"));
    let updated_session = orchestrator.active_session("chan_meta").unwrap();
    assert_eq!(updated_session.current_metadata.live_title, "Updated Title");

    // Verify title change triggered ChannelUpdate
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
        "Must emit ChannelUpdate when title changed"
    );

    // Poll 3: Metadata change with UNCHANGED title (only tags changed)
    orchestrator.poll_channels_once(&upload_tx).await;
    let tag_updated_session = orchestrator.active_session("chan_meta").unwrap();
    assert_eq!(
        tag_updated_session.current_metadata.tags,
        vec!["gaming".to_string()]
    );
    assert_eq!(tag_updated_session.metadata_history.len(), 3);

    // Verify NO ChannelUpdate was emitted since title did not change
    let mut saw_channel_update_on_tag_change = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::ChannelUpdate { .. } = event {
            saw_channel_update_on_tag_change = true;
        }
    }
    assert!(
        !saw_channel_update_on_tag_change,
        "Must NOT emit ChannelUpdate when title did not change"
    );

    // Poll 4: Identical metadata poll
    orchestrator.poll_channels_once(&upload_tx).await;
    let identical_session = orchestrator.active_session("chan_meta").unwrap();
    assert_eq!(
        identical_session.metadata_history.len(),
        3,
        "Identical metadata must not append to history"
    );

    // Clean up
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_handles_api_restricted_stream_via_registry() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_adult".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_adult",
        vec![
            make_restricted_detail(
                "chan_adult",
                "AdultStreamer",
                "Adult Only Stream",
                Some(99999),
                true,
            ),
            make_close_detail(Some("AdultStreamer")),
        ],
    );

    let chzzk = Arc::new(source);
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

    let source = MockLiveStreamSource::new();
    source.enqueue_channel_states(
        "chan_sports",
        vec![
            // Poll 2: OPEN broadcast (API cache still OPEN with same liveId)
            make_open_detail(
                "chan_sports",
                "SportsStreamer",
                "Sports Broadcast (Encrypted)",
                21326414,
                "https://test.com/hls.m3u8",
            ),
            // Poll 3: CLOSE (offline)
            make_close_detail(Some("SportsStreamer")),
        ],
    );

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

    let chzzk = Arc::new(source);
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

    let source = MockLiveStreamSource::new().with_channel_state(
        "chan_shutdown",
        make_open_detail(
            "chan_shutdown",
            "ShutdownStreamer",
            "Shutdown Test Stream",
            88888,
            "https://test.com/hls.m3u8",
        ),
    );

    let chzzk = Arc::new(source);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);

    let orchestrator = Arc::new(
        EngineOrchestrator::new(settings, chzzk, None, event_tx)
            .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy()),
    );
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

#[tokio::test]
async fn test_orchestrator_cleanup_empty_session_dirs_bounded() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cleanup_bounded_{}", rand::random::<u32>()));
    let empty_session = temp_dir.join("empty_session_1");
    tokio::fs::create_dir_all(&empty_session).await.unwrap();

    let non_empty = temp_dir.join("active_session_2");
    tokio::fs::create_dir_all(&non_empty).await.unwrap();
    tokio::fs::write(non_empty.join("chunk_0000.ts"), b"data")
        .await
        .unwrap();

    let cleaned = EngineOrchestrator::cleanup_empty_session_dirs_bounded(
        &temp_dir,
        std::time::Duration::from_millis(500),
    )
    .await
    .expect("cleanup bounded succeeded");

    assert_eq!(cleaned, 1);
    assert!(!empty_session.exists());
    assert!(non_empty.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_abort_all_terminates_registered_sessions_and_cancels_tokens() {
    let settings = Settings::default();
    let chzzk = Arc::new(MockLiveStreamSource::new());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(10);
    let orchestrator = Arc::new(EngineOrchestrator::new(settings, chzzk, None, event_tx));

    let dummy_handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
    let dummy_upload_handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    orchestrator.push_session_handle_for_test(dummy_handle);
    orchestrator.register_upload_handle_for_test(dummy_upload_handle);

    assert!(!orchestrator.cancel_token().is_cancelled());
    let aborted = orchestrator.abort_all();
    assert_eq!(aborted, 2);
    assert!(orchestrator.cancel_token().is_cancelled());
}

#[tokio::test]
async fn test_orchestrator_grace_period_timeout_escalation() {
    tokio::time::pause();
    let temp_dir = std::env::temp_dir().join(format!("test_escalation_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let chzzk = Arc::new(MockLiveStreamSource::new());
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(10);
    let orchestrator = Arc::new(EngineOrchestrator::new(settings, chzzk, None, event_tx));

    // Simulate lingering session task that does not terminate within short grace period
    let lingering_task = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
    orchestrator.push_session_handle_for_test(lingering_task);

    // Create an empty session folder that should be cleaned up on escalation
    let empty_dir = temp_dir.join("empty_session_timeout");
    tokio::fs::create_dir_all(&empty_dir).await.unwrap();

    // Verify timeout expiration condition using virtual time (0ms wall-clock)
    let mut run_handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });

    let timeout_res =
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut run_handle).await;
    assert!(
        timeout_res.is_err(),
        "Simulated grace period must expire on lingering tasks"
    );

    // Escalation actions:
    // 1. Abort all lingering orchestrator tasks
    let aborted = orchestrator.abort_all();
    run_handle.abort();
    assert_eq!(aborted, 1);
    assert!(orchestrator.cancel_token().is_cancelled());

    // 2. Perform bounded empty directory cleanup (<500ms)
    let cleaned = EngineOrchestrator::cleanup_empty_session_dirs_bounded(
        &temp_dir,
        std::time::Duration::from_millis(500),
    )
    .await
    .expect("bounded cleanup must succeed");
    assert_eq!(cleaned, 1);
    assert!(!empty_dir.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_polling_offline_with_fake_source() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_offline".to_string(),
            alias: Some("OfflineStreamer".to_string()),
        }],
        ..Default::default()
    };
    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_offline",
        LiveDetail::Close {
            streamer_name: Some("OfflineStreamer".to_string()),
        },
    ));
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx);

    orchestrator.poll_channels_once(&upload_tx).await;

    assert!(!orchestrator.is_recording("chan_offline"));
    assert!(!orchestrator.is_restricted("chan_offline"));
    assert!(matches!(
        orchestrator.channel_state("chan_offline"),
        ChannelLifecycleState::Idle
    ));
    assert_eq!(mock.call_count("chan_offline"), 1);
}

#[tokio::test]
async fn test_orchestrator_polling_live_starts_recording_with_fake_source() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_fake_live_{}", rand::random::<u32>()));
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

    let live_detail = LiveDetail::Open(LiveStreamInfo {
        channel_id: "chan_live".to_string(),
        live_id: Some(12345),
        streamer_name: "LiveStreamer".to_string(),
        title: "Rust Intake Stream".to_string(),
        hls_url: "https://mock.stream/live.m3u8".to_string(),
        chat_channel_id: Some("chat_live".to_string()),
        metadata: chzzk_load::chzzk::models_metadata::StreamMetadataState {
            live_title: "Rust Intake Stream".to_string(),
            live_id: Some(12345),
            ..Default::default()
        },
    });

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state("chan_live", live_detail));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx)
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy());

    orchestrator.poll_channels_once(&upload_tx).await;

    assert!(orchestrator.is_recording("chan_live"));
    assert_eq!(orchestrator.active_recording_ids(), vec!["chan_live"]);
    assert!(matches!(
        orchestrator.channel_state("chan_live"),
        ChannelLifecycleState::Recording { .. }
    ));
    let session = orchestrator
        .active_session("chan_live")
        .expect("active session must exist");
    assert_eq!(session.streamer_name, "LiveStreamer");
    assert_eq!(session.alias, Some("StreamerAlias".to_string()));
    assert_eq!(session.current_metadata.live_title, "Rust Intake Stream");
    assert_eq!(mock.call_count("chan_live"), 1);

    // Verify ChannelUpdate event emitted
    let mut saw_channel_update = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::ChannelUpdate {
            channel_id,
            is_live,
            title,
            ..
        } = event
        {
            if channel_id == "chan_live" && is_live && title == "Rust Intake Stream" {
                saw_channel_update = true;
            }
        }
    }
    assert!(saw_channel_update);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_polling_restricted_stream_with_fake_source() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_restricted".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let restricted_detail = LiveDetail::Restricted {
        channel_id: "chan_restricted".to_string(),
        live_id: Some(99999),
        streamer_name: "AdultStreamer".to_string(),
        title: "19+ Broadcast".to_string(),
        chat_channel_id: None,
        adult: true,
    };

    let mock = Arc::new(
        MockLiveStreamSource::new().with_channel_state("chan_restricted", restricted_detail),
    );
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx);

    // Poll 1: Restricted
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_restricted"));
    assert!(!orchestrator.is_recording("chan_restricted"));
    assert!(matches!(
        orchestrator.channel_state("chan_restricted"),
        ChannelLifecycleState::Restricted {
            reason: chzzk_load::engine::RestrictionReason::AgeRestricted,
            ..
        }
    ));
    assert_eq!(mock.call_count("chan_restricted"), 1);

    // Verify warning log emitted
    let mut saw_log = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = event {
            if entry.message.contains("19+ age-restricted") {
                saw_log = true;
            }
        }
    }
    assert!(saw_log);

    // Poll 2: Stream goes offline (Close) -> transitions back to Idle
    mock.set_channel_state(
        "chan_restricted",
        LiveDetail::Close {
            streamer_name: Some("AdultStreamer".to_string()),
        },
    );
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_restricted"));
    assert!(!orchestrator.is_recording("chan_restricted"));
    assert!(matches!(
        orchestrator.channel_state("chan_restricted"),
        ChannelLifecycleState::Idle
    ));
    assert_eq!(mock.call_count("chan_restricted"), 2);
}

#[tokio::test]
async fn test_orchestrator_polling_error_handling_with_fake_source() {
    let settings = Settings {
        channels: vec![ChannelConfig {
            id: "chan_err".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.inject_channel_error("chan_err", "500 Internal Server Error: Gateway Timeout");

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx);

    // Poll 1: Errored query must not crash orchestrator and must retain Idle state
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_recording("chan_err"));
    assert!(!orchestrator.is_restricted("chan_err"));
    assert!(matches!(
        orchestrator.channel_state("chan_err"),
        ChannelLifecycleState::Idle
    ));
    assert_eq!(mock.call_count("chan_err"), 1);

    // Verify warning log for polling failure
    let mut saw_warn = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = event {
            if entry.message.contains("Polling failed for chan_err") {
                saw_warn = true;
            }
        }
    }
    assert!(saw_warn, "Must log warning when polling fails");

    // Clear error and verify recovery
    mock.clear_channel_error("chan_err");
    mock.set_channel_state(
        "chan_err",
        LiveDetail::Close {
            streamer_name: Some("RecoveredStreamer".to_string()),
        },
    );
    orchestrator.poll_channels_once(&upload_tx).await;
    assert_eq!(mock.call_count("chan_err"), 2);
}

#[tokio::test]
async fn test_orchestrator_polling_sequential_transitions_with_fake_source() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_fake_seq_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_seq".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let mock = Arc::new(MockLiveStreamSource::new());
    mock.enqueue_channel_states(
        "chan_seq",
        vec![
            LiveDetail::Close {
                streamer_name: Some("SeqStreamer".to_string()),
            },
            LiveDetail::Open(LiveStreamInfo {
                channel_id: "chan_seq".to_string(),
                live_id: Some(11111),
                streamer_name: "SeqStreamer".to_string(),
                title: "Sequential Stream 1".to_string(),
                hls_url: "https://mock.stream/live.m3u8".to_string(),
                chat_channel_id: None,
                metadata: chzzk_load::chzzk::models_metadata::StreamMetadataState {
                    live_title: "Sequential Stream 1".to_string(),
                    live_id: Some(11111),
                    ..Default::default()
                },
            }),
            LiveDetail::Open(LiveStreamInfo {
                channel_id: "chan_seq".to_string(),
                live_id: Some(11111),
                streamer_name: "SeqStreamer".to_string(),
                title: "Sequential Stream 1 Updated".to_string(),
                hls_url: "https://mock.stream/live.m3u8".to_string(),
                chat_channel_id: None,
                metadata: chzzk_load::chzzk::models_metadata::StreamMetadataState {
                    live_title: "Sequential Stream 1 Updated".to_string(),
                    live_id: Some(11111),
                    ..Default::default()
                },
            }),
            LiveDetail::Restricted {
                channel_id: "chan_seq".to_string(),
                live_id: Some(11111),
                streamer_name: "SeqStreamer".to_string(),
                title: "Restricted Stream".to_string(),
                chat_channel_id: None,
                adult: true,
            },
            LiveDetail::Close {
                streamer_name: Some("SeqStreamer".to_string()),
            },
        ],
    );

    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx)
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy());

    // Poll 1: Close -> Idle
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_recording("chan_seq"));
    assert_eq!(mock.call_count("chan_seq"), 1);

    // Poll 2: Open -> Recording
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_seq"));
    assert_eq!(
        orchestrator
            .active_session("chan_seq")
            .unwrap()
            .current_metadata
            .live_title,
        "Sequential Stream 1"
    );
    assert_eq!(mock.call_count("chan_seq"), 2);

    // Poll 3: Metadata update -> Still Recording, title updated
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_seq"));
    assert_eq!(
        orchestrator
            .active_session("chan_seq")
            .unwrap()
            .current_metadata
            .live_title,
        "Sequential Stream 1 Updated"
    );
    assert_eq!(mock.call_count("chan_seq"), 3);

    // Poll 4: Restricted -> Transitions to Restricted
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_restricted("chan_seq"));
    assert!(!orchestrator.is_recording("chan_seq"));
    assert_eq!(mock.call_count("chan_seq"), 4);

    // Poll 5: Close -> Transitions to Idle
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_restricted("chan_seq"));
    assert!(!orchestrator.is_recording("chan_seq"));
    assert!(matches!(
        orchestrator.channel_state("chan_seq"),
        ChannelLifecycleState::Idle
    ));
    assert_eq!(mock.call_count("chan_seq"), 5);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_polling_cooldown_evaluation_with_fake_source() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_cooldown_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            poll_interval_seconds: 1,
            stream_cooldown_seconds: 60,
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_cd".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let fake_stream = LiveDetail::Open(LiveStreamInfo {
        channel_id: "chan_cd".to_string(),
        live_id: Some(44444),
        streamer_name: "CooldownStreamer".to_string(),
        title: "Cooldown Stream".to_string(),
        hls_url: "https://mock.stream/live.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    });

    let mock = Arc::new(MockLiveStreamSource::new().with_channel_state("chan_cd", fake_stream));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx)
        .with_ffmpeg_bin(get_mock_ffmpeg_bin().to_string_lossy());

    // Mark channel as finished / in cooldown for live_id 44444
    orchestrator
        .register_finished_session("chan_cd", Some(44444))
        .await;
    assert!(matches!(
        orchestrator.channel_state("chan_cd"),
        ChannelLifecycleState::Cooldown {
            live_id: Some(44444),
            ..
        }
    ));

    // Poll 1: While within cooldown window and live_id is unchanged, poll evaluation remains in Cooldown
    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(!orchestrator.is_recording("chan_cd"));
    assert!(matches!(
        orchestrator.channel_state("chan_cd"),
        ChannelLifecycleState::Cooldown { .. }
    ));
    assert_eq!(mock.call_count("chan_cd"), 1);

    // Verify InCooldown AppEvent::Log was emitted
    let mut saw_cooldown_log = false;
    while let Ok(event) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = event {
            if entry.message.contains("Waiting for API cache to close") {
                saw_cooldown_log = true;
                break;
            }
        }
    }
    assert!(saw_cooldown_log, "Must log cooldown waiting message");

    // Enqueue a new stream with a DIFFERENT live_id -> Cooldown should be bypassed and start recording immediately
    let new_stream = LiveDetail::Open(LiveStreamInfo {
        channel_id: "chan_cd".to_string(),
        live_id: Some(55555),
        streamer_name: "CooldownStreamer".to_string(),
        title: "New Stream".to_string(),
        hls_url: "https://mock.stream/live.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    });
    mock.set_channel_state("chan_cd", new_stream);

    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_cd"));
    assert_eq!(mock.call_count("chan_cd"), 2);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_orchestrator_inherits_mock_ffmpeg_bin_from_environment_without_explicit_builder_call()
{
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_orch_hermetic_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let settings = Settings {
        general: GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            ..Default::default()
        },
        channels: vec![ChannelConfig {
            id: "chan_hermetic".to_string(),
            alias: None,
        }],
        ..Default::default()
    };

    let live_detail = LiveDetail::Open(LiveStreamInfo {
        channel_id: "chan_hermetic".to_string(),
        live_id: Some(99901),
        streamer_name: "HermeticStreamer".to_string(),
        title: "Hermetic Stream".to_string(),
        hls_url: "https://mock.stream/live.m3u8".to_string(),
        chat_channel_id: None,
        metadata: Default::default(),
    });

    let mock =
        Arc::new(MockLiveStreamSource::new().with_channel_state("chan_hermetic", live_detail));
    let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(100);
    let (upload_tx, _upload_rx) = mpsc::channel(100);

    // Explicitly omit .with_ffmpeg_bin(...) to verify environment fallback
    let orchestrator = EngineOrchestrator::new(settings, mock.clone(), None, event_tx);
    assert_eq!(
        orchestrator.ffmpeg_bin(),
        Some(mock_bin.to_string_lossy().as_ref()),
        "Orchestrator must automatically inherit CHZZK_LOAD_FFMPEG_BIN from environment"
    );

    orchestrator.poll_channels_once(&upload_tx).await;
    assert!(orchestrator.is_recording("chan_hermetic"));

    orchestrator.cancel();
    let _ = std::fs::remove_dir_all(&temp_dir);
}
