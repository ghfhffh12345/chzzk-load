use chzzk_load::chzzk::models_metadata::StreamMetadataState;
use chzzk_load::engine::registry::{
    ChannelLifecycleKind, ChannelLifecycleRegistry, ChannelLifecycleState, RestrictionReason,
};
use chzzk_load::engine::session::ActiveSessionState;
use tokio_util::sync::CancellationToken;

fn make_test_session(streamer: &str, title: &str) -> ActiveSessionState {
    ActiveSessionState::new(
        "2026-10-01_150000".to_string(),
        streamer.to_string(),
        None,
        StreamMetadataState {
            live_title: title.to_string(),
            ..Default::default()
        },
    )
}

#[test]
fn test_initial_channel_state_is_idle() {
    let registry = ChannelLifecycleRegistry::new();
    assert_eq!(
        registry.channel_state("test_channel").kind(),
        ChannelLifecycleKind::Idle
    );
    assert!(!registry.is_recording("test_channel"));
    assert!(!registry.is_restricted("test_channel"));
    assert!(registry.active_session("test_channel").is_none());
    assert!(registry.active_recording_ids().is_empty());
}

#[test]
fn test_transition_to_recording() {
    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("StreamerA", "Live Gaming");
    let token = CancellationToken::new();

    registry.start_recording("test_channel", session, token.clone());

    assert_eq!(
        registry.channel_state("test_channel").kind(),
        ChannelLifecycleKind::Recording
    );
    assert!(registry.is_recording("test_channel"));
    assert!(!registry.is_restricted("test_channel"));
    assert_eq!(
        registry
            .active_session("test_channel")
            .unwrap()
            .initial_title,
        "Live Gaming"
    );
    assert_eq!(
        registry.active_recording_ids(),
        vec!["test_channel".to_string()]
    );
}

#[test]
fn test_transition_recording_to_cooldown_triggers_cancellation() {
    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("StreamerA", "Live Stream");
    let token = CancellationToken::new();

    registry.start_recording("ch1", session, token.clone());
    assert!(!token.is_cancelled());

    registry.finish_recording("ch1", Some(12345));

    assert!(
        token.is_cancelled(),
        "Token must be cancelled upon transitioning to Cooldown"
    );
    assert!(!registry.is_recording("ch1"));
    let state = registry.channel_state("ch1");
    assert_eq!(state.kind(), ChannelLifecycleKind::Cooldown);
    if let ChannelLifecycleState::Cooldown { live_id, .. } = state {
        assert_eq!(live_id, Some(12345));
    } else {
        panic!("Expected Cooldown state");
    }
}

#[test]
fn test_transition_recording_to_restricted_triggers_cancellation() {
    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("StreamerA", "Live Stream");
    let token = CancellationToken::new();

    registry.start_recording("ch1", session, token.clone());
    assert!(!token.is_cancelled());

    registry.mark_restricted("ch1", Some(54321), RestrictionReason::KeyForbidden);

    assert!(
        token.is_cancelled(),
        "Token must be cancelled upon transitioning to Restricted"
    );
    assert!(!registry.is_recording("ch1"));
    assert!(registry.is_restricted("ch1"));
    let state = registry.channel_state("ch1");
    assert_eq!(state.kind(), ChannelLifecycleKind::Restricted);
    if let ChannelLifecycleState::Restricted { live_id, reason } = state {
        assert_eq!(live_id, Some(54321));
        assert_eq!(reason, RestrictionReason::KeyForbidden);
    } else {
        panic!("Expected Restricted state");
    }
}

#[test]
fn test_transition_recording_to_idle_triggers_cancellation() {
    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("StreamerA", "Live Stream");
    let token = CancellationToken::new();

    registry.start_recording("ch1", session, token.clone());
    assert!(!token.is_cancelled());

    registry.reset_to_idle("ch1");

    assert!(
        token.is_cancelled(),
        "Token must be cancelled upon transitioning to Idle"
    );
    assert_eq!(
        registry.channel_state("ch1").kind(),
        ChannelLifecycleKind::Idle
    );
    assert!(!registry.is_recording("ch1"));
}

#[test]
fn test_replacing_recording_cancels_previous_token() {
    let registry = ChannelLifecycleRegistry::new();
    let session1 = make_test_session("StreamerA", "First Session");
    let token1 = CancellationToken::new();

    let session2 = make_test_session("StreamerA", "Second Session");
    let token2 = CancellationToken::new();

    registry.start_recording("ch1", session1, token1.clone());
    assert!(!token1.is_cancelled());

    registry.start_recording("ch1", session2, token2.clone());

    assert!(token1.is_cancelled(), "Old session token must be cancelled");
    assert!(
        !token2.is_cancelled(),
        "New session token must remain active"
    );
    assert_eq!(
        registry.active_session("ch1").unwrap().initial_title,
        "Second Session"
    );
}

#[test]
fn test_cancel_channel_and_cancel_all() {
    let registry = ChannelLifecycleRegistry::new();
    let s1 = make_test_session("S1", "T1");
    let t1 = CancellationToken::new();
    let s2 = make_test_session("S2", "T2");
    let t2 = CancellationToken::new();

    registry.start_recording("ch1", s1, t1.clone());
    registry.start_recording("ch2", s2, t2.clone());

    assert!(registry.cancel_channel("ch1"));
    assert!(t1.is_cancelled());
    assert!(!t2.is_cancelled());

    registry.cancel_all();
    assert!(t2.is_cancelled());
}

#[test]
fn test_streamer_name_caching_and_display_name_resolution() {
    let registry = ChannelLifecycleRegistry::new();

    // 1. Initially uncached: resolution falls back to channel_id
    assert_eq!(registry.cached_streamer_name("ch_1"), None);
    assert_eq!(registry.resolve_display_name("ch_1", None), "ch_1");

    // 2. If alias is provided, alias takes highest precedence
    assert_eq!(
        registry.resolve_display_name("ch_1", Some("MyAlias")),
        "MyAlias"
    );

    // 3. Cache official streamer name
    registry.cache_streamer_name("ch_1", "Official Streamer Name");
    assert_eq!(
        registry.cached_streamer_name("ch_1"),
        Some("Official Streamer Name".to_string())
    );

    // 4. Without alias, resolution now returns official streamer name
    assert_eq!(
        registry.resolve_display_name("ch_1", None),
        "Official Streamer Name"
    );

    // 5. With alias, alias still wins over cached official name
    assert_eq!(
        registry.resolve_display_name("ch_1", Some("CustomAlias")),
        "CustomAlias"
    );
}

fn make_open_detail(
    channel_id: &str,
    live_id: Option<u64>,
    streamer: &str,
    title: &str,
) -> chzzk_load::chzzk::models::LiveDetail {
    chzzk_load::chzzk::models::LiveDetail::Open(chzzk_load::chzzk::models::LiveStreamInfo {
        channel_id: channel_id.to_string(),
        live_id,
        streamer_name: streamer.to_string(),
        title: title.to_string(),
        hls_url: "https://example.com/live.m3u8".to_string(),
        chat_channel_id: Some("chat_123".to_string()),
        metadata: StreamMetadataState {
            live_title: title.to_string(),
            ..Default::default()
        },
    })
}

#[test]
fn test_evaluate_poll_idle_channel_returns_ready_to_record() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    let detail = make_open_detail("ch1", Some(101), "Streamer1", "Gaming Live");

    let action = registry.evaluate_poll("ch1", Some("Alias1"), &detail, Duration::from_secs(30));

    match action {
        PollAction::ReadyToRecord {
            info,
            display_name,
            was_api_restricted,
        } => {
            assert_eq!(display_name, "Alias1");
            assert_eq!(info.streamer_name, "Streamer1");
            assert_eq!(info.live_id, Some(101));
            assert!(!was_api_restricted);
        }
        other => panic!("Expected ReadyToRecord, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_recording_metadata_change_and_already_recording() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("Streamer1", "Initial Title");
    let token = CancellationToken::new();
    registry.start_recording("ch1", session, token);

    // 1. Same title -> AlreadyRecording
    let detail_same = make_open_detail("ch1", Some(101), "Streamer1", "Initial Title");
    let action = registry.evaluate_poll("ch1", None, &detail_same, Duration::from_secs(30));
    match action {
        PollAction::AlreadyRecording {
            display_name,
            title,
        } => {
            assert_eq!(display_name, "Streamer1");
            assert_eq!(title, "Initial Title");
        }
        other => panic!("Expected AlreadyRecording, got {other:?}"),
    }

    // 2. Changed title -> RecordingMetadataChanged
    let mut detail_new = make_open_detail("ch1", Some(101), "Streamer1", "Updated Title");
    if let chzzk_load::chzzk::models::LiveDetail::Open(ref mut info) = detail_new {
        info.metadata.live_title = "Updated Title".to_string();
    }
    let action2 = registry.evaluate_poll("ch1", None, &detail_new, Duration::from_secs(30));
    match action2 {
        PollAction::RecordingMetadataChanged {
            delta,
            display_name,
            title,
            ..
        } => {
            assert_eq!(display_name, "Streamer1");
            assert_eq!(title, "Updated Title");
            assert_eq!(delta.live_title.as_ref().unwrap().old, "Initial Title");
            assert_eq!(delta.live_title.as_ref().unwrap().new, "Updated Title");
        }
        other => panic!("Expected RecordingMetadataChanged, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_cooldown_unexpired_and_expired() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::{Duration, Instant};

    let registry = ChannelLifecycleRegistry::new();

    // 1. Active cooldown (recent finish)
    registry.mark_cooldown("ch1", Some(101), Instant::now());
    let detail = make_open_detail("ch1", Some(101), "Streamer1", "Live Again");
    let action = registry.evaluate_poll("ch1", None, &detail, Duration::from_secs(30));

    match action {
        PollAction::InCooldown { live_id, .. } => {
            assert_eq!(live_id, Some(101));
        }
        other => panic!("Expected InCooldown, got {other:?}"),
    }

    // 2. Expired cooldown -> resumes interrupted stream
    let past = Instant::now() - Duration::from_secs(35);
    registry.mark_cooldown("ch1", Some(101), past);
    let action2 = registry.evaluate_poll("ch1", None, &detail, Duration::from_secs(30));

    match action2 {
        PollAction::ReadyToRecord {
            was_api_restricted, ..
        } => {
            assert!(!was_api_restricted);
        }
        other => panic!("Expected ReadyToRecord, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_cooldown_live_id_switched_bypasses_cooldown() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    registry.finish_recording("ch1", Some(101)); // finished live_id 101

    // New stream starts with live_id 202
    let detail_new = make_open_detail("ch1", Some(202), "Streamer1", "Brand New Stream");
    let action = registry.evaluate_poll("ch1", None, &detail_new, Duration::from_secs(30));

    match action {
        PollAction::ReadyToRecord { info, .. } => {
            assert_eq!(info.live_id, Some(202));
        }
        other => panic!("Expected ReadyToRecord for new live_id, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_restricted_and_return_to_public() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    let restricted_detail = chzzk_load::chzzk::models::LiveDetail::Restricted {
        channel_id: "ch1".to_string(),
        live_id: Some(500),
        streamer_name: "Streamer1".to_string(),
        title: "19+ Stream".to_string(),
        chat_channel_id: None,
        adult: true,
    };

    // First time restricted -> is_newly_restricted = true
    let action1 = registry.evaluate_poll("ch1", None, &restricted_detail, Duration::from_secs(30));
    match action1 {
        PollAction::Restricted {
            is_newly_restricted,
            reason,
            ..
        } => {
            assert!(is_newly_restricted);
            assert_eq!(reason, RestrictionReason::AgeRestricted);
        }
        other => panic!("Expected Restricted, got {other:?}"),
    }

    // Subsequent poll while still restricted -> is_newly_restricted = false
    let action2 = registry.evaluate_poll("ch1", None, &restricted_detail, Duration::from_secs(30));
    match action2 {
        PollAction::Restricted {
            is_newly_restricted,
            ..
        } => {
            assert!(!is_newly_restricted);
        }
        other => panic!("Expected Restricted, got {other:?}"),
    }

    // Stream returns to public (Open) -> ReadyToRecord with was_api_restricted = true
    let open_detail = make_open_detail("ch1", Some(500), "Streamer1", "Now Public Stream");
    let action3 = registry.evaluate_poll("ch1", None, &open_detail, Duration::from_secs(30));
    match action3 {
        PollAction::ReadyToRecord {
            was_api_restricted, ..
        } => {
            assert!(was_api_restricted);
        }
        other => panic!("Expected ReadyToRecord after restriction, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_key_forbidden_and_live_id_switch() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    // Marked restricted due to 403 Forbidden on live_id 777
    registry.mark_restricted("ch1", Some(777), RestrictionReason::KeyForbidden);

    // Open detail with SAME live_id 777 -> Remains restricted
    let open_same = make_open_detail("ch1", Some(777), "Streamer1", "Forbidden Stream");
    let action1 = registry.evaluate_poll("ch1", None, &open_same, Duration::from_secs(30));
    match action1 {
        PollAction::Restricted {
            is_newly_restricted,
            reason,
            ..
        } => {
            assert!(!is_newly_restricted);
            assert_eq!(reason, RestrictionReason::KeyForbidden);
        }
        other => panic!("Expected Restricted for same live_id, got {other:?}"),
    }

    // Open detail with DIFFERENT live_id 888 -> ReadyToRecord (new broadcast!)
    let open_new = make_open_detail("ch1", Some(888), "Streamer1", "New Unrestricted Stream");
    let action2 = registry.evaluate_poll("ch1", None, &open_new, Duration::from_secs(30));
    match action2 {
        PollAction::ReadyToRecord {
            was_api_restricted,
            info,
            ..
        } => {
            assert!(!was_api_restricted);
            assert_eq!(info.live_id, Some(888));
        }
        other => panic!("Expected ReadyToRecord for switched live_id, got {other:?}"),
    }
}

#[test]
fn test_evaluate_poll_stream_closed_resets_state_and_cancels_token() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    let session = make_test_session("Streamer1", "Stream to Close");
    let token = CancellationToken::new();
    registry.start_recording("ch1", session, token.clone());

    let close_detail = chzzk_load::chzzk::models::LiveDetail::Close {
        streamer_name: Some("Streamer1".to_string()),
    };

    let action = registry.evaluate_poll("ch1", None, &close_detail, Duration::from_secs(30));
    match action {
        PollAction::StreamClosed {
            display_name,
            was_recording,
        } => {
            assert_eq!(display_name, "Streamer1");
            assert!(was_recording);
        }
        other => panic!("Expected StreamClosed, got {other:?}"),
    }

    assert!(token.is_cancelled());
    assert_eq!(
        registry.channel_state("ch1").kind(),
        ChannelLifecycleKind::Idle
    );
}

#[test]
fn test_registry_snapshot_queries() {
    let registry = ChannelLifecycleRegistry::new();
    registry.cache_streamer_name("ch1", "Name1");
    registry.cache_streamer_name("ch2", "Name2");

    let s1 = make_test_session("Name1", "Stream 1");
    let t1 = CancellationToken::new();
    registry.start_recording("ch1", s1, t1);

    registry.finish_recording("ch2", Some(999));

    let names = registry.cached_streamer_names();
    assert_eq!(names.get("ch1").map(|s| s.as_str()), Some("Name1"));
    assert_eq!(names.get("ch2").map(|s| s.as_str()), Some("Name2"));

    let states = registry.all_channel_states();
    assert_eq!(
        states.get("ch1").map(|s| s.kind()),
        Some(ChannelLifecycleKind::Recording)
    );
    assert_eq!(
        states.get("ch2").map(|s| s.kind()),
        Some(ChannelLifecycleKind::Cooldown)
    );

    let active_sessions = registry.active_sessions();
    assert_eq!(active_sessions.len(), 1);
    assert_eq!(
        active_sessions.get("ch1").unwrap().initial_title,
        "Stream 1"
    );
}

#[test]
fn test_evaluate_poll_close_preserves_unexpired_cooldown() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::{Duration, Instant};

    let registry = ChannelLifecycleRegistry::new();
    // Channel recently entered cooldown with 30s window
    registry.mark_cooldown("ch1", Some(123), Instant::now());

    let close_detail = chzzk_load::chzzk::models::LiveDetail::Close {
        streamer_name: None,
    };
    let action = registry.evaluate_poll("ch1", None, &close_detail, Duration::from_secs(30));

    match action {
        PollAction::StreamClosed { was_recording, .. } => {
            assert!(!was_recording);
        }
        other => panic!("Expected StreamClosed, got {other:?}"),
    }

    // Crucial invariant: Cooldown is NOT wiped out if cooldown is unexpired!
    assert_eq!(
        registry.channel_state("ch1").kind(),
        ChannelLifecycleKind::Cooldown
    );
}

#[test]
fn test_evaluate_poll_live_id_switched_during_recording() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::Duration;

    let registry = ChannelLifecycleRegistry::new();
    let mut session = make_test_session("Streamer1", "Broadcast 1");
    session.current_metadata.live_id = Some(111);
    let token = CancellationToken::new();
    registry.start_recording("ch1", session, token.clone());

    // Streamer suddenly starts a new broadcast with live_id 222
    let detail_new = make_open_detail("ch1", Some(222), "Streamer1", "Broadcast 2");
    let action = registry.evaluate_poll("ch1", None, &detail_new, Duration::from_secs(30));

    match action {
        PollAction::ReadyToRecord { info, .. } => {
            assert_eq!(info.live_id, Some(222));
        }
        other => panic!("Expected ReadyToRecord on live_id switch during recording, got {other:?}"),
    }

    // Previous session cancellation token must be triggered!
    assert!(token.is_cancelled());
}

#[test]
fn test_evaluate_poll_cooldown_retains_live_id_when_info_live_id_is_none() {
    use chzzk_load::engine::registry::PollAction;
    use std::time::{Duration, Instant};

    let registry = ChannelLifecycleRegistry::new();
    registry.mark_cooldown("ch1", Some(999), Instant::now());

    // Open detail without live_id (None)
    let detail = make_open_detail("ch1", None, "Streamer1", "Live Title");
    let action = registry.evaluate_poll("ch1", None, &detail, Duration::from_secs(30));

    match action {
        PollAction::InCooldown { live_id, .. } => {
            assert_eq!(
                live_id,
                Some(999),
                "Should retain cooldown live_id even when info.live_id is None"
            );
        }
        other => panic!("Expected InCooldown, got {other:?}"),
    }
}
