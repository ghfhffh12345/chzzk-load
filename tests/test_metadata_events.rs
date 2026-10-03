use chzzk_load::chzzk::models_metadata::{
    BroadcastPolicies, CategoryType, ChatRulesState, MetadataEvent, MetadataEventType,
    MetadataEventV2, StreamAccessTier, StreamMetadataState, StreamMetadataStateV2, WatchPartyState,
};

fn sample_metadata_state() -> StreamMetadataState {
    StreamMetadataState {
        live_id: Some(21378610),
        open_date: Some("2026-09-30 14:00:00".to_string()),
        close_date: None,
        channel_id: "chan_123".to_string(),
        channel_name: "TestStreamer".to_string(),
        channel_image_url: Some("https://test.com/pfp.png".to_string()),
        verified_mark: true,
        live_title: "Initial Title".to_string(),
        category_type: Some(CategoryType::Talk),
        live_category: Some("talk".to_string()),
        live_category_value: Some("Just Chatting".to_string()),
        tags: vec!["소통".to_string()],
        access_tier: StreamAccessTier::Public,
        policies: BroadcastPolicies {
            kr_only_viewing: false,
            playable_status: Some("PLAYABLE".to_string()),
            blind_type: None,
            time_machine_active: true,
            clip_active: true,
            tv_app_viewing_policy_type: None,
        },
        watch_party: WatchPartyState::default(),
        chat_rules: ChatRulesState {
            chat_active: true,
            chat_available_group: Some("ALL".to_string()),
            chat_available_condition: Some("NONE".to_string()),
            min_follower_minute: Some(0),
            allow_subscriber_in_follower_mode: false,
            chat_slow_mode_sec: Some(0),
            chat_emoji_mode: false,
            chat_donation_ranking_exposure: true,
        },
        paid_promotion: false,
        drops_campaign_no: None,
        log_power_active: false,
        live_thumbnail_image_url: Some("https://test.com/thumb.jpg".to_string()),
        default_thumbnail_image_url: None,
        concurrent_user_count: Some(100),
        accumulate_count: Some(500),
    }
}

fn sample_metadata_state_v2() -> StreamMetadataStateV2 {
    StreamMetadataStateV2 {
        live_id: Some(12345),
        open_date: Some("2026-10-03 01:00:00".to_string()),
        close_date: None,
        channel_id: "chan_999".to_string(),
        channel_name: "TestStreamer".to_string(),
        live_title: "Lean Title".to_string(),
        category_type: Some(CategoryType::Game),
        live_category: Some("game".to_string()),
        live_category_value: Some("League of Legends".to_string()),
        tags: vec!["LOL".to_string()],
        access_tier: StreamAccessTier::AdultOnly,
        is_kr_only: true,
        is_chat_active: false,
        is_watch_party: true,
        paid_promotion: true,
        drops_campaign_no: Some("camp_77".to_string()),
    }
}

#[test]
fn test_metadata_delta_computation_no_change_on_telemetry() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    // Changing only telemetry or thumbnails should NOT produce a delta
    state2.concurrent_user_count = Some(200);
    state2.accumulate_count = Some(600);
    state2.live_thumbnail_image_url = Some("https://test.com/thumb2.jpg".to_string());
    state2.default_thumbnail_image_url = Some("https://test.com/default2.jpg".to_string());
    state2.channel_image_url = Some("https://test.com/pfp2.png".to_string());

    assert!(state1.compute_delta(&state2).is_none());
}

#[test]
fn test_metadata_delta_computation_title_and_category_change() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    state2.live_title = "New Game Broadcast".to_string();
    state2.category_type = Some(CategoryType::Game);
    state2.live_category_value = Some("Valorant".to_string());

    let delta = state1.compute_delta(&state2).expect("delta must exist");
    assert_eq!(delta.live_title.as_ref().unwrap().old, "Initial Title");
    assert_eq!(delta.live_title.as_ref().unwrap().new, "New Game Broadcast");
    assert_eq!(
        delta.category_type.as_ref().unwrap().new,
        Some(CategoryType::Game)
    );
    assert_eq!(
        delta.live_category_value.as_ref().unwrap().new,
        Some("Valorant".to_string())
    );
    assert!(delta.watch_party.is_none());
}

#[test]
fn test_metadata_delta_computation_watch_party_and_drops() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    state2.watch_party = WatchPartyState {
        is_active: true,
        no: Some(520),
        tag: Some("2026아시안게임".to_string()),
        party_type: Some("RS".to_string()),
        paid_product_id: None,
    };
    state2.drops_campaign_no = Some("camp_val_99".to_string());
    state2.policies.kr_only_viewing = true;

    let delta = state1.compute_delta(&state2).expect("delta must exist");
    assert_eq!(delta.watch_party.as_ref().unwrap().new.no, Some(520));
    assert_eq!(
        delta.drops_campaign_no.as_ref().unwrap().new,
        Some("camp_val_99".to_string())
    );
    assert!(delta.policies.as_ref().unwrap().new.kr_only_viewing);
}

#[test]
fn test_metadata_jsonl_serialization_roundtrip() {
    let state = sample_metadata_state();
    let event = MetadataEvent {
        version: 1,
        event: MetadataEventType::InitialState,
        timestamp: "2026-09-30T05:00:00Z".to_string(),
        time_local: "2026-09-30 14:00:00".to_string(),
        stream_offset_ms: 0,
        changes: None,
        state,
    };

    let serialized = serde_json::to_string(&event).unwrap();
    let deserialized: MetadataEvent = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized.version, 1);
    assert_eq!(deserialized.event, MetadataEventType::InitialState);
    assert_eq!(deserialized.stream_offset_ms, 0);
    assert_eq!(deserialized.state.channel_name, "TestStreamer");
    assert!(deserialized.changes.is_none());
}

#[test]
fn test_deserialize_optional_string_or_number() {
    #[derive(serde::Deserialize)]
    struct TestContainer {
        #[serde(
            default,
            deserialize_with = "chzzk_load::chzzk::models_metadata::deserialize_optional_string_or_number"
        )]
        val: Option<String>,
    }

    let parsed_str: TestContainer = serde_json::from_str(r#"{"val": "campaign_123"}"#).unwrap();
    assert_eq!(parsed_str.val, Some("campaign_123".to_string()));

    let parsed_num: TestContainer = serde_json::from_str(r#"{"val": 98765}"#).unwrap();
    assert_eq!(parsed_num.val, Some("98765".to_string()));

    let parsed_null: TestContainer = serde_json::from_str(r#"{"val": null}"#).unwrap();
    assert_eq!(parsed_null.val, None);

    let parsed_empty: TestContainer = serde_json::from_str(r#"{}"#).unwrap();
    assert_eq!(parsed_empty.val, None);
}

#[test]
fn test_lean_stream_metadata_state_v2_schema_and_omissions() {
    let state = sample_metadata_state_v2();

    let serialized = serde_json::to_string(&state).unwrap();
    let val: serde_json::Value = serde_json::from_str(&serialized).unwrap();

    // Verify expected fields are serialized
    assert_eq!(val["live_id"], 12345);
    assert_eq!(val["open_date"], "2026-10-03 01:00:00");
    assert_eq!(val["channel_id"], "chan_999");
    assert_eq!(val["channel_name"], "TestStreamer");
    assert_eq!(val["live_title"], "Lean Title");
    assert_eq!(val["category_type"], "GAME");
    assert_eq!(val["live_category"], "game");
    assert_eq!(val["live_category_value"], "League of Legends");
    assert_eq!(val["tags"][0], "LOL");
    assert_eq!(val["access_tier"], "ADULT_ONLY");
    assert_eq!(val["is_kr_only"], true);
    assert_eq!(val["is_chat_active"], false);
    assert_eq!(val["is_watch_party"], true);
    assert_eq!(val["paid_promotion"], true);
    assert_eq!(val["drops_campaign_no"], "camp_77");

    // Verify omitted and skipped fields are not present
    assert!(val.get("close_date").is_none());
    assert!(val.get("concurrent_user_count").is_none());
    assert!(val.get("accumulate_count").is_none());
    assert!(val.get("channel_image_url").is_none());
    assert!(val.get("live_thumbnail_image_url").is_none());
    assert!(val.get("default_thumbnail_image_url").is_none());
    assert!(val.get("verified_mark").is_none());
    assert!(val.get("log_power_active").is_none());
    assert!(val.get("policies").is_none());
    assert!(val.get("watch_party").is_none());
    assert!(val.get("chat_rules").is_none());

    // Roundtrip verification
    let roundtripped: StreamMetadataStateV2 = serde_json::from_str(&serialized).unwrap();
    assert_eq!(roundtripped, state);

    // Verify deserialization with aliases and defaults
    let json_with_aliases = r#"{
        "channel_id": "c1",
        "streamer_name": "AliasStreamer",
        "title": "AliasTitle"
    }"#;
    let parsed_alias: StreamMetadataStateV2 = serde_json::from_str(json_with_aliases).unwrap();
    assert_eq!(parsed_alias.channel_name, "AliasStreamer");
    assert_eq!(parsed_alias.live_title, "AliasTitle");
    assert!(parsed_alias.is_chat_active);
    assert!(!parsed_alias.is_kr_only);
    assert!(!parsed_alias.is_watch_party);
}

#[test]
fn test_metadata_event_v2_jsonl_serialization_roundtrip() {
    let state = StreamMetadataStateV2 {
        live_id: Some(999888),
        open_date: Some("2026-10-03 12:00:00".to_string()),
        close_date: None,
        channel_id: "chan_v2".to_string(),
        channel_name: "V2Streamer".to_string(),
        live_title: "Version 2 Stream".to_string(),
        category_type: Some(CategoryType::Talk),
        live_category: Some("talk".to_string()),
        live_category_value: Some("Chatting".to_string()),
        tags: vec!["chat".to_string()],
        access_tier: StreamAccessTier::Public,
        is_kr_only: false,
        is_chat_active: true,
        is_watch_party: false,
        paid_promotion: false,
        drops_campaign_no: None,
    };

    let initial_event = MetadataEventV2::initial("2026-10-03T03:00:00Z".to_string(), state.clone());

    let serialized_initial = serde_json::to_string(&initial_event).unwrap();
    let val_initial: serde_json::Value = serde_json::from_str(&serialized_initial).unwrap();

    // Verify v2 wire format specification
    assert_eq!(val_initial["version"], 2);
    assert_eq!(val_initial["event"], "INITIAL_STATE");
    assert_eq!(val_initial["timestamp"], "2026-10-03T03:00:00Z");
    assert_eq!(val_initial["stream_offset_ms"], 0);
    assert_eq!(val_initial["state"]["channel_id"], "chan_v2");
    assert_eq!(val_initial["state"]["live_title"], "Version 2 Stream");

    // Strictly verify omission of v1 legacy wire fields
    assert!(val_initial.get("time_local").is_none());
    assert!(val_initial.get("changes").is_none());

    // Roundtrip verification
    let deserialized_initial: MetadataEventV2 = serde_json::from_str(&serialized_initial).unwrap();
    assert_eq!(deserialized_initial, initial_event);

    // Simulate metadata change event
    let mut updated_state = state;
    updated_state.live_title = "Updated Title v2".to_string();
    updated_state.tags = vec!["chat".to_string(), "update".to_string()];

    let changed_event =
        MetadataEventV2::changed("2026-10-03T03:30:00Z".to_string(), 1_800_000, updated_state);

    let serialized_changed = serde_json::to_string(&changed_event).unwrap();
    let val_changed: serde_json::Value = serde_json::from_str(&serialized_changed).unwrap();

    assert_eq!(val_changed["version"], 2);
    assert_eq!(val_changed["event"], "METADATA_CHANGED");
    assert_eq!(val_changed["timestamp"], "2026-10-03T03:30:00Z");
    assert_eq!(val_changed["stream_offset_ms"], 1_800_000);
    assert_eq!(val_changed["state"]["live_title"], "Updated Title v2");
    assert!(val_changed.get("time_local").is_none());
    assert!(val_changed.get("changes").is_none());

    // Verify JSON Lines formatting (one JSON per line)
    let jsonl_content = format!("{serialized_initial}\n{serialized_changed}\n");
    let parsed_events: Vec<MetadataEventV2> = jsonl_content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(parsed_events.len(), 2);
    assert_eq!(parsed_events[0].event, MetadataEventType::InitialState);
    assert_eq!(parsed_events[1].event, MetadataEventType::MetadataChanged);
    assert_eq!(parsed_events[1].stream_offset_ms, 1_800_000);

    // Verify to_json_line helper produces newline-terminated record
    let jsonl_line = initial_event.to_json_line().unwrap();
    assert!(jsonl_line.ends_with('\n'));
    let roundtripped_line: MetadataEventV2 = serde_json::from_str(jsonl_line.trim_end()).unwrap();
    assert_eq!(roundtripped_line, initial_event);
}

#[test]
fn test_v1_to_v2_domain_model_conversion() {
    let mut state_v1 = sample_metadata_state();
    state_v1.policies.kr_only_viewing = true;
    state_v1.chat_rules.chat_active = false;
    state_v1.watch_party.is_active = true;
    state_v1.paid_promotion = true;
    state_v1.drops_campaign_no = Some("camp_123".to_string());

    let state_v2: StreamMetadataStateV2 = (&state_v1).into();
    assert!(state_v2.is_kr_only);
    assert!(!state_v2.is_chat_active);
    assert!(state_v2.is_watch_party);
    assert!(state_v2.paid_promotion);
    assert_eq!(state_v2.drops_campaign_no, Some("camp_123".to_string()));
    assert_eq!(state_v2.live_id, state_v1.live_id);
    assert_eq!(state_v2.open_date, state_v1.open_date);
    assert_eq!(state_v2.close_date, state_v1.close_date);
    assert_eq!(state_v2.channel_id, state_v1.channel_id);
    assert_eq!(state_v2.channel_name, state_v1.channel_name);
    assert_eq!(state_v2.live_title, state_v1.live_title);
    assert_eq!(state_v2.category_type, state_v1.category_type);
    assert_eq!(state_v2.live_category, state_v1.live_category);
    assert_eq!(state_v2.live_category_value, state_v1.live_category_value);
    assert_eq!(state_v2.tags, state_v1.tags);
    assert_eq!(state_v2.access_tier, state_v1.access_tier);

    let state_v2_owned: StreamMetadataStateV2 = state_v1.clone().into();
    assert_eq!(state_v2_owned, state_v2);

    let event_v1 = MetadataEvent {
        version: 1,
        event: MetadataEventType::MetadataChanged,
        timestamp: "2026-10-03T04:00:00Z".to_string(),
        time_local: "2026-10-03 13:00:00".to_string(),
        stream_offset_ms: 120_000,
        changes: None,
        state: state_v1,
    };

    let event_v2: MetadataEventV2 = (&event_v1).into();
    assert_eq!(event_v2.version, 2);
    assert_eq!(event_v2.event, MetadataEventType::MetadataChanged);
    assert_eq!(event_v2.timestamp, "2026-10-03T04:00:00Z");
    assert_eq!(event_v2.stream_offset_ms, 120_000);
    assert_eq!(event_v2.state, state_v2);

    let event_v2_owned: MetadataEventV2 = event_v1.into();
    assert_eq!(event_v2_owned, event_v2);
}

#[test]
fn test_metadata_v2_schema_validation_rejections() {
    // Malformed JSON should fail
    let malformed = r#"{"version": 2, "event": "INITIAL_STATE", "timestamp":"#;
    assert!(serde_json::from_str::<MetadataEventV2>(malformed).is_err());

    // Invalid event type should fail
    let invalid_event = r#"{
        "version": 2,
        "event": "INVALID_EVENT_TYPE",
        "timestamp": "2026-10-03T01:00:00Z",
        "stream_offset_ms": 0,
        "state": {
            "channel_id": "c1",
            "channel_name": "Streamer",
            "live_title": "Title"
        }
    }"#;
    assert!(serde_json::from_str::<MetadataEventV2>(invalid_event).is_err());

    // Invalid access tier in state should fail
    let invalid_access_tier = r#"{
        "channel_id": "c1",
        "channel_name": "Streamer",
        "live_title": "Title",
        "access_tier": "INVALID_TIER"
    }"#;
    assert!(serde_json::from_str::<StreamMetadataStateV2>(invalid_access_tier).is_err());
}
