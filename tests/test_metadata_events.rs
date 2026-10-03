use chzzk_load::chzzk::models_metadata::{
    CategoryType, MetadataEvent, MetadataEventType, StreamAccessTier, StreamMetadataState,
};

fn sample_metadata_state() -> StreamMetadataState {
    StreamMetadataState {
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
fn test_stream_metadata_state_direct_equality_and_title_change_detection() {
    let state1 = sample_metadata_state();
    let state2 = state1.clone();

    // Direct equality on identical states
    assert_eq!(state1, state2);
    assert_eq!(state1.live_title, state2.live_title);

    // Title change detected via equality and field inspection
    let mut title_changed = state1.clone();
    title_changed.live_title = "Brand New Stream Title".to_string();
    assert_ne!(state1, title_changed);
    assert_ne!(state1.live_title, title_changed.live_title);

    // Category changes detected via equality
    let mut category_changed = state1.clone();
    category_changed.category_type = Some(CategoryType::Sports);
    category_changed.live_category_value = Some("E-Sports".to_string());
    assert_ne!(state1, category_changed);
    assert_eq!(state1.live_title, category_changed.live_title);

    // Tags change detected via equality
    let mut tags_changed = state1.clone();
    tags_changed.tags = vec!["LOL".to_string(), "Tournament".to_string()];
    assert_ne!(state1, tags_changed);

    // Access tier change detected via equality
    let mut access_changed = state1.clone();
    access_changed.access_tier = StreamAccessTier::Public;
    assert_ne!(state1, access_changed);

    // Flattened policy flags detected via equality
    let mut kr_only_changed = state1.clone();
    kr_only_changed.is_kr_only = false;
    assert_ne!(state1, kr_only_changed);

    let mut chat_active_changed = state1.clone();
    chat_active_changed.is_chat_active = true;
    assert_ne!(state1, chat_active_changed);

    let mut watch_party_changed = state1.clone();
    watch_party_changed.is_watch_party = false;
    assert_ne!(state1, watch_party_changed);

    let mut paid_promo_changed = state1.clone();
    paid_promo_changed.paid_promotion = false;
    assert_ne!(state1, paid_promo_changed);

    let mut drops_changed = state1.clone();
    drops_changed.drops_campaign_no = Some("camp_88".to_string());
    assert_ne!(state1, drops_changed);
}

#[test]
fn test_stream_metadata_title_change_flag_evaluation() {
    let old_state = sample_metadata_state();

    // 1. Identical metadata: no title change
    let same_state = old_state.clone();
    let title_changed = old_state.live_title != same_state.live_title;
    assert!(!title_changed);

    // 2. Modified title: title change flag is true
    let mut modified_title = old_state.clone();
    modified_title.live_title = "Brand New Stream Title".to_string();
    let title_changed = old_state.live_title != modified_title.live_title;
    assert!(title_changed);

    // 3. Modified non-title fields (tags, category, flags): title change flag is false
    let mut modified_tags = old_state.clone();
    modified_tags.tags.push("NewTag".to_string());
    assert_ne!(old_state, modified_tags);
    assert_eq!(old_state.live_title, modified_tags.live_title);
    assert!(!(old_state.live_title != modified_tags.live_title));

    let mut modified_category = old_state.clone();
    modified_category.category_type = Some(CategoryType::Sports);
    assert_ne!(old_state, modified_category);
    assert!(!(old_state.live_title != modified_category.live_title));

    let mut modified_flags = old_state.clone();
    modified_flags.is_kr_only = false;
    assert_ne!(old_state, modified_flags);
    assert!(!(old_state.live_title != modified_flags.live_title));
}

#[test]
fn test_stream_metadata_state_schema_and_omissions() {
    let state = sample_metadata_state();

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
    let roundtripped: StreamMetadataState = serde_json::from_str(&serialized).unwrap();
    assert_eq!(roundtripped, state);

    // Verify deserialization with aliases and defaults
    let json_with_aliases = r#"{
        "channel_id": "c1",
        "streamer_name": "AliasStreamer",
        "title": "AliasTitle"
    }"#;
    let parsed_alias: StreamMetadataState = serde_json::from_str(json_with_aliases).unwrap();
    assert_eq!(parsed_alias.channel_name, "AliasStreamer");
    assert_eq!(parsed_alias.live_title, "AliasTitle");
    assert!(parsed_alias.is_chat_active);
    assert!(!parsed_alias.is_kr_only);
    assert!(!parsed_alias.is_watch_party);
}

#[test]
fn test_metadata_event_jsonl_serialization_roundtrip() {
    let state = sample_metadata_state();

    let initial_event = MetadataEvent::initial("2026-10-03T03:00:00Z".to_string(), state.clone());

    let serialized_initial = serde_json::to_string(&initial_event).unwrap();
    let val_initial: serde_json::Value = serde_json::from_str(&serialized_initial).unwrap();

    // Verify v2 wire format specification
    assert_eq!(val_initial["version"], 2);
    assert_eq!(val_initial["event"], "INITIAL_STATE");
    assert_eq!(val_initial["timestamp"], "2026-10-03T03:00:00Z");
    assert_eq!(val_initial["stream_offset_ms"], 0);
    assert_eq!(val_initial["state"]["channel_id"], "chan_999");
    assert_eq!(val_initial["state"]["live_title"], "Lean Title");

    // Strictly verify omission of v1 legacy wire fields
    assert!(val_initial.get("time_local").is_none());
    assert!(val_initial.get("changes").is_none());

    // Roundtrip verification
    let deserialized_initial: MetadataEvent = serde_json::from_str(&serialized_initial).unwrap();
    assert_eq!(deserialized_initial, initial_event);

    // Simulate metadata change event
    let mut updated_state = state;
    updated_state.live_title = "Updated Title v2".to_string();
    updated_state.tags = vec!["LOL".to_string(), "update".to_string()];

    let changed_event =
        MetadataEvent::changed("2026-10-03T03:30:00Z".to_string(), 1_800_000, updated_state);

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
    let parsed_events: Vec<MetadataEvent> = jsonl_content
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
    let roundtripped_line: MetadataEvent = serde_json::from_str(jsonl_line.trim_end()).unwrap();
    assert_eq!(roundtripped_line, initial_event);
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
fn test_metadata_schema_validation_rejections() {
    // Malformed JSON should fail
    let malformed = r#"{"version": 2, "event": "INITIAL_STATE", "timestamp":"#;
    assert!(serde_json::from_str::<MetadataEvent>(malformed).is_err());

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
    assert!(serde_json::from_str::<MetadataEvent>(invalid_event).is_err());

    // Invalid access tier in state should fail
    let invalid_access_tier = r#"{
        "channel_id": "c1",
        "channel_name": "Streamer",
        "live_title": "Title",
        "access_tier": "INVALID_TIER"
    }"#;
    assert!(serde_json::from_str::<StreamMetadataState>(invalid_access_tier).is_err());
}
