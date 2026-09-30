use chzzk_load::chzzk::models_metadata::{
    BroadcastPolicies, CategoryType, ChatRulesState, MetadataEvent, MetadataEventType,
    StreamAccessTier, StreamMetadataState, WatchPartyState,
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
