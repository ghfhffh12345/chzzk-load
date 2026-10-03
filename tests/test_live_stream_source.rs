use chzzk_load::chzzk::models::LiveDetail;
use chzzk_load::chzzk::source::{LiveStreamSource, MockLiveStreamSource};
use std::sync::Arc;

#[tokio::test]
async fn test_source_object_safety_and_default_state() {
    let mock = MockLiveStreamSource::new();
    let source: Arc<dyn LiveStreamSource> = Arc::new(mock);

    // Default state for unconfigured channel is offline (LiveDetail::Close)
    let detail = source
        .get_live_detail("unconfigured_channel")
        .await
        .expect("query should succeed");

    assert!(matches!(
        detail,
        LiveDetail::Close {
            streamer_name: None
        }
    ));
}

#[tokio::test]
async fn test_configurable_default_channel_state() {
    let default_state = LiveDetail::Close {
        streamer_name: Some("DefaultStreamer".to_string()),
    };
    let mock = MockLiveStreamSource::new().with_default_channel_state(default_state.clone());

    let detail = mock
        .get_live_detail("unconfigured_channel_1")
        .await
        .expect("query should succeed");
    assert_eq!(detail, default_state);

    let detail2 = mock
        .get_live_detail("unconfigured_channel_2")
        .await
        .expect("query should succeed");
    assert_eq!(detail2, default_state);

    // Explicit sticky state overrides default channel state
    let specific_state = LiveDetail::Restricted {
        channel_id: "ch_specific".to_string(),
        live_id: Some(10),
        streamer_name: "SpecificStreamer".to_string(),
        title: "Subscribers Only".to_string(),
        chat_channel_id: None,
        adult: false,
    };
    mock.set_channel_state("ch_specific", specific_state.clone());

    let specific_detail = mock
        .get_live_detail("ch_specific")
        .await
        .expect("query should succeed");
    assert_eq!(specific_detail, specific_state);
}

#[tokio::test]
async fn test_sticky_channel_state_configuration() {
    let mock = MockLiveStreamSource::new();
    let restricted_detail = LiveDetail::Restricted {
        channel_id: "ch_restricted".to_string(),
        live_id: Some(999),
        streamer_name: "RestrictedStreamer".to_string(),
        title: "Sub Only".to_string(),
        chat_channel_id: Some("chat_999".to_string()),
        adult: false,
    };

    mock.set_channel_state("ch_restricted", restricted_detail.clone());

    // Multiple polls return the same sticky state
    for _ in 0..3 {
        let detail = mock
            .get_live_detail("ch_restricted")
            .await
            .expect("query should succeed");
        assert_eq!(detail, restricted_detail);
    }

    // Builder pattern with_channel_state also works
    let mock_builder = MockLiveStreamSource::new()
        .with_channel_state("ch_restricted_2", restricted_detail.clone());
    let detail_b = mock_builder
        .get_live_detail("ch_restricted_2")
        .await
        .expect("query should succeed");
    assert_eq!(detail_b, restricted_detail);
}

#[tokio::test]
async fn test_sequential_channel_state_transitions() {
    let mock = MockLiveStreamSource::new();

    let offline_detail = LiveDetail::Close {
        streamer_name: Some("StreamerA".to_string()),
    };
    let open_detail = LiveDetail::Open(chzzk_load::chzzk::models::LiveStreamInfo {
        channel_id: "ch_seq".to_string(),
        live_id: Some(101),
        streamer_name: "StreamerA".to_string(),
        title: "Live Part 1".to_string(),
        hls_url: "https://cdn.example.com/live/101.m3u8".to_string(),
        chat_channel_id: Some("chat_101".to_string()),
        metadata: chzzk_load::chzzk::models_metadata::StreamMetadataState {
            live_id: Some(101),
            open_date: None,
            close_date: None,
            channel_id: "ch_seq".to_string(),
            channel_name: "StreamerA".to_string(),
            live_title: "Live Part 1".to_string(),
            category_type: None,
            live_category: None,
            live_category_value: None,
            tags: vec![],
            access_tier: chzzk_load::chzzk::models_metadata::StreamAccessTier::Public,
            is_kr_only: false,
            is_chat_active: true,
            is_watch_party: false,
            paid_promotion: false,
            drops_campaign_no: None,
        },
    });
    let mut title_changed_detail = open_detail.clone();
    if let LiveDetail::Open(ref mut info) = title_changed_detail {
        info.title = "Live Part 2 - Changed Title".to_string();
        info.metadata.live_title = "Live Part 2 - Changed Title".to_string();
    }

    // Set sticky fallback state to offline
    mock.set_channel_state("ch_seq", offline_detail.clone());

    // Enqueue 2 sequential transitions
    mock.enqueue_channel_state("ch_seq", open_detail.clone());
    mock.enqueue_channel_states("ch_seq", vec![title_changed_detail.clone()]);

    // 1st tick: returns open_detail
    let tick1 = mock.get_live_detail("ch_seq").await.expect("tick 1 ok");
    assert_eq!(tick1, open_detail);

    // 2nd tick: returns title_changed_detail
    let tick2 = mock.get_live_detail("ch_seq").await.expect("tick 2 ok");
    assert_eq!(tick2, title_changed_detail);

    // 3rd tick: queue exhausted, falls back to sticky offline_detail
    let tick3 = mock.get_live_detail("ch_seq").await.expect("tick 3 ok");
    assert_eq!(tick3, offline_detail);

    // 4th tick: remains sticky offline_detail
    let tick4 = mock.get_live_detail("ch_seq").await.expect("tick 4 ok");
    assert_eq!(tick4, offline_detail);
}

#[tokio::test]
async fn test_api_error_simulation() {
    let mock = MockLiveStreamSource::new();

    // 1. Channel-specific error
    mock.inject_channel_error("ch_failing", "500 Internal Server Error");
    let res = mock.get_live_detail("ch_failing").await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("500 Internal Server Error")
    );

    // Other channels are unaffected
    let ok_res = mock.get_live_detail("ch_healthy").await;
    assert!(ok_res.is_ok());

    // Clearing channel error recovers polling
    mock.clear_channel_error("ch_failing");
    let recovered_res = mock.get_live_detail("ch_failing").await;
    assert!(recovered_res.is_ok());

    // 2. Global error affects all channels
    mock.inject_global_error("503 Service Unavailable");
    assert!(mock.get_live_detail("ch_healthy").await.is_err());
    assert!(mock.get_live_detail("ch_failing").await.is_err());

    // Clearing global error restores normal operation
    mock.clear_global_error();
    assert!(mock.get_live_detail("ch_healthy").await.is_ok());
}

#[tokio::test]
async fn test_chat_authorization_tokens_and_errors() {
    let mock = MockLiveStreamSource::new();

    // 1. Default fallback token
    let token = mock.get_chat_access_token("chat_default").await.unwrap();
    assert_eq!(token, "mock_access_token");

    // 2. Set channel-specific chat token
    mock.set_chat_token("chat_custom", "custom_secret_token_123");
    let custom_token = mock.get_chat_access_token("chat_custom").await.unwrap();
    assert_eq!(custom_token, "custom_secret_token_123");

    // 3. Builder pattern with_chat_token and with_default_chat_token
    let builder_mock = MockLiveStreamSource::new()
        .with_default_chat_token("custom_default_token")
        .with_chat_token("chat_explicit", "explicit_token");
    assert_eq!(
        builder_mock
            .get_chat_access_token("chat_other")
            .await
            .unwrap(),
        "custom_default_token"
    );
    assert_eq!(
        builder_mock
            .get_chat_access_token("chat_explicit")
            .await
            .unwrap(),
        "explicit_token"
    );

    // 4. Inject chat token error
    mock.inject_chat_token_error("chat_failing", "401 Unauthorized");
    let err_res = mock.get_chat_access_token("chat_failing").await;
    assert!(err_res.is_err());
    assert!(
        err_res
            .unwrap_err()
            .to_string()
            .contains("401 Unauthorized")
    );

    // Other chat channels still succeed
    assert!(mock.get_chat_access_token("chat_custom").await.is_ok());

    // Clearing chat token error recovers
    mock.clear_chat_token_error("chat_failing");
    assert!(mock.get_chat_access_token("chat_failing").await.is_ok());
}

#[tokio::test]
async fn test_per_channel_call_counting_and_reset() {
    let mock = MockLiveStreamSource::new();

    assert_eq!(mock.call_count("ch_a"), 0);
    assert_eq!(mock.total_call_count(), 0);
    assert_eq!(mock.chat_token_call_count("chat_a"), 0);
    assert_eq!(mock.total_chat_token_call_count(), 0);

    // Poll ch_a 3 times
    for _ in 0..3 {
        let _ = mock.get_live_detail("ch_a").await;
    }
    // Poll ch_b 2 times
    for _ in 0..2 {
        let _ = mock.get_live_detail("ch_b").await;
    }

    assert_eq!(mock.call_count("ch_a"), 3);
    assert_eq!(mock.call_count("ch_b"), 2);
    assert_eq!(mock.call_count("ch_c"), 0);
    assert_eq!(mock.total_call_count(), 5);

    // Request chat token for chat_a 4 times
    for _ in 0..4 {
        let _ = mock.get_chat_access_token("chat_a").await;
    }
    assert_eq!(mock.chat_token_call_count("chat_a"), 4);
    assert_eq!(mock.chat_token_call_count("chat_b"), 0);
    assert_eq!(mock.total_chat_token_call_count(), 4);

    // Reset counters
    mock.reset_call_counts();
    assert_eq!(mock.call_count("ch_a"), 0);
    assert_eq!(mock.call_count("ch_b"), 0);
    assert_eq!(mock.total_call_count(), 0);
    assert_eq!(mock.chat_token_call_count("chat_a"), 0);
    assert_eq!(mock.total_chat_token_call_count(), 0);
}

#[tokio::test]
async fn test_chat_ws_url_configuration() {
    let mock = MockLiveStreamSource::new().with_chat_ws_url("wss://mock.chat.naver.com/chat");

    let source: Arc<dyn LiveStreamSource> = Arc::new(mock);
    assert_eq!(source.chat_ws_url(), Some("wss://mock.chat.naver.com/chat"));
}

#[tokio::test]
async fn test_chzzk_client_implements_live_stream_source_directly() {
    use chzzk_load::chzzk::client::ChzzkClient;
    use chzzk_load::config::ChzzkConfig;

    let config = ChzzkConfig {
        nid_aut: "test_aut".to_string(),
        nid_ses: "test_ses".to_string(),
    };
    let client = ChzzkClient::new(&config)
        .with_base_url("http://invalid-domain.invalid")
        .with_game_base_url("http://invalid-domain.invalid")
        .with_chat_ws_url("wss://custom.chat.naver.com");

    // Trait object conversion with zero wrapper overhead
    let source: Arc<dyn LiveStreamSource> = Arc::new(client);

    assert_eq!(source.chat_ws_url(), Some("wss://custom.chat.naver.com"));

    // Verify dynamic trait object asynchronous dispatch works for both methods
    let live_detail_result = source.get_live_detail("test_channel_id").await;
    assert!(live_detail_result.is_err());

    let token_result = source.get_chat_access_token("test_chat_id").await;
    assert!(token_result.is_err());
}
