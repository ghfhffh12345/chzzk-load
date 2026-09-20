use chzzk_load::chzzk::models::{ChzzkResponse, LiveDetailContent};
use chzzk_load::chzzk::client::extract_best_hls_url;

#[test]
fn test_parse_live_detail_and_extract_hls() {
    let mock_json = r#"{
        "code": 200,
        "message": null,
        "content": {
            "status": "OPEN",
            "liveTitle": "Stream Title",
            "channel": {
                "channelId": "4c3b44869c9b1399723ec28ec236f736",
                "channelName": "TesterStreamer"
            },
            "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://live.chzzk.naver.com/hls/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"720p\",\"path\":\"https://live.chzzk.naver.com/hls/720p.m3u8\"},{\"encodingTrackId\":\"1080p\",\"path\":\"https://live.chzzk.naver.com/hls/1080p.m3u8\"}]}]}"
        }
    }"#;

    let response: ChzzkResponse<LiveDetailContent> = serde_json::from_str(mock_json).unwrap();
    let content = response.content.expect("content should be present");
    assert_eq!(content.status, "OPEN");
    assert_eq!(content.channel.channel_name, "TesterStreamer");

    let hls_url = extract_best_hls_url(&content.live_playback_json).expect("valid hls url");
    assert_eq!(hls_url, "https://live.chzzk.naver.com/hls/1080p.m3u8");
}

#[test]
fn test_parse_offline_channel() {
    let mock_json = r#"{
        "code": 200,
        "message": null,
        "content": {
            "status": "CLOSE",
            "liveTitle": null,
            "channel": {
                "channelId": "4c3b44869c9b1399723ec28ec236f736",
                "channelName": "OfflineStreamer"
            },
            "livePlaybackJson": null
        }
    }"#;

    let response: ChzzkResponse<LiveDetailContent> = serde_json::from_str(mock_json).unwrap();
    let content = response.content.unwrap();
    assert_eq!(content.status, "CLOSE");
    assert!(content.live_playback_json.is_none());
}

#[test]
fn test_hls_fallback_to_720p() {
    let playback_json = Some(
        r#"{"media":[{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[{"encodingTrackId":"480p","path":"https://live.chzzk.naver.com/480p.m3u8"},{"encodingTrackId":"720p","path":"https://live.chzzk.naver.com/720p.m3u8"}]}]}"#.to_string()
    );
    let hls_url = extract_best_hls_url(&playback_json).unwrap();
    assert_eq!(hls_url, "https://live.chzzk.naver.com/720p.m3u8");
}

#[test]
fn test_hls_fallback_to_first_track() {
    let playback_json = Some(
        r#"{"media":[{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[{"encodingTrackId":"480p","path":"https://live.chzzk.naver.com/480p.m3u8"},{"encodingTrackId":"360p","path":"https://live.chzzk.naver.com/360p.m3u8"}]}]}"#.to_string()
    );
    let hls_url = extract_best_hls_url(&playback_json).unwrap();
    assert_eq!(hls_url, "https://live.chzzk.naver.com/480p.m3u8");
}

#[test]
fn test_hls_fallback_to_root_path_when_no_tracks() {
    let playback_json = Some(
        r#"{"media":[{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[]}]}"#.to_string()
    );
    let hls_url = extract_best_hls_url(&playback_json).unwrap();
    assert_eq!(hls_url, "https://live.chzzk.naver.com/master.m3u8");
}

#[test]
fn test_hls_error_when_no_hls_media() {
    let playback_json = Some(
        r#"{"media":[{"mediaId":"LLHLS","path":"https://live.chzzk.naver.com/llhls.m3u8","encodingTrack":[]}]}"#.to_string()
    );
    assert!(extract_best_hls_url(&playback_json).is_err());
}

#[test]
fn test_hls_error_when_none_or_invalid_json() {
    assert!(extract_best_hls_url(&None).is_err());
    assert!(extract_best_hls_url(&Some("not valid json".to_string())).is_err());
}

#[test]
fn test_chzzk_client_cookie_configuration() {
    use chzzk_load::chzzk::client::ChzzkClient;
    use chzzk_load::config::ChzzkConfig;

    let config_with_cookies = ChzzkConfig {
        nid_aut: "test_aut".to_string(),
        nid_ses: "test_ses".to_string(),
    };
    let client = ChzzkClient::new(&config_with_cookies);
    assert_eq!(
        client.cookie_header(),
        Some("NID_AUT=test_aut; NID_SES=test_ses")
    );

    let config_without_cookies = ChzzkConfig {
        nid_aut: "".to_string(),
        nid_ses: "".to_string(),
    };
    let client_no_auth = ChzzkClient::new(&config_without_cookies);
    assert_eq!(client_no_auth.cookie_header(), None);
}
