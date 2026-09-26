use chzzk_load::chzzk::client::{ChzzkClient, extract_best_hls_url};
use chzzk_load::chzzk::models::{ChzzkResponse, LiveDetailContent};
use chzzk_load::config::ChzzkConfig;

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

#[tokio::test]
async fn test_get_live_detail_api_error_envelope() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{"code": 404, "message": "Channel not found", "content": null}"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let result = client.get_live_detail("test_chan").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("404"),
        "error message should contain code 404: {err_msg}"
    );
    assert!(
        err_msg.contains("Channel not found"),
        "error message should contain API message: {err_msg}"
    );
}

#[tokio::test]
async fn test_get_live_detail_success() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveTitle": "Test Live",
                    "channel": {
                        "channelId": "chan123",
                        "channelName": "Streamer123"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}"
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let stream_info = client
        .get_live_detail("chan123")
        .await
        .unwrap()
        .expect("stream info");
    assert_eq!(stream_info.streamer_name, "Streamer123");
    assert_eq!(stream_info.title, "Test Live");
    assert_eq!(stream_info.hls_url, "https://test.com/hls.m3u8");
}

#[test]
fn test_parse_real_world_live_playback_without_track_path() {
    let mock_json = r#"{
        "media": [
            {
                "mediaId": "HLS",
                "path": "https://live.chzzk.naver.com/hls/master.m3u8",
                "encodingTrack": [
                    {
                        "encodingTrackId": "1080p",
                        "videoBitRate": 8000000
                    },
                    {
                        "encodingTrackId": "720p",
                        "videoBitRate": 3000000
                    },
                    {
                        "encodingTrackId": "audioOnly",
                        "path": "https://live.chzzk.naver.com/hls/audioOnly.m3u8"
                    }
                ]
            }
        ]
    }"#;

    let hls_url =
        extract_best_hls_url(&Some(mock_json.to_string())).expect("should parse successfully");
    assert_eq!(hls_url, "https://live.chzzk.naver.com/hls/master.m3u8");
}

#[test]
fn test_parse_live_detail_with_numeric_and_string_live_id() {
    let json_num = r#"{
        "code": 200,
        "content": {
            "liveId": 21212268,
            "status": "OPEN",
            "channel": { "channelId": "c1", "channelName": "N" },
            "livePlaybackJson": null
        }
    }"#;
    let resp_num: ChzzkResponse<LiveDetailContent> = serde_json::from_str(json_num).unwrap();
    assert_eq!(resp_num.content.unwrap().live_id, Some(21212268));

    let json_str = r#"{
        "code": 200,
        "content": {
            "liveId": "21212268",
            "status": "OPEN",
            "channel": { "channelId": "c1", "channelName": "N" },
            "livePlaybackJson": null
        }
    }"#;
    let resp_str: ChzzkResponse<LiveDetailContent> = serde_json::from_str(json_str).unwrap();
    assert_eq!(resp_str.content.unwrap().live_id, Some(21212268));

    let json_null = r#"{
        "code": 200,
        "content": {
            "liveId": null,
            "status": "OPEN",
            "channel": { "channelId": "c1", "channelName": "N" },
            "livePlaybackJson": null
        }
    }"#;
    let resp_null: ChzzkResponse<LiveDetailContent> = serde_json::from_str(json_null).unwrap();
    assert_eq!(resp_null.content.unwrap().live_id, None);
}

#[tokio::test]
async fn test_get_chat_access_token_success() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let base_url = format!("http://127.0.0.1:{port}");

    let server_handle = tokio::task::spawn_blocking(move || {
        let req = server.recv().unwrap();
        assert!(req.url().contains("/v1/chats/access-token"));
        assert!(req.url().contains("channelId=chat_chan_123"));
        let response_body = r#"{
            "code": 200,
            "message": null,
            "content": {
                "accessToken": "mock_token_abc123",
                "extraToken": "mock_extra"
            }
        }"#;
        let resp = tiny_http::Response::from_string(response_body).with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        );
        req.respond(resp).unwrap();
    });

    let config = ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_game_base_url(&base_url);
    let token = client.get_chat_access_token("chat_chan_123").await.unwrap();
    assert_eq!(token, "mock_token_abc123");

    server_handle.await.unwrap();
}

#[test]
fn test_parse_live_detail_with_chat_channel_id() {
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
            "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://live.chzzk.naver.com/hls/master.m3u8\"}]}",
            "chatChannelId": "chat_chan_xyz789"
        }
    }"#;

    let response: ChzzkResponse<LiveDetailContent> = serde_json::from_str(mock_json).unwrap();
    let content = response.content.expect("content should be present");
    assert_eq!(
        content.chat_channel_id,
        Some("chat_chan_xyz789".to_string())
    );
}

#[tokio::test]
async fn test_get_live_detail_with_chat_channel_id() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveTitle": "Test Live",
                    "channel": {
                        "channelId": "chan123",
                        "channelName": "Streamer123"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                    "chatChannelId": "chat_999"
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_base_url(format!("http://127.0.0.1:{port}"));

    let stream_info = client
        .get_live_detail("chan123")
        .await
        .unwrap()
        .expect("stream info");
    assert_eq!(stream_info.chat_channel_id, Some("chat_999".to_string()));
}

#[tokio::test]
async fn test_get_chat_access_token_api_error() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let base_url = format!("http://127.0.0.1:{port}");

    let server_handle = tokio::task::spawn_blocking(move || {
        let req = server.recv().unwrap();
        let response_body = r#"{
            "code": 403,
            "message": "Access denied",
            "content": null
        }"#;
        let resp = tiny_http::Response::from_string(response_body).with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        );
        req.respond(resp).unwrap();
    });

    let config = ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_game_base_url(&base_url);
    let result = client.get_chat_access_token("chat_chan_123").await;
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(err_str.contains("403") || err_str.contains("Access denied"));

    server_handle.await.unwrap();
}

#[test]
fn test_recorded_chat_message_serde() {
    use chzzk_load::chzzk::models_chat::RecordedChatMessage;

    let msg = RecordedChatMessage {
        time_ms: 1727268158000,
        datetime: "2026-09-25 21:42:38".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "Viewer123".to_string(),
        user_id_hash: Some("hash123".to_string()),
        content: "Hello world!".to_string(),
        donation_amount: Some(1000),
        extras: Some(serde_json::json!({"emojis": {}})),
        raw: serde_json::json!({"cmd": 93101}),
    };

    let serialized = serde_json::to_string(&msg).unwrap();
    let deserialized: RecordedChatMessage = serde_json::from_str(&serialized).unwrap();
    assert_eq!(msg, deserialized);
}

#[test]
fn test_extract_best_hls_url_from_p2p_path_cdn_url() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    let raw_1080_url =
        "https://nvelop-livecloud.pstatic.net/chzzk/1080p_playlist.m3u8?hdnts=token1080";
    let raw_720_url =
        "https://nvelop-livecloud.pstatic.net/chzzk/720p_playlist.m3u8?hdnts=token720";
    let b64_1080 = STANDARD.encode(raw_1080_url);
    let b64_720 = STANDARD.encode(raw_720_url);

    let p2p_1080 =
        format!("/chzzk/live_1080p.m3u8?channel_id=123_1080p&cdn_url={b64_1080}&timemachine=false");
    let p2p_720 =
        format!("/chzzk/live_720p.m3u8?channel_id=123_720p&cdn_url={b64_720}&timemachine=false");

    let json = format!(
        r#"{{"media":[{{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[
            {{"encodingTrackId":"720p","path":null,"p2pPath":"{p2p_720}"}},
            {{"encodingTrackId":"1080p","path":null,"p2pPath":"{p2p_1080}"}},
            {{"encodingTrackId":"audioOnly","path":"https://live.chzzk.naver.com/audio.m3u8"}}
        ]}}]}}"#
    );

    let hls_url = extract_best_hls_url(&Some(json)).unwrap();
    assert_eq!(hls_url, raw_1080_url);
}

#[test]
fn test_extract_best_hls_url_from_p2p_path_url_encoding() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    let raw_1080_url =
        "https://nvelop-livecloud.pstatic.net/chzzk/1080p_playlist.m3u8?hdnts=token1080";
    let b64_1080 = STANDARD.encode(raw_1080_url);
    let p2p_enc = format!(
        "/chzzk/live.m3u8?channel_id%3D123%26cdn_url%3D{}%26timemachine%3Dfalse",
        b64_1080.replace('=', "%3D")
    );

    let json = format!(
        r#"{{"media":[{{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[
            {{"encodingTrackId":"1080p","path":null,"p2pPathUrlEncoding":"{p2p_enc}"}}
        ]}}]}}"#
    );

    let hls_url = extract_best_hls_url(&Some(json)).unwrap();
    assert_eq!(hls_url, raw_1080_url);
}

#[test]
fn test_extract_best_hls_url_prefers_p2p_720p_when_1080p_absent() {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    let raw_720_url =
        "https://nvelop-livecloud.pstatic.net/chzzk/720p_playlist.m3u8?hdnts=token720";
    let b64_720 = STANDARD.encode(raw_720_url);
    let p2p_720 =
        format!("/chzzk/live_720p.m3u8?channel_id=123_720p&cdn_url={b64_720}&timemachine=false");

    let json = format!(
        r#"{{"media":[{{"mediaId":"HLS","path":"https://live.chzzk.naver.com/master.m3u8","encodingTrack":[
            {{"encodingTrackId":"480p","path":null}},
            {{"encodingTrackId":"720p","path":null,"p2pPath":"{p2p_720}"}}
        ]}}]}}"#
    );

    let hls_url = extract_best_hls_url(&Some(json)).unwrap();
    assert_eq!(hls_url, raw_720_url);
}
