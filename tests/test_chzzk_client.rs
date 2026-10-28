use chzzk_load::chzzk::client::{ChzzkClient, extract_best_hls_url};
use chzzk_load::chzzk::models::{ChzzkResponse, LiveDetail, LiveDetailContent};
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

    let detail = client.get_live_detail("chan123").await.unwrap();
    match detail {
        LiveDetail::Open(stream_info) => {
            assert_eq!(stream_info.streamer_name, "Streamer123");
            assert_eq!(stream_info.title, "Test Live");
            assert_eq!(stream_info.hls_url, "https://test.com/hls.m3u8");
        }
        other => panic!("Expected LiveDetail::Open, got {other:?}"),
    }
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

    let detail = client.get_live_detail("chan123").await.unwrap();
    match detail {
        LiveDetail::Open(stream_info) => {
            assert_eq!(stream_info.chat_channel_id, Some("chat_999".to_string()));
        }
        other => panic!("Expected LiveDetail::Open, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_restricted_when_playback_json_null() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 9999,
                    "status": "OPEN",
                    "liveTitle": "[19+] Adult Restricted Stream",
                    "channel": {
                        "channelId": "chan_restricted",
                        "channelName": "RestrictedStreamer"
                    },
                    "livePlaybackJson": null,
                    "adult": true,
                    "chatChannelId": "chat_restricted_123"
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

    let detail = client.get_live_detail("chan_restricted").await.unwrap();
    match detail {
        LiveDetail::Restricted {
            channel_id,
            live_id,
            streamer_name,
            title,
            chat_channel_id,
            adult,
        } => {
            assert_eq!(channel_id, "chan_restricted");
            assert_eq!(live_id, Some(9999));
            assert_eq!(streamer_name, "RestrictedStreamer");
            assert_eq!(title, "[19+] Adult Restricted Stream");
            assert_eq!(chat_channel_id, Some("chat_restricted_123".to_string()));
            assert!(adult);
        }
        other => panic!("Expected LiveDetail::Restricted, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_restricted_when_paid_stream_and_aes_key_returns_403() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let url = request.url().to_string();
            if url.contains("live-detail") {
                let mock_body = r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21326414,
                        "status": "OPEN",
                        "liveTitle": "Sports Broadcast (Requires Cheat Key)",
                        "channel": {
                            "channelId": "chan_sports",
                            "channelName": "SportsStreamer"
                        },
                        "livePlaybackJson": "{\"meta\":{\"paidLive\":true,\"playbackAuthType\":\"CHZZK_CHEAT_KEY\"},\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                        "adult": false,
                        "chatChannelId": "chat_sports_123"
                    }
                }"#;
                let response = tiny_http::Response::from_string(mock_body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = request.respond(response);
            } else if url.contains("/service/v1/encryption/lives/21326414/aes_key") {
                let mock_body = r#"{"code":403,"message":"접근이 거부 되었습니다."}"#;
                let response = tiny_http::Response::from_string(mock_body)
                    .with_status_code(403)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/json"[..],
                        )
                        .unwrap(),
                    );
                let _ = request.respond(response);
            } else {
                let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
                let _ = request.respond(response);
            }
        }
    });

    let config = chzzk_load::config::ChzzkConfig {
        nid_aut: "invalid_aut".to_string(),
        nid_ses: "invalid_ses".to_string(),
    };
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let detail = client.get_live_detail("chan_sports").await.unwrap();
    match detail {
        LiveDetail::Restricted {
            channel_id,
            live_id,
            streamer_name,
            title,
            chat_channel_id,
            adult,
        } => {
            assert_eq!(channel_id, "chan_sports");
            assert_eq!(live_id, Some(21326414));
            assert_eq!(streamer_name, "SportsStreamer");
            assert_eq!(title, "Sports Broadcast (Requires Cheat Key)");
            assert_eq!(chat_channel_id, Some("chat_sports_123".to_string()));
            assert!(!adult, "Sports broadcast is not an adult stream");
        }
        other => panic!("Expected LiveDetail::Restricted, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_open_when_paid_stream_and_aes_key_returns_200() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let url = request.url().to_string();
            if url.contains("live-detail") {
                let mock_body = r#"{
                    "code": 200,
                    "message": null,
                    "content": {
                        "liveId": 21326414,
                        "status": "OPEN",
                        "liveTitle": "Sports Broadcast (Subscribed)",
                        "channel": {
                            "channelId": "chan_sports_sub",
                            "channelName": "SportsStreamer"
                        },
                        "livePlaybackJson": "{\"meta\":{\"paidLive\":true,\"playbackAuthType\":\"CHZZK_CHEAT_KEY\"},\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                        "adult": false,
                        "chatChannelId": "chat_sports_123"
                    }
                }"#;
                let response = tiny_http::Response::from_string(mock_body).with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
                let _ = request.respond(response);
            } else if url.contains("/service/v1/encryption/lives/21326414/aes_key") {
                let mock_key = vec![0u8; 16];
                let response = tiny_http::Response::from_data(mock_key)
                    .with_status_code(200)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"application/octet-stream"[..],
                        )
                        .unwrap(),
                    );
                let _ = request.respond(response);
            } else {
                let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
                let _ = request.respond(response);
            }
        }
    });

    let config = chzzk_load::config::ChzzkConfig {
        nid_aut: "valid_aut".to_string(),
        nid_ses: "valid_ses".to_string(),
    };
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let detail = client.get_live_detail("chan_sports_sub").await.unwrap();
    match detail {
        LiveDetail::Open(stream_info) => {
            assert_eq!(stream_info.channel_id, "chan_sports_sub");
            assert_eq!(stream_info.live_id, Some(21326414));
            assert_eq!(stream_info.streamer_name, "SportsStreamer");
            assert_eq!(stream_info.hls_url, "https://test.com/hls.m3u8");
        }
        other => panic!("Expected LiveDetail::Open, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_restricted_when_playback_auth_type_requires_auth_and_no_cookies() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 21326414,
                    "status": "OPEN",
                    "liveTitle": "Membership Exclusive Live",
                    "channel": {
                        "channelId": "chan_membership",
                        "channelName": "MemberStreamer"
                    },
                    "livePlaybackJson": "{\"meta\":{\"paidLive\":false,\"playbackAuthType\":\"MEMBERSHIP\"},\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                    "adult": false,
                    "chatChannelId": null
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default(); // no cookies
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let detail = client.get_live_detail("chan_membership").await.unwrap();
    match detail {
        LiveDetail::Restricted {
            channel_id,
            live_id,
            adult,
            ..
        } => {
            assert_eq!(channel_id, "chan_membership");
            assert_eq!(live_id, Some(21326414));
            assert!(!adult);
        }
        other => panic!("Expected LiveDetail::Restricted, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_restricted_when_playable_status_not_playable() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": 21326414,
                    "status": "OPEN",
                    "liveTitle": "Restricted Broadcast",
                    "channel": {
                        "channelId": "chan_status_restr",
                        "channelName": "RestrictedStreamer"
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                    "livePollingStatusJson": "{\"status\":\"STARTED\",\"isPublishing\":true,\"playableStatus\":\"RESTRICTED\"}",
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

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = chzzk_load::chzzk::client::ChzzkClient::new(&config)
        .with_base_url(format!("http://127.0.0.1:{port}"));

    let detail = client.get_live_detail("chan_status_restr").await.unwrap();
    match detail {
        LiveDetail::Restricted {
            channel_id, adult, ..
        } => {
            assert_eq!(channel_id, "chan_status_restr");
            assert!(!adult);
        }
        other => panic!("Expected LiveDetail::Restricted, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_live_detail_close_when_offline() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "liveId": null,
                    "status": "CLOSE",
                    "liveTitle": null,
                    "channel": {
                        "channelId": "chan_offline",
                        "channelName": "OfflineStreamer"
                    },
                    "livePlaybackJson": null
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

    let detail = client.get_live_detail("chan_offline").await.unwrap();
    assert_eq!(
        detail,
        LiveDetail::Close {
            streamer_name: Some("OfflineStreamer".to_string())
        }
    );
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
        player_message_time: None,
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

#[test]
fn test_resolve_access_tier_precedence() {
    use chzzk_load::chzzk::client::resolve_access_tier;
    use chzzk_load::chzzk::models::{ChannelInfo, LiveDetailContent, PlaybackMeta};
    use chzzk_load::chzzk::models_metadata::StreamAccessTier;

    let base_content = LiveDetailContent {
        live_id: Some(123),
        status: "OPEN".to_string(),
        live_title: Some("Title".to_string()),
        channel: ChannelInfo {
            channel_id: "c1".to_string(),
            channel_name: "Name".to_string(),
            channel_image_url: None,
            verified_mark: Some(true),
        },
        live_playback_json: None,
        chat_channel_id: None,
        adult: Some(false),
        open_date: None,
        close_date: None,
        category_type: None,
        live_category: None,
        live_category_value: None,
        tags: vec![],
        paid_promotion: None,
        drops_campaign_no: None,
        kr_only_viewing: None,
        clip_active: None,
        time_machine_active: None,
        chat_active: None,
        chat_available_group: None,
        chat_available_condition: None,
        min_follower_minute: None,
        allow_subscriber_in_follower_mode: None,
        chat_slow_mode_sec: None,
        chat_emoji_mode: None,
        chat_donation_ranking_exposure: None,
        live_image_url: None,
        default_thumbnail_image_url: None,
        concurrent_user_count: None,
        accumulate_count: None,
        watch_party_no: None,
        watch_party_tag: None,
        watch_party_type: None,
        watch_party_paid_product_id: None,
        paid_product: None,
        live_polling_status_json: None,
        user_adult_status: None,
        membership_benefit_type: None,
        tv_app_viewing_policy_type: None,
        blind_type: None,
        log_power_active: None,
    };

    // 1. Default Public
    assert_eq!(
        resolve_access_tier(&base_content, None),
        StreamAccessTier::Public
    );

    // 2. AdultOnly
    let mut adult_content = base_content.clone();
    adult_content.adult = Some(true);
    assert_eq!(
        resolve_access_tier(&adult_content, None),
        StreamAccessTier::AdultOnly
    );

    // 3. CheatKey beats AdultOnly
    let cheat_meta = PlaybackMeta {
        video_id: None,
        stream_seq: None,
        live_id: None,
        paid_live: None,
        playback_auth_type: Some("CHZZK_CHEAT_KEY".to_string()),
    };
    assert_eq!(
        resolve_access_tier(&adult_content, Some(&cheat_meta)),
        StreamAccessTier::CheatKey
    );

    // 4. NaverPlus beats CheatKey
    let mut plus_content = adult_content.clone();
    plus_content.membership_benefit_type = Some("NAVER_PLUS".to_string());
    assert_eq!(
        resolve_access_tier(&plus_content, Some(&cheat_meta)),
        StreamAccessTier::NaverPlus
    );

    // 5. ChannelSubscription beats NaverPlus
    let mut sub_content = plus_content.clone();
    sub_content.membership_benefit_type = Some("MEMBER_ONLY".to_string());
    assert_eq!(
        resolve_access_tier(&sub_content, Some(&cheat_meta)),
        StreamAccessTier::ChannelSubscription
    );

    // 6. PayPerView beats ChannelSubscription
    let mut ppv_content = sub_content.clone();
    ppv_content.paid_product = Some(serde_json::json!({"sku": "ticket_1"}));
    assert_eq!(
        resolve_access_tier(&ppv_content, Some(&cheat_meta)),
        StreamAccessTier::PayPerView
    );
}

#[tokio::test]
async fn test_get_live_detail_lean_metadata_mapping_and_omits_telemetry() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "status": "OPEN",
                    "liveId": 888123,
                    "liveTitle": "Chzzk Championship Finals",
                    "channel": {
                        "channelId": "chan_lean",
                        "channelName": "EsportsBroadcaster",
                        "channelImageUrl": "https://ssl.pstatic.net/avatar.png",
                        "verifiedMark": true
                    },
                    "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://test.com/hls.m3u8\"}]}",
                    "chatChannelId": "chat_chan_lean",
                    "adult": false,
                    "openDate": "2026-10-03 12:00:00",
                    "closeDate": null,
                    "categoryType": "GAME",
                    "liveCategory": "game",
                    "liveCategoryValue": "League of Legends",
                    "tags": ["esports", "finals"],
                    "paidPromotion": true,
                    "dropsCampaignNo": "camp_888",
                    "krOnlyViewing": true,
                    "chatActive": false,
                    "watchPartyNo": 777,
                    "watchPartyTag": "watch_party_final",
                    "concurrentUserCount": 45000,
                    "accumulateCount": 120000,
                    "liveImageUrl": "https://ssl.pstatic.net/thumb.jpg",
                    "defaultThumbnailImageUrl": "https://ssl.pstatic.net/def.jpg",
                    "logPowerActive": true
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

    let detail = client.get_live_detail("chan_lean").await.unwrap();
    match detail {
        LiveDetail::Open(stream_info) => {
            assert_eq!(stream_info.channel_id, "chan_lean");
            assert_eq!(stream_info.streamer_name, "EsportsBroadcaster");
            assert_eq!(stream_info.title, "Chzzk Championship Finals");
            assert_eq!(stream_info.live_id, Some(888123));

            let meta = stream_info.metadata;
            // Preserved core identifiers & lifecycles
            assert_eq!(meta.live_id, Some(888123));
            assert_eq!(meta.channel_id, "chan_lean");
            assert_eq!(meta.channel_name, "EsportsBroadcaster");
            assert_eq!(meta.live_title, "Chzzk Championship Finals");
            assert_eq!(meta.open_date, Some("2026-10-03 12:00:00".to_string()));
            assert_eq!(meta.close_date, None);

            // Classification & tags
            assert_eq!(
                meta.category_type,
                Some(chzzk_load::chzzk::models_metadata::CategoryType::Game)
            );
            assert_eq!(meta.live_category, Some("game".to_string()));
            assert_eq!(
                meta.live_category_value,
                Some("League of Legends".to_string())
            );
            assert_eq!(meta.tags, vec!["esports".to_string(), "finals".to_string()]);
            assert_eq!(
                meta.access_tier,
                chzzk_load::chzzk::models_metadata::StreamAccessTier::Public
            );

            // Flattened flags
            assert!(meta.is_kr_only);
            assert!(!meta.is_chat_active);
            assert!(meta.is_watch_party);
            assert!(meta.paid_promotion);
            assert_eq!(meta.drops_campaign_no, Some("camp_888".to_string()));
        }
        other => panic!("Expected LiveDetail::Open, got {other:?}"),
    }
}

#[test]
fn test_chzzk_client_formats_auth_config_cookies() {
    use chzzk_load::config::AuthConfig;

    let auth = AuthConfig::new("aut_token_123", "ses_token_456");
    let client = ChzzkClient::new_with_auth(&auth);
    assert_eq!(
        client.cookie_header(),
        Some("NID_AUT=aut_token_123; NID_SES=ses_token_456")
    );
}

#[tokio::test]
async fn test_chzzk_client_passes_cookie_to_chzzk_vod_detail() {
    use chzzk_load::config::AuthConfig;
    use std::sync::mpsc;

    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let cookie_header = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Cookie"))
                .map(|h| h.value.as_str().to_string());
            let _ = tx.send(cookie_header);

            let mock_body = r#"{
                "code": 200,
                "message": null,
                "content": {
                    "videoNo": 4531238,
                    "videoId": "neon_vid_999",
                    "videoTitle": "Authenticated VOD",
                    "channel": {
                        "channelId": "chan_01",
                        "channelName": "Streamer01"
                    }
                }
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let auth = AuthConfig::new("sample_nid_aut", "sample_nid_ses");
    let client =
        ChzzkClient::new_with_auth(&auth).with_base_url(format!("http://127.0.0.1:{port}"));

    let detail = client
        .get_video_detail(4531238)
        .await
        .expect("VOD detail should succeed");
    assert_eq!(detail.video_no, 4531238);

    let received_cookie = rx.recv().expect("Mock server should have received request");
    assert_eq!(
        received_cookie.as_deref(),
        Some("NID_AUT=sample_nid_aut; NID_SES=sample_nid_ses")
    );
}

#[tokio::test]
async fn test_chzzk_client_passes_cookie_to_neonplayer_playback() {
    use chzzk_load::config::AuthConfig;
    use std::sync::mpsc;

    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let cookie_header = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Cookie"))
                .map(|h| h.value.as_str().to_string());
            let _ = tx.send(cookie_header);

            let mock_body = r#"{
                "media": [
                    {
                        "mediaId": "HLS",
                        "protocol": "HLS",
                        "path": "https://cdn.example.com/vod/master.m3u8"
                    }
                ]
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let auth = AuthConfig::new("neon_aut_123", "neon_ses_456");
    let client = ChzzkClient::new_with_auth(&auth)
        .with_playback_base_url(format!("http://127.0.0.1:{port}"));

    let playback = client
        .get_playback_info("neon_vid_999", "secret_key_888")
        .await
        .expect("Playback info should succeed");
    assert_eq!(
        playback.find_hls_master_url(),
        Some("https://cdn.example.com/vod/master.m3u8")
    );

    let received_cookie = rx.recv().expect("Mock server should have received request");
    assert_eq!(
        received_cookie.as_deref(),
        Some("NID_AUT=neon_aut_123; NID_SES=neon_ses_456")
    );
}

#[tokio::test]
async fn test_get_playback_info_neonplayer_mpd_period_structure() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "period": [
                    {
                        "adaptationSet": [
                            {
                                "mimeType": "video/mp2t",
                                "otherAttributes": {
                                    "m3u": "https://cdn.example.com/vod/master.m3u8?token=xyz"
                                },
                                "representation": [
                                    {
                                        "id": "rep_1080",
                                        "width": 1920,
                                        "height": 1080
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client =
        ChzzkClient::new(&config).with_playback_base_url(format!("http://127.0.0.1:{port}"));

    let playback = client
        .get_playback_info("neon_vid_dash", "key_dash")
        .await
        .expect("Playback info with MPD structure should succeed");
    assert_eq!(
        playback.find_hls_master_url(),
        Some("https://cdn.example.com/vod/master.m3u8?token=xyz")
    );
}

#[tokio::test]
async fn test_get_video_detail_adult_4001_diagnostic_error() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 4001,
                "message": "Adult certification is required",
                "content": null
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_base_url(format!("http://127.0.0.1:{port}"));

    let err = client
        .get_video_detail(99901)
        .await
        .expect_err("Adult 4001 video must return error");
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("4001") && err_msg.contains("19+ age-restricted"),
        "Error message must specify code 4001 and age restriction: {err_msg}"
    );
    assert!(
        err_msg.contains("nid_aut") && err_msg.contains("nid_ses"),
        "Error message must guide user to provide nid_aut and nid_ses: {err_msg}"
    );
}

#[tokio::test]
async fn test_get_video_detail_subscriber_4003_diagnostic_error() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 4003,
                "message": "Channel subscription required",
                "content": null
            }"#;
            let response = tiny_http::Response::from_string(mock_body).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_base_url(format!("http://127.0.0.1:{port}"));

    let err = client
        .get_video_detail(99902)
        .await
        .expect_err("Subscriber 4003 video must return error");
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("4003") && err_msg.contains("subscriber-only"),
        "Error message must specify code 4003 and subscription restriction: {err_msg}"
    );
    assert!(
        err_msg.contains("nid_aut") && err_msg.contains("nid_ses"),
        "Error message must guide user to provide credentials: {err_msg}"
    );
}

#[tokio::test]
async fn test_get_video_detail_http_403_with_4001_envelope() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            let mock_body = r#"{
                "code": 4001,
                "message": "Adult verification required",
                "content": null
            }"#;
            let response = tiny_http::Response::from_string(mock_body)
                .with_status_code(403)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_base_url(format!("http://127.0.0.1:{port}"));

    let err = client
        .get_video_detail(99903)
        .await
        .expect_err("HTTP 403 with code 4001 must return error");
    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("4001") && err_msg.contains("19+ age-restricted"),
        "Error message should identify 4001 age restriction: {err_msg}"
    );
    assert!(
        err_msg.contains("nid_aut") && err_msg.contains("nid_ses"),
        "Error message should instruct user on credentials: {err_msg}"
    );
}

#[tokio::test]
async fn test_get_playback_info_restricted_http_403_and_codes() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    std::thread::spawn(move || {
        // Request 1: 403 with code 4001
        if let Ok(request) = server.recv() {
            let mock_body = r#"{"code": 4001, "message": "Restricted"}"#;
            let response = tiny_http::Response::from_string(mock_body)
                .with_status_code(403)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
            let _ = request.respond(response);
        }
        // Request 2: 403 plain without json
        if let Ok(request) = server.recv() {
            let response = tiny_http::Response::from_string("Access Denied").with_status_code(403);
            let _ = request.respond(response);
        }
    });

    let config = chzzk_load::config::ChzzkConfig::default();
    let client =
        ChzzkClient::new(&config).with_playback_base_url(format!("http://127.0.0.1:{port}"));

    let err1 = client
        .get_playback_info("neon_vid_1", "key1")
        .await
        .expect_err("Playback 403 with code 4001 should fail");
    let err1_msg = format!("{err1:#}");
    assert!(
        err1_msg.contains("4001") && err1_msg.contains("19+ age-restricted"),
        "Playback error should diagnose 4001: {err1_msg}"
    );
    assert!(
        err1_msg.contains("nid_aut") && err1_msg.contains("nid_ses"),
        "Playback error should mention credentials: {err1_msg}"
    );

    let err2 = client
        .get_playback_info("neon_vid_2", "key2")
        .await
        .expect_err("Playback 403 plain should fail");
    let err2_msg = format!("{err2:#}");
    assert!(
        err2_msg.contains("403") && err2_msg.contains("nid_aut"),
        "Playback error should diagnose 403 and mention credentials: {err2_msg}"
    );
}

#[test]
fn test_vod_find_progressive_mp4_quality_best_and_worst() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "PD_1080P",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 8000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/1080p.mp4?token=123" }
                                ]
                            },
                            {
                                "id": "PD_720P",
                                "width": 1280,
                                "height": 720,
                                "bandwidth": 4000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/720p.mp4?token=123" }
                                ]
                            },
                            {
                                "id": "PD_480P",
                                "width": 854,
                                "height": 480,
                                "bandwidth": 2000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/480p.mp4?token=123" }
                                ]
                            },
                            {
                                "id": "PD_360P",
                                "width": 640,
                                "height": 360,
                                "bandwidth": 1000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/360p.mp4?token=123" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("best"),
        Some("https://cdn.example.com/vod/1080p.mp4?token=123")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("worst"),
        Some("https://cdn.example.com/vod/360p.mp4?token=123")
    );
    // Legacy helper delegates to best
    assert_eq!(
        info.find_progressive_mp4_url(),
        Some("https://cdn.example.com/vod/1080p.mp4?token=123")
    );
}

#[test]
fn test_vod_find_progressive_mp4_exact_and_closest_match() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "PD_1080P",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 8000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/1080p.mp4?token=123" }
                                ]
                            },
                            {
                                "id": "PD_720P",
                                "width": 1280,
                                "height": 720,
                                "bandwidth": 4000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/720p.mp4?token=123" }
                                ]
                            },
                            {
                                "id": "PD_480P",
                                "width": 854,
                                "height": 480,
                                "bandwidth": 2000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/480p.mp4?token=123" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();

    // Exact matches
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("1080p"),
        Some("https://cdn.example.com/vod/1080p.mp4?token=123")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("720p"),
        Some("https://cdn.example.com/vod/720p.mp4?token=123")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("720"),
        Some("https://cdn.example.com/vod/720p.mp4?token=123")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("480p"),
        Some("https://cdn.example.com/vod/480p.mp4?token=123")
    );

    // Exact matching only: unmatched resolutions return None to enable HLS fallback
    assert_eq!(info.find_progressive_mp4_url_with_quality("900p"), None);
    assert_eq!(info.find_progressive_mp4_url_with_quality("600p"), None);

    // Unreachable lower resolution target returns None
    assert_eq!(info.find_progressive_mp4_url_with_quality("360p"), None);
    assert_eq!(info.find_progressive_mp4_url_with_quality("240p"), None);

    // Unrecognized quality returns None
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("unrecognized_quality"),
        None
    );
}

#[test]
fn test_vod_find_progressive_mp4_returns_none_when_requested_resolution_absent() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "PD_720P",
                                "bandwidth": 4000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/720p.mp4" }
                                ],
                                "height": 720
                            },
                            {
                                "id": "PD_480P",
                                "bandwidth": 2000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/480p.mp4" }
                                ],
                                "height": 480
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();

    // 1080p requested when only 720p/480p present -> must return None so pipeline falls back to HLS
    assert_eq!(info.find_progressive_mp4_url_with_quality("1080p"), None);
    assert_eq!(info.find_progressive_mp4_url_with_quality("1080"), None);

    // Exact matches succeed
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("720p"),
        Some("https://cdn.example.com/vod/720p.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("480p"),
        Some("https://cdn.example.com/vod/480p.mp4")
    );
}

#[test]
fn test_vod_find_progressive_mp4_highest_bandwidth_tiebreaker() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "PD_1080P_30FPS",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 5000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/1080p30.mp4" }
                                ]
                            },
                            {
                                "id": "PD_1080P_60FPS",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 9000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/1080p60.mp4" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("1080p"),
        Some("https://cdn.example.com/vod/1080p60.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("best"),
        Some("https://cdn.example.com/vod/1080p60.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("worst"),
        Some("https://cdn.example.com/vod/1080p30.mp4")
    );
}

#[test]
fn test_vod_find_progressive_mp4_adaptation_set_dimensions_inheritance() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "width": 1280,
                        "height": 720,
                        "bandwidth": 3500000,
                        "representation": [
                            {
                                "id": "rep_inherited_720",
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/inherited_720p.mp4" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("720p"),
        Some("https://cdn.example.com/vod/inherited_720p.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("best"),
        Some("https://cdn.example.com/vod/inherited_720p.mp4")
    );
}

#[test]
fn test_vod_find_progressive_mp4_segregated_audio_returns_none() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    // Period contains both a video/mp4 adaptation set and an audio/mp4 adaptation set
    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "PD_1080P",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 8000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/video_only_1080p.mp4" }
                                ]
                            }
                        ]
                    },
                    {
                        "mimeType": "audio/mp4",
                        "representation": [
                            {
                                "id": "audio_track_1",
                                "bandwidth": 192000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/audio.m4a" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    // Segregated audio streams require multi-track demuxing; progressive single-file discovery returns None
    assert_eq!(info.find_progressive_mp4_url_with_quality("best"), None);
    assert_eq!(info.find_progressive_mp4_url_with_quality("1080p"), None);
    assert_eq!(info.find_progressive_mp4_url(), None);
}

#[test]
fn test_vod_find_progressive_mp4_segregated_audio_in_representation_returns_none() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "video_1080p",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 8000000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/video_1080p.mp4" }
                                ]
                            }
                        ]
                    },
                    {
                        "representation": [
                            {
                                "id": "audio_rep",
                                "mimeType": "audio/aac",
                                "bandwidth": 128000,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/audio.aac" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(info.find_progressive_mp4_url_with_quality("best"), None);
}

#[test]
fn test_vod_find_progressive_mp4_muxed_codecs_with_audio_adaptation_set_succeeds() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    // Real Chzzk NeonPlayer response: period contains video/mp4 with muxed codecs
    // (codecs: "avc1.64002a,mp4a.40.2") AND an audio/mp4 adaptation set for DASH.
    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp4",
                        "representation": [
                            {
                                "id": "1080p",
                                "width": 1920,
                                "height": 1080,
                                "bandwidth": 8000000,
                                "codecs": "avc1.64002a,mp4a.40.2",
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/muxed_1080p.mp4" }
                                ]
                            },
                            {
                                "id": "720p",
                                "width": 1280,
                                "height": 720,
                                "bandwidth": 4000000,
                                "codecs": "avc1.4d401f,mp4a.40.2",
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/muxed_720p.mp4" }
                                ]
                            }
                        ]
                    },
                    {
                        "mimeType": "audio/mp4",
                        "representation": [
                            {
                                "id": "audio_track_1",
                                "bandwidth": 192000,
                                "codecs": "mp4a.40.2",
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/audio.m4a" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("best"),
        Some("https://cdn.example.com/vod/muxed_1080p.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url_with_quality("720p"),
        Some("https://cdn.example.com/vod/muxed_720p.mp4")
    );
    assert_eq!(
        info.find_progressive_mp4_url(),
        Some("https://cdn.example.com/vod/muxed_1080p.mp4")
    );
}

#[test]
fn test_vod_find_progressive_mp4_missing_or_hls_only_returns_none() {
    use chzzk_load::chzzk::models_vod::VodPlaybackInfo;

    let json = r#"{
        "period": [
            {
                "adaptationSet": [
                    {
                        "mimeType": "video/mp2t",
                        "otherAttributes": {
                            "m3u": "https://cdn.example.com/vod/master.m3u8"
                        },
                        "representation": [
                            {
                                "id": "rep_1080",
                                "width": 1920,
                                "height": 1080,
                                "baseURL": [
                                    { "value": "https://cdn.example.com/vod/1080p.m3u8" }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }"#;

    let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
    assert_eq!(info.find_progressive_mp4_url_with_quality("best"), None);
    assert_eq!(info.find_progressive_mp4_url_with_quality("1080p"), None);
}
