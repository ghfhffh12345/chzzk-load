mod common;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use chzzk_load::chzzk::models::ChannelInfo;
use chzzk_load::chzzk::models_vod::{
    VodDetail, VodEncodingOption, VodPlaybackInfo, VodPlaybackMedia,
};
use chzzk_load::chzzk::source::FakeVodSource;
use chzzk_load::cli::VodArgs;
use chzzk_load::vod::{VodOptions, run_vod_archival, run_vod_archival_with_options};
use common::mock_aria2c::get_mock_aria2c_bin;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;

const SAMPLE_MASTER_M3U8: &str = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=1920x1080,NAME="1080p"
1080p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1280x720,NAME="720p"
720p/playlist.m3u8
"#;

const SAMPLE_1080P_M3U8: &str = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXTINF:6.000,
chunk_00.ts
#EXTINF:6.000,
chunk_01.ts
#EXTINF:4.500,
chunk_02.ts
#EXT-X-ENDLIST
"#;

const SAMPLE_720P_M3U8: &str = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXTINF:6.000,
chunk_720_00.ts
#EXTINF:6.000,
chunk_720_01.ts
#EXT-X-ENDLIST
"#;

fn create_fake_vod_source(video_no: u64, video_id: &str, in_key: &str) -> FakeVodSource {
    let detail = VodDetail {
        video_no,
        video_id: video_id.to_string(),
        video_title: "Chzzk Live Replay".to_string(),
        in_key: Some(in_key.to_string()),
        live_open_date: Some("2026-09-30 11:00:00".to_string()),
        publish_date: Some("2026-09-30 15:00:00".to_string()),
        duration: Some(7200),
        channel: ChannelInfo {
            channel_id: "chan_gamer_01".to_string(),
            channel_name: "ProGamer".to_string(),
            channel_image_url: None,
            verified_mark: None,
        },
        category_type: Some("GAME".to_string()),
        video_category: None,
        video_category_value: None,
        tags: vec!["game".to_string()],
        paid_promotion: None,
        adult: None,
    };

    let playback_info = VodPlaybackInfo {
        media: vec![VodPlaybackMedia {
            media_id: Some("HLS".to_string()),
            protocol: Some("HLS".to_string()),
            path: "https://cdn.example.com/vod/master.m3u8".to_string(),
            encoding_option: Some(VodEncodingOption {
                width: Some(1920),
                height: Some(1080),
                bitrate: Some(8000000),
                name: Some("1080p".to_string()),
            }),
        }],
    };

    FakeVodSource::new()
        .with_video_detail(video_no, detail)
        .with_playback_info(video_id, in_key, playback_info)
        .with_hls_playlist(
            "https://cdn.example.com/vod/master.m3u8",
            SAMPLE_MASTER_M3U8,
        )
        .with_hls_playlist(
            "https://cdn.example.com/vod/1080p/playlist.m3u8",
            SAMPLE_1080P_M3U8,
        )
        .with_hls_playlist(
            "https://cdn.example.com/vod/720p/playlist.m3u8",
            SAMPLE_720P_M3U8,
        )
}

#[tokio::test]
async fn test_vod_archival_e2e_full_url_success() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir = std::env::temp_dir().join(format!("test_vod_e2e_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 4531238;
    let video_id = "neon_vid_999";
    let in_key = "key_secret_888";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let args = VodArgs {
        target: format!("https://chzzk.naver.com/video/{video_no}?extra=true"),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: false,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source, cancel_token)
        .await
        .expect("run_vod_archival should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.streamer, "ProGamer");
    assert_eq!(summary.title, "Chzzk Live Replay");
    assert_eq!(summary.segments_downloaded, 3);

    // Verify canonical output directory format: [YYYY-MM-DD_HHMMSS] Streamer - Title
    assert_eq!(
        summary.output_dir.file_name().unwrap().to_string_lossy(),
        "[2026-09-30_110000] ProGamer - Chzzk Live Replay"
    );

    // Verify final vod.mp4 exists and is non-empty
    let finalized_video = summary.output_dir.join("vod.mp4");
    assert!(finalized_video.exists(), "vod.mp4 must exist");
    assert!(
        std::fs::metadata(&finalized_video).unwrap().len() > 0,
        "vod.mp4 must not be empty"
    );

    // Verify intermediate .part file was removed upon promotion
    let staged_part = summary.output_dir.join("vod.mp4.part");
    assert!(!staged_part.exists(), "vod.mp4.part must be promoted");

    // Verify vod_metadata.jsonl exists and .part was promoted
    let finalized_meta = summary.output_dir.join("vod_metadata.jsonl");
    assert!(finalized_meta.exists(), "vod_metadata.jsonl must exist");
    assert!(!summary.output_dir.join("vod_metadata.jsonl.part").exists());
    assert_eq!(summary.metadata_path, finalized_meta);

    // Verify staging directory was purged
    let staging_dir = summary.output_dir.join(format!(".vod_staging_{video_id}"));
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged upon success"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_numeric_id_and_quality_selector() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir = std::env::temp_dir().join(format!("test_vod_numeric_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 7771234;
    let video_id = "neon_720_vid";
    let in_key = "key_720";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "720p".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: false,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source, cancel_token)
        .await
        .expect("run_vod_archival with numeric ID and 720p should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.segments_downloaded, 2); // 720p variant has 2 segments

    let finalized_video = summary.output_dir.join("vod.mp4");
    assert!(finalized_video.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_fails_fast_on_missing_aria2c() {
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_missing_aria_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 12345;
    let source = Arc::new(create_fake_vod_source(video_no, "v_id", "in_k"));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: false,
        config: None,
    };

    let options = VodOptions {
        aria2c_bin: Some("/nonexistent/bin/aria2c_missing_binary".to_string()),
        ffmpeg_bin: None,
        chat_options: None,
    };

    let cancel_token = CancellationToken::new();
    let err = run_vod_archival_with_options(args, source, options, cancel_token)
        .await
        .expect_err("Missing aria2c binary must fail fast");

    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("aria2c binary '/nonexistent/bin/aria2c_missing_binary' was not found")
            || err_msg.contains("could not be executed"),
        "Error message must clearly identify missing aria2c: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_cooperative_cancellation() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir = std::env::temp_dir().join(format!("test_vod_cancel_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 99999;
    let source = Arc::new(create_fake_vod_source(video_no, "v_cancel", "in_cancel"));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: false,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    cancel_token.cancel(); // Pre-cancelled

    let err = run_vod_archival(args, source, cancel_token)
        .await
        .expect_err("Cancelled token must abort pipeline");

    assert!(
        format!("{err:#}").contains("cancelled"),
        "Error should indicate cancellation: {err:#}"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_fake_vod_source_chat_paging_and_queues() {
    use chzzk_load::chzzk::models_vod::{VodChatError, VodChatPage};
    use chzzk_load::chzzk::source::VodSource;

    let source = FakeVodSource::new();
    let video_no = 12345;

    // Enqueue a 429 error followed by a successful page
    source.enqueue_chat_page_response(video_no, 0, Err(VodChatError::RateLimited));
    source.enqueue_chat_page_response(
        video_no,
        0,
        Ok(VodChatPage {
            video_chats: vec![serde_json::json!({
                "playerMessageTime": 5000,
                "content": "message after retry"
            })],
            next_player_message_time: Some(10000),
        }),
    );

    // First call returns 429 error
    let err = source
        .get_chat_page(video_no, 0)
        .await
        .expect_err("Expected 429 error");
    assert!(matches!(err, VodChatError::RateLimited));

    // Second call returns successful page
    let page = source
        .get_chat_page(video_no, 0)
        .await
        .expect("Expected successful page");
    assert_eq!(page.video_chats.len(), 1);
    assert_eq!(page.next_player_message_time, Some(10000));
    assert_eq!(source.chat_page_call_count(), 2);
}

#[tokio::test]
async fn test_vod_archival_with_concurrent_chat_integration() {
    use chzzk_load::chzzk::models_chat::RecordedChatMessage;
    use chzzk_load::chzzk::models_vod::VodChatPage;

    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_chat_int_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 55512;
    let source = create_fake_vod_source(video_no, "v_chat_int", "in_chat");

    // Configure chat pages
    source.set_chat_page(
        video_no,
        0,
        VodChatPage {
            video_chats: vec![
                serde_json::json!({
                    "playerMessageTime": 1000,
                    "messageTime": 1727694001000_u64,
                    "nickname": "Audience1",
                    "content": "Live stream replay test",
                    "msgTypeCode": 1
                }),
                serde_json::json!({
                    "playerMessageTime": 2500,
                    "messageTime": 1727694002500_u64,
                    "nickname": "Donor1",
                    "content": "Nice play!",
                    "payAmount": 10000,
                    "msgTypeCode": 10
                }),
            ],
            next_player_message_time: None,
        },
    );

    let source = Arc::new(source);

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: false,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source, cancel_token)
        .await
        .expect("run_vod_archival with concurrent chat should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.segments_downloaded, 3);
    assert_eq!(summary.chat_messages, 2);

    let finalized_video = summary.output_dir.join("vod.mp4");
    assert!(finalized_video.exists(), "vod.mp4 must exist");

    let finalized_chat = summary.output_dir.join("vod_chat.jsonl");
    assert!(finalized_chat.exists(), "vod_chat.jsonl must exist");
    assert_eq!(summary.chat_path, Some(finalized_chat.clone()));

    // Verify chat contents deserializable as canonical RecordedChatMessage
    let content = std::fs::read_to_string(&finalized_chat).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);

    let msg1: RecordedChatMessage = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(msg1.player_message_time, Some(1000));
    assert_eq!(msg1.nickname, "Audience1");
    assert_eq!(msg1.content, "Live stream replay test");

    let msg2: RecordedChatMessage = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(msg2.player_message_time, Some(2500));
    assert_eq!(msg2.nickname, "Donor1");
    assert_eq!(msg2.donation_amount, Some(10000));
    assert_eq!(msg2.msg_type, "DONATION");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_no_chat_flag_skips_chat() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir = std::env::temp_dir().join(format!("test_vod_no_chat_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 66612;
    let source = Arc::new(create_fake_vod_source(video_no, "v_no_chat", "in_no_chat"));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: true,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source.clone(), cancel_token)
        .await
        .expect("run_vod_archival with --no-chat should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.segments_downloaded, 3);
    assert_eq!(summary.chat_messages, 0);
    assert_eq!(summary.chat_path, None);

    let finalized_video = summary.output_dir.join("vod.mp4");
    assert!(finalized_video.exists(), "vod.mp4 must exist");

    // vod_chat.jsonl must NOT exist
    let chat_file = summary.output_dir.join("vod_chat.jsonl");
    assert!(
        !chat_file.exists(),
        "vod_chat.jsonl must not be created when --no-chat is specified"
    );
    assert!(!summary.output_dir.join("vod_chat.jsonl.part").exists());

    // FakeVodSource get_chat_page must not have been invoked
    assert_eq!(source.chat_page_call_count(), 0);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_dual_event_metadata_synthesis_and_promotion() {
    use chzzk_load::vod::{VodMetadataEvent, VodMetadataEventType};

    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_meta_e2e_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 8881234;
    let video_id = "neon_meta_vid";
    let in_key = "key_meta_secret";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: true,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source, cancel_token)
        .await
        .expect("run_vod_archival should succeed");

    // 1. Verify metadata file exists and .part file is unlinked/promoted
    let final_meta = summary.output_dir.join("vod_metadata.jsonl");
    assert!(final_meta.exists(), "vod_metadata.jsonl must exist");
    assert_eq!(summary.metadata_path, final_meta);

    let part_meta = summary.output_dir.join("vod_metadata.jsonl.part");
    assert!(
        !part_meta.exists(),
        "vod_metadata.jsonl.part must be promoted"
    );

    // 2. Read and verify dual-event JSON Lines format
    let content = std::fs::read_to_string(&final_meta).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "vod_metadata.jsonl must contain exactly 2 events"
    );

    // Start event at stream_offset_ms: 0
    let start_event: VodMetadataEvent = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(start_event.version, 2);
    assert_eq!(start_event.event, VodMetadataEventType::Start);
    assert_eq!(start_event.stream_offset_ms, 0);
    assert_eq!(start_event.state.video_no, video_no);
    assert_eq!(start_event.state.video_id, video_id);
    assert_eq!(start_event.state.title, "Chzzk Live Replay");
    assert_eq!(start_event.state.channel_name, "ProGamer");
    assert_eq!(start_event.state.channel_id, "chan_gamer_01");
    assert_eq!(start_event.state.category_type.as_deref(), Some("GAME"));
    assert_eq!(start_event.state.tags, vec!["game"]);

    // Finish event at stream_offset_ms: duration_ms (7200s -> 7,200,000ms)
    let finish_event: VodMetadataEvent = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(finish_event.version, 2);
    assert_eq!(finish_event.event, VodMetadataEventType::Finish);
    assert_eq!(finish_event.stream_offset_ms, 7_200_000);
    assert_eq!(finish_event.state.video_no, video_no);
    assert_eq!(finish_event.state.title, "Chzzk Live Replay");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_failure_cleans_up_staged_metadata() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_meta_fail_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    // video_id containing 'fail' causes mock_aria2c to exit with code 1
    let video_no = 9991234;
    let video_id = "fail_video_neon";
    let in_key = "key_fail";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        no_chat: true,
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let err = run_vod_archival(args, source, cancel_token)
        .await
        .expect_err("Download failure should trigger pipeline error");

    assert!(format!("{err:#}").contains("download failed"));

    // Verify .part metadata file is unlinked and staging directory removed
    let folder_name = "[2026-09-30_110000] ProGamer - Chzzk Live Replay";
    let output_dir = temp_dir.join(folder_name);
    let part_meta = output_dir.join("vod_metadata.jsonl.part");
    assert!(
        !part_meta.exists(),
        "vod_metadata.jsonl.part must be unlinked on failure"
    );

    let final_meta = output_dir.join("vod_metadata.jsonl");
    assert!(
        !final_meta.exists(),
        "vod_metadata.jsonl must not be created on failure"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}
