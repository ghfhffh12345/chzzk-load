mod common;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use chzzk_load::chzzk::models::ChannelInfo;
use chzzk_load::chzzk::models_vod::{
    VodDetail, VodEncodingOption, VodPlaybackInfo, VodPlaybackMedia,
};
use chzzk_load::chzzk::source::FakeVodSource;
use chzzk_load::cli::VodArgs;
use chzzk_load::config::RcloneConfig;
use chzzk_load::vod::{VodOptions, run_vod_archival, run_vod_archival_with_options};
use common::mock_aria2c::get_mock_aria2c_bin;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::mock_rclone::get_mock_rclone_bin;

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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
    };

    let options = VodOptions {
        aria2c_bin: Some("/nonexistent/bin/aria2c_missing_binary".to_string()),
        ..Default::default()
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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
        temp_dir: None,
        no_chat: true,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
        temp_dir: None,
        no_chat: true,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
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
async fn test_vod_archival_retains_part_files_and_staging_dir_on_failure() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_fail_retain_{}", rand::random::<u32>()));
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
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
    };

    let cancel_token = CancellationToken::new();
    let err = run_vod_archival(args, source, cancel_token)
        .await
        .expect_err("Download failure should trigger pipeline error");

    assert!(format!("{err:#}").contains("download failed"));

    let folder_name = "[2026-09-30_110000] ProGamer - Chzzk Live Replay";
    let output_dir = temp_dir.join(folder_name);

    // Verify .part files and staging directory are RETAINED upon failure for manual resume
    let part_meta = output_dir.join("vod_metadata.jsonl.part");
    assert!(
        part_meta.exists(),
        "vod_metadata.jsonl.part must be retained on failure"
    );

    let staging_dir = output_dir.join(format!(".vod_staging_{video_id}"));
    assert!(
        staging_dir.exists(),
        "staging directory must be retained on failure for subsequent resume"
    );

    let final_meta = output_dir.join("vod_metadata.jsonl");
    assert!(
        !final_meta.exists(),
        "vod_metadata.jsonl must not be created on failure"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_resumes_partial_download_skipping_existing_segments() {
    use chzzk_load::chzzk::models_vod::VodChatPage;

    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_dir = std::env::temp_dir().join(format!("test_vod_resume_{}", rand::random::<u32>()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let video_no = 444321;
    let video_id = "neon_resume_vid";
    let in_key = "key_resume";
    let source = create_fake_vod_source(video_no, video_id, in_key);

    // Setup chat pages:
    // Chat page for cursor 10000 (resuming from probed cursor 10000)
    source.set_chat_page(
        video_no,
        10000,
        VodChatPage {
            video_chats: vec![
                // Overlapping duplicate at 10000 (must be skipped)
                serde_json::json!({
                    "playerMessageTime": 10000,
                    "nickname": "Audience1",
                    "content": "seeded msg"
                }),
                // New message at 20000
                serde_json::json!({
                    "playerMessageTime": 20000,
                    "nickname": "Audience2",
                    "content": "resumed msg"
                }),
            ],
            next_player_message_time: None,
        },
    );

    let source = Arc::new(source);

    let folder_name = "[2026-09-30_110000] ProGamer - Chzzk Live Replay";
    let output_dir = temp_dir.join(folder_name);
    let staging_dir = output_dir.join(format!(".vod_staging_{video_id}"));
    std::fs::create_dir_all(&staging_dir).unwrap();

    // 1. Simulate existing completed segment: chunk_00.ts (with non-zero bytes and no .aria2 file)
    let completed_seg = staging_dir.join("chunk_00.ts");
    let initial_data = [0x47u8; 376];
    std::fs::write(&completed_seg, initial_data).unwrap();

    // 2. Simulate partially downloaded chat: vod_chat.jsonl.part
    let seed_chat = serde_json::json!({
        "playerMessageTime": 10000,
        "time": 1727694010000_u64,
        "nickname": "Audience1",
        "content": "seeded msg",
        "msgTypeCode": 1
    });
    let part_chat = output_dir.join("vod_chat.jsonl.part");
    std::fs::write(
        &part_chat,
        format!(
            "{}\n",
            serde_json::to_string(&chzzk_load::vod::normalize_vod_chat_message(
                seed_chat, 10000, None
            ))
            .unwrap()
        ),
    )
    .unwrap();

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source.clone(), cancel_token)
        .await
        .expect("Resumed archival should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.segments_downloaded, 3);
    assert_eq!(summary.chat_messages, 2); // 1 seed + 1 resumed (duplicate 10000 suppressed)

    // Verify finalized artifacts
    let final_video = summary.output_dir.join("vod.mp4");
    assert!(final_video.exists(), "vod.mp4 must exist");

    let final_chat = summary.output_dir.join("vod_chat.jsonl");
    assert!(final_chat.exists(), "vod_chat.jsonl must exist");
    assert!(!part_chat.exists(), "vod_chat.jsonl.part must be promoted");

    let chat_lines: Vec<String> = std::fs::read_to_string(&final_chat)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(chat_lines.len(), 2);

    let final_meta = summary.output_dir.join("vod_metadata.jsonl");
    assert!(final_meta.exists(), "vod_metadata.jsonl must exist");

    // Staging directory must be automatically purged upon success
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged on successful completion"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_custom_temp_dir() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();

    let temp_root =
        std::env::temp_dir().join(format!("test_vod_custom_temp_{}", rand::random::<u32>()));
    let output_base = temp_root.join("recordings");
    let custom_temp = temp_root.join("fast_scratch");
    std::fs::create_dir_all(&output_base).unwrap();
    std::fs::create_dir_all(&custom_temp).unwrap();

    let video_no = 555666;
    let video_id = "neon_custom_scratch";
    let in_key = "key_scratch";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(output_base.clone()),
        temp_dir: Some(custom_temp.clone()),
        no_chat: true,
        headless: false,
        config: None,
        upload: false,
        delete_after_upload: false,
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival(args, source, cancel_token)
        .await
        .expect("run_vod_archival with custom --temp-dir should succeed");

    assert_eq!(summary.video_no, video_no);
    assert_eq!(summary.segments_downloaded, 3);

    let final_video = summary.output_dir.join("vod.mp4");
    assert!(final_video.exists(), "vod.mp4 must exist in output_dir");

    // The custom staging directory (.vod_staging_<video_id>) inside custom_temp must be purged upon success
    let custom_staging = custom_temp.join(format!(".vod_staging_{video_id}"));
    assert!(
        !custom_staging.exists(),
        "custom staging dir in temp_dir must be purged upon success"
    );

    let _ = std::fs::remove_dir_all(&temp_root);
}

#[tokio::test]
async fn test_vod_archival_upload_preflight_failure() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_vod_up_preflight_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let video_no = 111222;
    let source = Arc::new(create_fake_vod_source(
        video_no,
        "v_preflight",
        "key_preflight",
    ));

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(temp_dir.clone()),
        temp_dir: None,
        no_chat: true,
        headless: false,
        config: None,
        upload: true,
        delete_after_upload: false,
    };

    let options = VodOptions {
        rclone_config: Some(RcloneConfig {
            remote_path: "remote:fail_conn_bucket".to_string(),
            upload_concurrency: 1,
            rclone_bin: "rclone".to_string(),
            extra_args: vec![],
            consolidate_extra_args: vec![],
            skip_connection_check: false,
        }),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();
    let err = run_vod_archival_with_options(args, source, options, cancel_token)
        .await
        .expect_err("Pre-flight rclone check failure must abort pipeline");

    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("Pre-flight rclone remote connection validation check failed")
            || err_msg.contains("rclone connection check failed"),
        "Error message must report pre-flight connection failure: {err_msg}"
    );

    // Verify no output subfolder was created
    let entries: Vec<_> = std::fs::read_dir(&temp_dir).unwrap().collect();
    assert!(
        entries.is_empty(),
        "No subfolders should be created on pre-flight failure"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_vod_archival_upload_default_retention_preserves_local_files() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let temp_root =
        std::env::temp_dir().join(format!("test_vod_up_retain_{}", rand::random::<u32>()));
    let local_output = temp_root.join("local_vods");
    let remote_storage = temp_root.join("cloud_storage");
    std::fs::create_dir_all(&local_output).unwrap();
    std::fs::create_dir_all(&remote_storage).unwrap();

    let video_no = 222333;
    let video_id = "v_upload_retain";
    let in_key = "key_up_retain";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let remote_path_str = format!("remote:{}", remote_storage.to_string_lossy());

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(local_output.clone()),
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: true,
        delete_after_upload: false,
    };

    let options = VodOptions {
        rclone_config: Some(RcloneConfig {
            remote_path: remote_path_str,
            upload_concurrency: 1,
            rclone_bin: "rclone".to_string(),
            extra_args: vec![],
            consolidate_extra_args: vec![],
            skip_connection_check: false,
        }),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival_with_options(args, source, options, cancel_token)
        .await
        .expect("VOD archival with upload should succeed");

    assert!(summary.uploaded, "summary.uploaded must be true");

    // 1. Verify local files are PRESERVED by default
    assert!(
        summary.video_path.exists(),
        "Local vod.mp4 must remain on disk"
    );
    assert!(
        summary.chat_path.as_ref().unwrap().exists(),
        "Local vod_chat.jsonl must remain on disk"
    );
    assert!(
        summary.metadata_path.exists(),
        "Local vod_metadata.jsonl must remain on disk"
    );

    // 2. Verify remote files exist at promoted destination
    let folder_name = summary.output_dir.file_name().unwrap();
    let remote_folder = remote_storage.join(folder_name);
    let remote_video = remote_folder.join("vod.mp4");
    let remote_chat = remote_folder.join("vod_chat.jsonl");
    let remote_metadata = remote_folder.join("vod_metadata.jsonl");

    assert!(remote_video.exists(), "Remote vod.mp4 must exist");
    assert!(remote_chat.exists(), "Remote vod_chat.jsonl must exist");
    assert!(
        remote_metadata.exists(),
        "Remote vod_metadata.jsonl must exist"
    );

    // 3. Verify remote .part files were promoted and no longer exist
    assert!(
        !remote_folder.join("vod.mp4.part").exists(),
        "Remote vod.mp4.part must be promoted"
    );
    assert!(
        !remote_folder.join("vod_chat.jsonl.part").exists(),
        "Remote vod_chat.jsonl.part must be promoted"
    );
    assert!(
        !remote_folder.join("vod_metadata.jsonl.part").exists(),
        "Remote vod_metadata.jsonl.part must be promoted"
    );

    let _ = std::fs::remove_dir_all(&temp_root);
}

#[tokio::test]
async fn test_vod_archival_upload_delete_after_upload_purges_local_files() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let temp_root = std::env::temp_dir().join(format!("test_vod_up_del_{}", rand::random::<u32>()));
    let local_output = temp_root.join("local_vods");
    let remote_storage = temp_root.join("cloud_storage");
    std::fs::create_dir_all(&local_output).unwrap();
    std::fs::create_dir_all(&remote_storage).unwrap();

    let video_no = 333444;
    let video_id = "v_upload_del";
    let in_key = "key_up_del";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let remote_path_str = format!("remote:{}", remote_storage.to_string_lossy());

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(local_output.clone()),
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: true,
        delete_after_upload: true,
    };

    let options = VodOptions {
        rclone_config: Some(RcloneConfig {
            remote_path: remote_path_str,
            upload_concurrency: 1,
            rclone_bin: "rclone".to_string(),
            extra_args: vec![],
            consolidate_extra_args: vec![],
            skip_connection_check: false,
        }),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();
    let summary = run_vod_archival_with_options(args, source, options, cancel_token)
        .await
        .expect("VOD archival with upload and --delete-after-upload should succeed");

    assert!(summary.uploaded, "summary.uploaded must be true");

    // 1. Verify remote files exist
    let folder_name = summary.output_dir.file_name().unwrap();
    let remote_folder = remote_storage.join(folder_name);
    assert!(
        remote_folder.join("vod.mp4").exists(),
        "Remote vod.mp4 must exist"
    );
    assert!(
        remote_folder.join("vod_chat.jsonl").exists(),
        "Remote vod_chat.jsonl must exist"
    );
    assert!(
        remote_folder.join("vod_metadata.jsonl").exists(),
        "Remote vod_metadata.jsonl must exist"
    );

    // 2. Verify local files are PURGED post-upload
    assert!(!summary.video_path.exists(), "Local vod.mp4 must be purged");
    assert!(
        !summary.chat_path.as_ref().unwrap().exists(),
        "Local vod_chat.jsonl must be purged"
    );
    assert!(
        !summary.metadata_path.exists(),
        "Local vod_metadata.jsonl must be purged"
    );

    let _ = std::fs::remove_dir_all(&temp_root);
}

#[tokio::test]
async fn test_vod_archival_upload_failure_preserves_local_artifacts() {
    let _mock_aria2c = get_mock_aria2c_bin();
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let temp_root = std::env::temp_dir().join(format!("test_vod_up_err_{}", rand::random::<u32>()));
    let local_output = temp_root.join("local_vods");
    let remote_storage = temp_root.join("fail_copy_remote");
    std::fs::create_dir_all(&local_output).unwrap();
    std::fs::create_dir_all(&remote_storage).unwrap();

    let video_no = 444555;
    let video_id = "v_upload_test_id";
    let in_key = "key_up_test";
    let source = Arc::new(create_fake_vod_source(video_no, video_id, in_key));

    let remote_path_str = format!("remote:{}", remote_storage.to_string_lossy());

    let args = VodArgs {
        target: video_no.to_string(),
        quality: "best".to_string(),
        output_dir: Some(local_output.clone()),
        temp_dir: None,
        no_chat: false,
        headless: false,
        config: None,
        upload: true,
        delete_after_upload: true, // even if delete_after_upload is set, failures MUST preserve local files
    };

    let options = VodOptions {
        rclone_config: Some(RcloneConfig {
            remote_path: remote_path_str,
            upload_concurrency: 1,
            rclone_bin: "rclone".to_string(),
            extra_args: vec![],
            consolidate_extra_args: vec![],
            skip_connection_check: false,
        }),
        ..Default::default()
    };

    let cancel_token = CancellationToken::new();
    let err = run_vod_archival_with_options(args, source, options, cancel_token)
        .await
        .expect_err("Upload failure must return error");

    let err_msg = format!("{err:#}");
    assert!(
        err_msg.contains("cloud upload failed") || err_msg.contains("rclone failed"),
        "Error message must clearly report upload failure: {err_msg}"
    );

    // Verify local files are strictly preserved
    let folder_name = "[2026-09-30_110000] ProGamer - Chzzk Live Replay";
    let output_dir = local_output.join(folder_name);
    assert!(
        output_dir.join("vod.mp4").exists(),
        "Local vod.mp4 must be preserved on upload failure"
    );
    assert!(
        output_dir.join("vod_chat.jsonl").exists(),
        "Local vod_chat.jsonl must be preserved on upload failure"
    );
    assert!(
        output_dir.join("vod_metadata.jsonl").exists(),
        "Local vod_metadata.jsonl must be preserved on upload failure"
    );

    let _ = std::fs::remove_dir_all(&temp_root);
}
