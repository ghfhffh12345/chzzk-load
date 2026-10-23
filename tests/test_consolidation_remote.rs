use std::fs;
use std::path::PathBuf;

mod common;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::mock_rclone::get_mock_rclone_bin;

use chzzk_load::cli::ConsolidateArgs;
use chzzk_load::consolidation::{run_consolidation, upload_remote_staged_file};

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}_{}", rand::random::<u32>()));
    fs::create_dir_all(&dir).unwrap();
    common::register_mock_temp_dir(dir.clone());
    dir
}

fn make_chat_line(time_ms: u64, user: &str, content: &str) -> String {
    serde_json::to_string(&chzzk_load::chzzk::models_chat::RecordedChatMessage {
        time_ms,
        datetime: "2026-10-08 20:00:00".to_string(),
        msg_type: "COMMERCE".to_string(),
        nickname: "tester".to_string(),
        user_id_hash: Some(user.to_string()),
        content: content.to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    })
    .unwrap()
}

#[tokio::test]
async fn test_upload_remote_staged_file_success_and_failure() {
    let mock_rclone = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_upload_staged_direct");
    let local_file = temp_dir.join("local.txt");
    let remote_dest_file = temp_dir.join("remote.txt");

    fs::write(&local_file, b"test payload").unwrap();

    let remote_dest = format!(
        "mock_remote:{}",
        remote_dest_file.to_string_lossy().replace('\\', "/")
    );

    // 1. Success
    let res =
        upload_remote_staged_file(mock_rclone.to_str().unwrap(), &local_file, &remote_dest).await;
    assert!(res.is_ok(), "Upload should succeed: {:?}", res.err());
    assert_eq!(
        fs::read_to_string(&remote_dest_file).unwrap(),
        "test payload"
    );

    // 2. Failure trigger via fail_copy
    let fail_dest = format!(
        "mock_remote:{}",
        temp_dir
            .join("fail_copy.txt")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let fail_res =
        upload_remote_staged_file(mock_rclone.to_str().unwrap(), &local_file, &fail_dest).await;
    assert!(fail_res.is_err(), "fail_copy should cause error");
}

#[tokio::test]
async fn test_remote_consolidation_complete_pipeline_loopback_upload_and_purge() {
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_full");
    let staging_parent = create_temp_test_dir("test_remote_cons_staging_parent");

    // Create remote chunks
    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chunk1 = remote_dir.join("chunk_0001.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let chat1 = remote_dir.join("chat_0001.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    fs::write(&chunk1, b"mock_video_chunk_1").unwrap();

    let line1 = make_chat_line(10_000, "user1", "msg1");
    let line2 = make_chat_line(15_000, "user2", "msg2");
    let line3 = make_chat_line(20_000, "user3", "msg3");

    fs::write(&chat0, format!("{line1}\n{line2}\n")).unwrap();
    fs::write(&chat1, format!("{line2}\n{line3}\n")).unwrap(); // line2 duplicate
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
    };

    let summary = run_consolidation(args)
        .await
        .expect("run_consolidation should succeed");

    assert_eq!(summary.video_chunks_count, 2);
    assert_eq!(summary.chat_chunks_count, 2);
    assert!(summary.has_metadata);

    // 1. Verify remote consolidated files exist
    let remote_mp4 = remote_dir.join("consolidated.mp4");
    let remote_jsonl = remote_dir.join("consolidated.jsonl");
    assert!(remote_mp4.exists(), "Remote consolidated.mp4 must exist");
    assert!(
        remote_jsonl.exists(),
        "Remote consolidated.jsonl must exist"
    );

    // 2. Verify remote .part files do NOT exist
    assert!(!remote_dir.join("consolidated.mp4.part").exists());
    assert!(!remote_dir.join("consolidated.jsonl.part").exists());

    // 3. Verify deduplication was applied to chat
    let chat_content = fs::read_to_string(&remote_jsonl).unwrap();
    let lines: Vec<&str> = chat_content.lines().collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], line1);
    assert_eq!(lines[1], line2);
    assert_eq!(lines[2], line3);

    // 4. Verify original video and chat chunks are purged from remote
    assert!(!chunk0.exists(), "chunk_0000.ts must be purged");
    assert!(!chunk1.exists(), "chunk_0001.ts must be purged");
    assert!(!chat0.exists(), "chat_0000.jsonl must be purged");
    assert!(!chat1.exists(), "chat_0001.jsonl must be purged");

    // 5. Verify metadata.jsonl is strictly preserved on remote
    assert!(meta.exists(), "metadata.jsonl must be preserved");

    // 6. Verify local staging directory was completely removed
    assert!(
        !staging_parent.exists(),
        "Local staging directory must be unlinked post-upload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_keep_original_retains_chunks_and_cleans_staging() {
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_keep");
    let staging_parent = create_temp_test_dir("test_remote_cons_keep_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: true,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
    };

    let summary = run_consolidation(args)
        .await
        .expect("run_consolidation should succeed with keep_original");

    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // 1. Verify remote consolidated files exist
    assert!(remote_dir.join("consolidated.mp4").exists());
    assert!(remote_dir.join("consolidated.jsonl").exists());

    // 2. Verify original chunks STILL exist on remote
    assert!(
        chunk0.exists(),
        "chunk0 must be retained with --keep-original"
    );
    assert!(
        chat0.exists(),
        "chat0 must be retained with --keep-original"
    );
    assert!(meta.exists(), "metadata.jsonl must be retained");

    // 3. Verify local staging directory is STILL cleaned up
    assert!(
        !staging_parent.exists(),
        "Local staging directory must be cleaned up even with keep_original"
    );
}

#[tokio::test]
async fn test_remote_consolidation_dual_pipeline_failure_preserves_remote_chunks() {
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_fail");
    let staging_parent = create_temp_test_dir("test_remote_cons_fail_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let malformed = "not valid json";
    fs::write(&chat0, format!("{malformed}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    // strict: true causes chat failure
    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: false,
        strict: true,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
    };

    let res = run_consolidation(args).await;
    assert!(
        res.is_err(),
        "Consolidation should fail when chat has malformed line in strict mode"
    );

    // 1. Verify NO remote files were modified
    assert!(!remote_dir.join("consolidated.mp4").exists());
    assert!(!remote_dir.join("consolidated.jsonl").exists());
    assert!(!remote_dir.join("consolidated.mp4.part").exists());
    assert!(!remote_dir.join("consolidated.jsonl.part").exists());

    // 2. Verify original chunks are completely intact
    assert!(chunk0.exists());
    assert!(chat0.exists());
    assert!(meta.exists());
}

#[tokio::test]
async fn test_remote_consolidation_upload_failure_cleans_up_remote_part_and_preserves_chunks() {
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_upload_fail_copy");
    let staging_parent = create_temp_test_dir("test_remote_cons_upload_fail_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    // Directory name includes fail_copy to trigger mock rclone copyto error
    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
    };

    let res = run_consolidation(args).await;
    assert!(
        res.is_err(),
        "Upload failure should cause consolidation to fail"
    );

    // 1. Remote final files must NOT exist
    assert!(!remote_dir.join("consolidated.mp4").exists());
    assert!(!remote_dir.join("consolidated.jsonl").exists());

    // 2. Remote .part files must NOT linger
    assert!(!remote_dir.join("consolidated.mp4.part").exists());
    assert!(!remote_dir.join("consolidated.jsonl.part").exists());

    // 3. Original chunks must NOT be purged
    assert!(chunk0.exists());
    assert!(chat0.exists());
    assert!(meta.exists());
}
