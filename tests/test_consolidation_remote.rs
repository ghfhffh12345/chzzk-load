use std::fs;
use std::path::PathBuf;

mod common;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::mock_rclone::get_mock_rclone_bin;

use chzzk_load::cli::ConsolidateArgs;
use chzzk_load::consolidation::{
    build_remote_staged_upload_command, run_consolidation, run_consolidation_with_cancel,
    upload_remote_staged_file, upload_remote_staged_file_with_args, upload_remote_staged_media,
};
use tokio_util::sync::CancellationToken;

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

fn setup_test_stream_chunks(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
    let chunk0 = dir.join("chunk_0000.ts");
    let chat0 = dir.join("chat_0000.jsonl");
    let meta = dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    (chunk0, chat0, meta)
}

static REMOTE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_upload_remote_staged_file_success_and_failure() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
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
    let _lock = REMOTE_TEST_MUTEX.lock().await;
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
        config: None,
    };

    let summary = run_consolidation(args)
        .await
        .expect("run_consolidation should succeed");

    assert_eq!(summary.video_chunks_count, 2);
    assert_eq!(summary.chat_chunks_count, 2);
    assert!(summary.has_metadata);

    // 1. Verify remote consolidated files exist
    let remote_mp4 = remote_dir.join("stream.mp4");
    let remote_jsonl = remote_dir.join("chat.jsonl");
    assert!(remote_mp4.exists(), "Remote stream.mp4 must exist");
    assert!(remote_jsonl.exists(), "Remote chat.jsonl must exist");

    // 2. Verify remote .part files do NOT exist
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

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
    let _lock = REMOTE_TEST_MUTEX.lock().await;
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
        config: None,
    };

    let summary = run_consolidation(args)
        .await
        .expect("run_consolidation should succeed with keep_original");

    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // 1. Verify remote consolidated files exist
    assert!(remote_dir.join("stream.mp4").exists());
    assert!(remote_dir.join("chat.jsonl").exists());

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
    let _lock = REMOTE_TEST_MUTEX.lock().await;
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
        config: None,
    };

    let res = run_consolidation(args).await;
    assert!(
        res.is_err(),
        "Consolidation should fail when chat has malformed line in strict mode"
    );

    // 1. Verify NO remote files were modified
    assert!(!remote_dir.join("stream.mp4").exists());
    assert!(!remote_dir.join("chat.jsonl").exists());
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

    // 2. Verify original chunks are completely intact
    assert!(chunk0.exists());
    assert!(chat0.exists());
    assert!(meta.exists());
}

#[tokio::test]
async fn test_remote_consolidation_upload_failure_cleans_up_remote_part_and_preserves_chunks() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
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
        config: None,
    };

    let res = run_consolidation(args).await;
    assert!(
        res.is_err(),
        "Upload failure should cause consolidation to fail"
    );

    // 1. Remote final files must NOT exist
    assert!(!remote_dir.join("stream.mp4").exists());
    assert!(!remote_dir.join("chat.jsonl").exists());

    // 2. Remote .part files must NOT linger
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

    // 3. Original chunks must NOT be purged
    assert!(chunk0.exists());
    assert!(chat0.exists());
    assert!(meta.exists());

    // 4. Finalized local staging files must be PRESERVED
    let local_mp4 = staging_parent.join("stream.mp4");
    let local_jsonl = staging_parent.join("chat.jsonl");
    assert!(
        local_mp4.exists(),
        "Local stream.mp4 must be preserved in staging directory upon upload failure"
    );
    assert!(
        local_jsonl.exists(),
        "Local chat.jsonl must be preserved in staging directory upon upload failure"
    );
}

#[tokio::test]
async fn test_remote_consolidation_resume_upload_bypasses_ffmpeg_on_valid_staging() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_resume_bypasses_ffmpeg");
    let staging_parent = create_temp_test_dir("test_remote_resume_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    // Stage 1: Pre-populate valid completed staging files (simulating completed remux from prior run)
    let local_mp4 = staging_parent.join("stream.mp4");
    let local_jsonl = staging_parent.join("chat.jsonl");

    // Faststart MP4: ftyp (24 bytes) + moov (16 bytes) + mdat (16 bytes)
    let mut fs_bytes = vec![0x00, 0x00, 0x00, 0x18];
    fs_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"moovmvhd1234");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"mdat12345678");
    fs::write(&local_mp4, &fs_bytes).unwrap();
    fs::write(&local_jsonl, format!("{line1}\n")).unwrap();

    // Point CHZZK_LOAD_FFMPEG_BIN to a nonexistent binary to guarantee that if FFmpeg is spawned, it fails!
    let bogus_ffmpeg = staging_parent.join("nonexistent_ffmpeg_bin_that_fails_if_spawned.exe");
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", &bogus_ffmpeg);
    }

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
        config: None,
    };

    let summary = run_consolidation(args)
        .await
        .expect("Resumed consolidation must succeed without calling FFmpeg");

    // Restore real mock ffmpeg bin in env
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_ffmpeg);
    }

    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // 1. Verify remote consolidated files exist
    assert!(remote_dir.join("stream.mp4").exists());
    assert!(remote_dir.join("chat.jsonl").exists());

    // 2. Verify original chunks were purged
    assert!(!chunk0.exists(), "Original chunk0 must be purged");
    assert!(!chat0.exists(), "Original chat0 must be purged");
    assert!(meta.exists(), "metadata.jsonl must be preserved");

    // 3. Verify local staging directory was cleaned up upon confirmed upload
    assert!(
        !staging_parent.exists(),
        "Local staging directory must be unlinked post-upload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_overwrite_forces_clean_remux() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_overwrite_forces_remux");
    let staging_parent = create_temp_test_dir("test_remote_overwrite_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    // Pre-populate valid staging files
    let local_mp4 = staging_parent.join("stream.mp4");
    let local_jsonl = staging_parent.join("chat.jsonl");
    fs::write(&local_mp4, b"existing_mp4_bytes").unwrap();
    fs::write(&local_jsonl, b"existing_jsonl_bytes\n").unwrap();

    // Point CHZZK_LOAD_FFMPEG_BIN to a bogus binary
    let bogus_ffmpeg = staging_parent.join("nonexistent_ffmpeg_bin.exe");
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", &bogus_ffmpeg);
    }

    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    // overwrite: true MUST invalidate staging and attempt to spawn FFmpeg
    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: true,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
        config: None,
    };

    let res = run_consolidation(args).await;

    // Restore real mock ffmpeg
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_ffmpeg);
    }

    assert!(
        res.is_err(),
        "--overwrite must force FFmpeg remuxing and fail when FFmpeg binary is invalid"
    );
    let err_msg = format!("{:#}", res.unwrap_err());
    assert!(
        err_msg.contains("Failed to spawn ffmpeg binary"),
        "error must indicate ffmpeg spawn failure: {err_msg}"
    );

    // Existing staging files should have been invalidated
    assert!(
        !local_mp4.exists(),
        "Local stream.mp4 must be invalidated by --overwrite"
    );
    assert!(
        !local_jsonl.exists(),
        "Local chat.jsonl must be invalidated by --overwrite"
    );
}

#[tokio::test]
async fn test_remote_consolidation_upload_failure_then_resume_without_ffmpeg_end_to_end() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir_fail = create_temp_test_dir("test_remote_resume_e2e_fail_copy");
    let staging_parent = create_temp_test_dir("test_remote_resume_e2e_staging");

    let chunk0 = remote_dir_fail.join("chunk_0000.ts");
    let chat0 = remote_dir_fail.join("chat_0000.jsonl");
    let meta = remote_dir_fail.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    let remote_path_fail = format!(
        "mock_remote:{}",
        remote_dir_fail.to_string_lossy().replace('\\', "/")
    );

    // Phase 1: First run fails during cloud upload because destination path contains fail_copy
    let args1 = ConsolidateArgs {
        path: remote_path_fail,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
        config: None,
    };

    let res1 = run_consolidation(args1).await;
    assert!(res1.is_err(), "Phase 1 must fail on cloud upload");

    // Verify local staging files are intact
    let local_mp4 = staging_parent.join("stream.mp4");
    let local_jsonl = staging_parent.join("chat.jsonl");
    assert!(
        local_mp4.exists(),
        "Local stream.mp4 must be preserved in staging directory"
    );
    assert!(
        local_jsonl.exists(),
        "Local chat.jsonl must be preserved in staging directory"
    );
    assert!(
        !remote_dir_fail.join("stream.mp4").exists(),
        "Remote stream.mp4 must not exist after failed upload"
    );

    // Phase 2: Move the remote directory to a path without fail_copy
    let remote_dir_ok = create_temp_test_dir("test_remote_resume_e2e_recovered");
    let ok_chunk0 = remote_dir_ok.join("chunk_0000.ts");
    let ok_chat0 = remote_dir_ok.join("chat_0000.jsonl");
    let ok_meta = remote_dir_ok.join("metadata.jsonl");

    fs::rename(&chunk0, &ok_chunk0).unwrap();
    fs::rename(&chat0, &ok_chat0).unwrap();
    fs::rename(&meta, &ok_meta).unwrap();

    let remote_path_ok = format!(
        "mock_remote:{}",
        remote_dir_ok.to_string_lossy().replace('\\', "/")
    );

    // Point CHZZK_LOAD_FFMPEG_BIN to a bogus binary to verify FFmpeg is strictly bypassed
    let bogus_ffmpeg = staging_parent.join("nonexistent_ffmpeg_bin.exe");
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", &bogus_ffmpeg);
    }

    let args2 = ConsolidateArgs {
        path: remote_path_ok,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
        config: None,
    };

    let summary2 = run_consolidation(args2)
        .await
        .expect("Phase 2 must resume cloud upload directly and succeed without calling FFmpeg");

    // Restore real mock ffmpeg
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_ffmpeg);
    }

    assert_eq!(summary2.video_chunks_count, 1);
    assert_eq!(summary2.chat_chunks_count, 1);

    // Remote consolidated files now exist
    assert!(
        remote_dir_ok.join("stream.mp4").exists(),
        "Remote stream.mp4 must exist after resumed upload"
    );
    assert!(
        remote_dir_ok.join("chat.jsonl").exists(),
        "Remote chat.jsonl must exist after resumed upload"
    );

    // Original chunks are purged on remote
    assert!(!ok_chunk0.exists(), "Original chunk must be purged");
    assert!(!ok_chat0.exists(), "Original chat must be purged");
    assert!(ok_meta.exists(), "Metadata must be preserved");

    // Local staging directory was completely unlinked
    assert!(
        !staging_parent.exists(),
        "Local staging directory must be unlinked upon successful completion"
    );
}

#[tokio::test]
async fn test_remote_consolidation_upload_cancellation_preserves_local_files() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_cancel");
    let staging_parent = create_temp_test_dir("test_remote_cons_cancel_staging");

    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_0").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    // Hang rclone copyto so upload stays in flight
    let remote_dest_hang = remote_dir.join("fail_copy_hang");
    fs::create_dir_all(&remote_dest_hang).unwrap();
    let remote_path = format!(
        "mock_remote:{}",
        remote_dest_hang.to_string_lossy().replace('\\', "/")
    );
    // Copy chunks into the hanging remote dir
    fs::copy(&chunk0, remote_dest_hang.join("chunk_0000.ts")).unwrap();
    fs::copy(&chat0, remote_dest_hang.join("chat_0000.jsonl")).unwrap();
    fs::copy(&meta, remote_dest_hang.join("metadata.jsonl")).unwrap();

    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
        config: None,
    };

    let handle = tokio::spawn(async move { run_consolidation(args).await });

    let local_mp4 = staging_parent.join("stream.mp4");
    let local_jsonl = staging_parent.join("chat.jsonl");

    // Wait until remuxing completes and files are finalized in staging
    let start = std::time::Instant::now();
    while (!local_mp4.exists() || !local_jsonl.exists())
        && start.elapsed() < std::time::Duration::from_secs(5)
    {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Abort task while upload is hanging in flight
    handle.abort();
    let _ = handle.await;

    // Completed staging files must be preserved in staging directory
    assert!(
        local_mp4.exists(),
        "Local stream.mp4 must be preserved upon upload cancellation"
    );
    assert!(
        local_jsonl.exists(),
        "Local chat.jsonl must be preserved upon upload cancellation"
    );
}

#[tokio::test]
async fn test_remote_staged_upload_high_throughput_args_and_custom_extra_args() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_rclone = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_upload_staged_args");
    let local_file = temp_dir.join("local.txt");
    let remote_dest_file = temp_dir.join("remote.txt");

    fs::write(&local_file, b"sample high throughput payload").unwrap();

    let remote_dest = format!(
        "mock_remote:{}",
        remote_dest_file.to_string_lossy().replace('\\', "/")
    );

    let extra_args = vec![
        "--low-level-retries".to_string(),
        "10".to_string(),
        "--fast-list".to_string(),
    ];

    // 1. Verify command construction via public Command builder
    let cmd = build_remote_staged_upload_command(
        mock_rclone.to_str().unwrap(),
        &local_file,
        &remote_dest,
        &extra_args,
    );
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "copyto");
    assert_eq!(args[1], local_file.to_string_lossy().to_string());
    assert_eq!(args[2], remote_dest);
    assert!(args.contains(&"--drive-chunk-size".to_string()));
    assert!(args.contains(&"128M".to_string()));
    assert!(args.contains(&"--s3-chunk-size".to_string()));
    assert!(args.contains(&"64M".to_string()));
    assert!(args.contains(&"--s3-upload-concurrency".to_string()));
    assert!(args.contains(&"4".to_string()));
    assert!(args.contains(&"--buffer-size".to_string()));
    assert!(args.contains(&"64M".to_string()));
    assert!(args.contains(&"--multi-thread-streams".to_string()));
    assert!(args.contains(&"4".to_string()));
    assert!(args.contains(&"--multi-thread-cutoff".to_string()));
    assert!(args.contains(&"250M".to_string()));
    assert!(args.contains(&"--low-level-retries".to_string()));
    assert!(args.contains(&"10".to_string()));
    assert!(args.contains(&"--fast-list".to_string()));

    // 2. Verify domain execution with mock rclone
    let res = upload_remote_staged_file_with_args(
        mock_rclone.to_str().unwrap(),
        &local_file,
        &remote_dest,
        &extra_args,
    )
    .await;

    assert!(res.is_ok(), "Upload should succeed: {:?}", res.err());
    assert_eq!(
        fs::read_to_string(&remote_dest_file).unwrap(),
        "sample high throughput payload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_with_custom_config_file_and_extra_args() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_cfg");
    let staging_parent = create_temp_test_dir("test_remote_cons_cfg_staging");
    let config_dir = create_temp_test_dir("test_remote_cons_cfg_dir");

    // Create remote chunks
    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_cfg").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
    fs::write(&meta, r#"{"type":"SESSION_START","stream_offset_ms":0}"#).unwrap();

    let remote_path = format!(
        "mock_remote:{}",
        remote_dir.to_string_lossy().replace('\\', "/")
    );

    // Create custom settings.toml file specifying consolidate_extra_args
    let config_file = config_dir.join("custom_settings.toml");
    let toml_content = r#"
[general]
recordings_dir = "recordings"

[rclone]
remote_path = "mock_remote:test"
rclone_bin = "mock_rclone"
consolidate_extra_args = ["--checkers", "12", "--stats", "500ms"]
"#;
    fs::write(&config_file, toml_content).unwrap();

    let args = ConsolidateArgs {
        path: remote_path,
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 16,
        temp_dir: Some(staging_parent.clone()),
        config: Some(config_file),
    };

    let summary = run_consolidation(args).await;
    let summary = summary.expect("run_consolidation with custom config should succeed");
    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // Domain outcome assertions
    assert!(
        remote_dir.join("stream.mp4").exists(),
        "Remote stream.mp4 must exist after finalization"
    );
    assert!(
        remote_dir.join("chat.jsonl").exists(),
        "Remote chat.jsonl must exist after finalization"
    );
    assert!(
        !remote_dir.join("stream.mp4.part").exists(),
        "Remote stream.mp4.part must be finalized"
    );
    assert!(
        !remote_dir.join("chat.jsonl.part").exists(),
        "Remote chat.jsonl.part must be finalized"
    );
    assert!(!chunk0.exists(), "Original chunk_0000.ts must be purged");
    assert!(!chat0.exists(), "Original chat_0000.jsonl must be purged");
    assert!(meta.exists(), "metadata.jsonl must be preserved");
    assert!(
        !staging_parent.exists(),
        "Staging directory must be unlinked post-upload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_without_config_uses_default_high_performance_args() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_default");
    let staging_parent = create_temp_test_dir("test_remote_cons_default_staging");

    // Create remote chunks
    let chunk0 = remote_dir.join("chunk_0000.ts");
    let chat0 = remote_dir.join("chat_0000.jsonl");
    let meta = remote_dir.join("metadata.jsonl");

    fs::write(&chunk0, b"mock_video_chunk_default").unwrap();
    let line1 = make_chat_line(10_000, "user1", "msg1");
    fs::write(&chat0, format!("{line1}\n")).unwrap();
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
        config: None,
    };

    let summary = run_consolidation(args).await;
    let summary = summary.expect("run_consolidation without config should succeed");
    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // Domain outcome assertions
    assert!(
        remote_dir.join("stream.mp4").exists(),
        "Remote stream.mp4 must exist after finalization"
    );
    assert!(
        remote_dir.join("chat.jsonl").exists(),
        "Remote chat.jsonl must exist after finalization"
    );
    assert!(
        !remote_dir.join("stream.mp4.part").exists(),
        "Remote stream.mp4.part must be finalized"
    );
    assert!(
        !remote_dir.join("chat.jsonl.part").exists(),
        "Remote chat.jsonl.part must be finalized"
    );
    assert!(!chunk0.exists(), "Original chunk_0000.ts must be purged");
    assert!(!chat0.exists(), "Original chat_0000.jsonl must be purged");
    assert!(meta.exists(), "metadata.jsonl must be preserved");
    assert!(
        !staging_parent.exists(),
        "Staging directory must be unlinked post-upload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_uploads_concurrently_parallel() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_cons_assert_parallel");
    let staging_parent = create_temp_test_dir("test_remote_cons_parallel_staging");

    let (chunk0, chat0, meta) = setup_test_stream_chunks(&remote_dir);

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
        config: None,
    };

    let summary = run_consolidation(args).await;
    let summary = summary.expect("run_consolidation with parallel upload should succeed");
    assert_eq!(summary.video_chunks_count, 1);
    assert_eq!(summary.chat_chunks_count, 1);

    // Verify remote final files and cleanup
    assert!(remote_dir.join("stream.mp4").exists());
    assert!(remote_dir.join("chat.jsonl").exists());
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());
    assert!(!chunk0.exists(), "Original chunk0 must be purged");
    assert!(!chat0.exists(), "Original chat0 must be purged");
    assert!(meta.exists(), "Metadata must be preserved");
    assert!(
        !staging_parent.exists(),
        "Staging directory must be unlinked post-upload"
    );
}

#[tokio::test]
async fn test_remote_consolidation_chat_failure_aborts_hanging_video_immediately() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_fail_hang_vid_and_fail_err_chat");
    let staging_parent = create_temp_test_dir("test_remote_cons_abort_staging");

    setup_test_stream_chunks(&remote_dir);

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
        config: None,
    };

    let start = std::time::Instant::now();
    let res = run_consolidation(args).await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Chat failure must fail consolidation");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Sibling video hanging process must be aborted immediately, but took {:?}",
        elapsed
    );

    // Verify remote .part files were removed
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

    // Verify finalized local staging files are preserved
    assert!(staging_parent.join("stream.mp4").exists());
    assert!(staging_parent.join("chat.jsonl").exists());
}

#[tokio::test]
async fn test_remote_consolidation_video_failure_aborts_hanging_chat_immediately() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_fail_hang_chat_and_fail_err_vid");
    let staging_parent = create_temp_test_dir("test_remote_cons_abort_vid_staging");

    setup_test_stream_chunks(&remote_dir);

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
        config: None,
    };

    let start = std::time::Instant::now();
    let res = run_consolidation(args).await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Video failure must fail consolidation");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Sibling chat hanging process must be aborted immediately, but took {:?}",
        elapsed
    );

    // Verify remote .part files were removed
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

    // Verify finalized local staging files are preserved
    assert!(staging_parent.join("stream.mp4").exists());
    assert!(staging_parent.join("chat.jsonl").exists());
}

#[tokio::test]
async fn test_remote_consolidation_cancellation_token_promptly_terminates_active_uploads() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let _mock_ffmpeg = get_mock_ffmpeg_bin();
    let _mock_rclone = get_mock_rclone_bin();

    let remote_dir = create_temp_test_dir("test_remote_fail_copy_hang_cancel");
    let staging_parent = create_temp_test_dir("test_remote_cons_cancel_tok_staging");

    setup_test_stream_chunks(&remote_dir);

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
        config: None,
    };

    let cancel_token = CancellationToken::new();
    let cancel_trigger = cancel_token.clone();

    // Trigger cancellation after 100ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel_trigger.cancel();
    });

    let start = std::time::Instant::now();
    let res = run_consolidation_with_cancel(args, cancel_token).await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Cancelled consolidation must return error");
    let err_msg = format!("{:#}", res.unwrap_err());
    assert!(
        err_msg.contains("cancelled by cooperative cancellation token"),
        "Error message should mention cancellation: {err_msg}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Cancellation must terminate active uploads promptly, but took {:?}",
        elapsed
    );

    // Remote .part files must be cleaned up
    assert!(!remote_dir.join("stream.mp4.part").exists());
    assert!(!remote_dir.join("chat.jsonl.part").exists());

    // Finalized local staging files must be preserved
    assert!(staging_parent.join("stream.mp4").exists());
    assert!(staging_parent.join("chat.jsonl").exists());
}

#[tokio::test]
async fn test_upload_remote_staged_media_seam_success_and_concurrency() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_rclone = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_seam_upload_assert_parallel");
    let staging_dir = temp_dir.join("staging");
    fs::create_dir_all(&staging_dir).unwrap();

    let local_mp4 = staging_dir.join("stream.mp4");
    let local_jsonl = staging_dir.join("chat.jsonl");
    fs::write(&local_mp4, b"test_stream_content").unwrap();
    fs::write(&local_jsonl, b"test_chat_content\n").unwrap();

    let remote_dest_dir = temp_dir.join("remote_target");
    fs::create_dir_all(&remote_dest_dir).unwrap();
    let remote_base = format!(
        "mock_remote:{}",
        remote_dest_dir.to_string_lossy().replace('\\', "/")
    );

    let cancel_token = CancellationToken::new();
    let res = upload_remote_staged_media(
        mock_rclone.to_str().unwrap(),
        &remote_base,
        &staging_dir,
        true,
        true,
        &[],
        cancel_token,
    )
    .await;

    assert!(
        res.is_ok(),
        "Concurrent upload must succeed: {:?}",
        res.err()
    );

    // Verify .part files exist on remote
    assert!(remote_dest_dir.join("stream.mp4.part").exists());
    assert!(remote_dest_dir.join("chat.jsonl.part").exists());

    // Verify local staging files were preserved
    assert!(local_mp4.exists());
    assert!(local_jsonl.exists());
}

#[tokio::test]
async fn test_upload_remote_staged_media_seam_sibling_abort_and_remote_cleanup() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_rclone = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_seam_fail_hang_vid_and_fail_err_chat");
    let staging_dir = temp_dir.join("staging");
    fs::create_dir_all(&staging_dir).unwrap();

    let local_mp4 = staging_dir.join("stream.mp4");
    let local_jsonl = staging_dir.join("chat.jsonl");
    fs::write(&local_mp4, b"test_stream_content").unwrap();
    fs::write(&local_jsonl, b"test_chat_content\n").unwrap();

    let remote_dest_dir = temp_dir.join("remote_target");
    fs::create_dir_all(&remote_dest_dir).unwrap();
    let remote_base = format!(
        "mock_remote:{}",
        remote_dest_dir.to_string_lossy().replace('\\', "/")
    );

    let cancel_token = CancellationToken::new();
    let start = std::time::Instant::now();
    let res = upload_remote_staged_media(
        mock_rclone.to_str().unwrap(),
        &remote_base,
        &staging_dir,
        true,
        true,
        &[],
        cancel_token,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Upload must fail due to chat failure");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Sibling video hanging process must be aborted immediately, but took {:?}",
        elapsed
    );

    // Verify remote .part files were removed via rclone deletefile
    assert!(!remote_dest_dir.join("stream.mp4.part").exists());
    assert!(!remote_dest_dir.join("chat.jsonl.part").exists());

    // Verify local staging files remain intact
    assert!(local_mp4.exists());
    assert!(local_jsonl.exists());
}

#[tokio::test]
async fn test_upload_remote_staged_media_seam_cancellation_and_remote_cleanup() {
    let _lock = REMOTE_TEST_MUTEX.lock().await;
    let mock_rclone = get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_seam_fail_copy_hang_cancel");
    let staging_dir = temp_dir.join("staging");
    fs::create_dir_all(&staging_dir).unwrap();

    let local_mp4 = staging_dir.join("stream.mp4");
    let local_jsonl = staging_dir.join("chat.jsonl");
    fs::write(&local_mp4, b"test_stream_content").unwrap();
    fs::write(&local_jsonl, b"test_chat_content\n").unwrap();

    let remote_dest_dir = temp_dir.join("remote_target");
    fs::create_dir_all(&remote_dest_dir).unwrap();
    let remote_base = format!(
        "mock_remote:{}",
        remote_dest_dir.to_string_lossy().replace('\\', "/")
    );

    let cancel_token = CancellationToken::new();
    let trigger = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        trigger.cancel();
    });

    let start = std::time::Instant::now();
    let res = upload_remote_staged_media(
        mock_rclone.to_str().unwrap(),
        &remote_base,
        &staging_dir,
        true,
        true,
        &[],
        cancel_token,
    )
    .await;
    let elapsed = start.elapsed();

    assert!(res.is_err(), "Cancelled upload must return error");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Cancellation must terminate uploads promptly, but took {:?}",
        elapsed
    );

    // Verify remote .part files were cleaned up
    assert!(!remote_dest_dir.join("stream.mp4.part").exists());
    assert!(!remote_dest_dir.join("chat.jsonl.part").exists());

    // Verify local staging files remain intact
    assert!(local_mp4.exists());
    assert!(local_jsonl.exists());
}
