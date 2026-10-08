use chzzk_load::consolidation::manifest::{ConsolidationChunk, TargetLocation};
use chzzk_load::consolidation::video::{
    VideoConsolidationOptions, build_ffmpeg_remux_args, build_ffmpeg_remux_command,
    cleanup_staged_video, consolidate_video, feed_video_chunks, finalize_staged_video,
    join_remote_path, resolve_ffmpeg_bin,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn ensure_ffmpeg_available() -> bool {
    let bin = std::env::var("CHZZK_LOAD_FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());
    Command::new(&bin)
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn ensure_rclone_available() -> bool {
    let bin = std::env::var("CHZZK_LOAD_RCLONE_BIN").unwrap_or_else(|_| "rclone".to_string());
    Command::new(&bin)
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{}_{}", prefix, rand::random::<u32>()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn test_ffmpeg_bin_resolution() {
    // 1. Direct override takes highest priority
    let resolved = resolve_ffmpeg_bin(Some("custom_ffmpeg"));
    assert_eq!(resolved, "custom_ffmpeg");

    // 2. Fallback to default "ffmpeg" when env var not set or empty
    let original_env = std::env::var("CHZZK_LOAD_FFMPEG_BIN").ok();
    unsafe {
        std::env::remove_var("CHZZK_LOAD_FFMPEG_BIN");
    }
    let fallback = resolve_ffmpeg_bin(None);
    assert_eq!(fallback, "ffmpeg");

    // 3. Env var takes priority over fallback
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", "env_ffmpeg");
    }
    let from_env = resolve_ffmpeg_bin(None);
    assert_eq!(from_env, "env_ffmpeg");

    // Restore original env
    unsafe {
        if let Some(val) = original_env {
            std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", val);
        } else {
            std::env::remove_var("CHZZK_LOAD_FFMPEG_BIN");
        }
    }
}

#[test]
fn test_build_ffmpeg_remux_args_contains_required_flags() {
    let args = build_ffmpeg_remux_args();

    // Verify required remuxing flags per ADR 0009 and Ticket #24
    assert!(args.contains(&"-c".to_string()));
    assert!(args.contains(&"copy".to_string()));
    assert!(args.contains(&"-movflags".to_string()));
    assert!(args.contains(&"frag_keyframe+empty_moov".to_string()));
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"mp4".to_string()));
    assert!(args.contains(&"-fflags".to_string()));
    assert!(args.contains(&"+genpts+discardcorrupt".to_string()));

    // Verify stdin input and stdout pipe destination
    assert!(args.contains(&"-i".to_string()));
    assert!(args.contains(&"pipe:0".to_string()) || args.contains(&"-".to_string()));
    assert!(args.contains(&"pipe:1".to_string()) || args.contains(&"-".to_string()));

    // Verify command builder produces configured command
    let cmd = build_ffmpeg_remux_command("test_ffmpeg");
    let program = cmd.as_std().get_program().to_string_lossy();
    assert_eq!(program, "test_ffmpeg");
}

#[test]
fn test_join_remote_path_formatting() {
    assert_eq!(
        join_remote_path("gdrive:archive", "chunk_0000.ts"),
        "gdrive:archive/chunk_0000.ts"
    );
    assert_eq!(
        join_remote_path("gdrive:archive/", "chunk_0000.ts"),
        "gdrive:archive/chunk_0000.ts"
    );
    assert_eq!(
        join_remote_path("gdrive:archive///", "/chunk_0000.ts"),
        "gdrive:archive/chunk_0000.ts"
    );
}

#[tokio::test]
async fn test_staged_video_atomic_finalization_local() {
    let temp_dir = create_temp_test_dir("test_finalize_local");
    let staged_file = temp_dir.join("consolidated.mp4.part");
    let final_file = temp_dir.join("consolidated.mp4");

    tokio::fs::write(&staged_file, b"test-mp4-payload")
        .await
        .unwrap();
    assert!(staged_file.exists());
    assert!(!final_file.exists());

    let target = TargetLocation::Local(temp_dir.clone());
    finalize_staged_video(&target, None)
        .await
        .expect("Atomic finalization should succeed");

    assert!(!staged_file.exists(), ".part file must no longer exist");
    assert!(final_file.exists(), "Final .mp4 file must exist");
    let content = tokio::fs::read(&final_file).await.unwrap();
    assert_eq!(content, b"test-mp4-payload");

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_staged_video_cleanup_on_abort_local() {
    let temp_dir = create_temp_test_dir("test_cleanup_local");
    let staged_file = temp_dir.join("consolidated.mp4.part");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    tokio::fs::write(&staged_file, b"partial-garbage")
        .await
        .unwrap();
    tokio::fs::write(&chunk0, b"original-chunk-0")
        .await
        .unwrap();
    tokio::fs::write(&chunk1, b"original-chunk-1")
        .await
        .unwrap();

    let target = TargetLocation::Local(temp_dir.clone());
    cleanup_staged_video(&target, None)
        .await
        .expect("Cleanup of staged file should succeed");

    assert!(
        !staged_file.exists(),
        ".part file must be removed on cleanup"
    );
    assert!(
        chunk0.exists(),
        "Original chunk 0 must be strictly preserved"
    );
    assert!(
        chunk1.exists(),
        "Original chunk 1 must be strictly preserved"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_feed_video_chunks_local_streaming() {
    let temp_dir = create_temp_test_dir("test_feed_local");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");
    let chunk2 = temp_dir.join("chunk_0002.ts");

    let data0 = vec![0x47, 0x01, 0x02, 0x03];
    let data1 = vec![0x47, 0x11, 0x12, 0x13, 0x14];
    let data2 = vec![0x47, 0x21, 0x22];

    tokio::fs::write(&chunk0, &data0).await.unwrap();
    tokio::fs::write(&chunk1, &data1).await.unwrap();
    tokio::fs::write(&chunk2, &data2).await.unwrap();

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chunk_0000.ts".to_string(),
            size: data0.len() as u64,
        },
        ConsolidationChunk {
            index: 1,
            name: "chunk_0001.ts".to_string(),
            size: data1.len() as u64,
        },
        ConsolidationChunk {
            index: 2,
            name: "chunk_0002.ts".to_string(),
            size: data2.len() as u64,
        },
    ];

    let target = TargetLocation::Local(temp_dir.clone());
    let mut buffer = Vec::new();

    let total_bytes = feed_video_chunks(&target, &chunks, None, &mut buffer)
        .await
        .expect("Streaming chunks into buffer should succeed");

    let mut expected = Vec::new();
    expected.extend_from_slice(&data0);
    expected.extend_from_slice(&data1);
    expected.extend_from_slice(&data2);

    assert_eq!(total_bytes, expected.len() as u64);
    assert_eq!(buffer, expected);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_feed_video_chunks_missing_file_errors() {
    let temp_dir = create_temp_test_dir("test_feed_missing");
    let target = TargetLocation::Local(temp_dir.clone());

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chunk_0000.ts".to_string(),
        size: 100,
    }];

    let mut buffer = Vec::new();
    let err = feed_video_chunks(&target, &chunks, None, &mut buffer)
        .await
        .expect_err("Feeding missing chunk must return error");

    assert!(err.to_string().contains("chunk_0000.ts"));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_empty_chunks_noop() {
    let temp_dir = create_temp_test_dir("test_consolidate_empty");
    let target = TargetLocation::Local(temp_dir.clone());

    let result = consolidate_video(&target, &[], &VideoConsolidationOptions::default())
        .await
        .expect("Empty chunks consolidation should succeed as a no-op");

    assert_eq!(result.chunks_processed, 0);
    assert_eq!(result.bytes_written, 0);
    assert!(!temp_dir.join("consolidated.mp4.part").exists());
    assert!(!temp_dir.join("consolidated.mp4").exists());

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_failure_cleans_up_part_and_preserves_chunks() {
    let temp_dir = create_temp_test_dir("test_cons_fail");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    tokio::fs::write(&chunk0, b"not-a-valid-ts-stream")
        .await
        .unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chunk_0000.ts".to_string(),
        size: 21,
    }];

    let target = TargetLocation::Local(temp_dir.clone());
    // Use an invalid binary to guarantee failure
    let options =
        VideoConsolidationOptions::default().with_ffmpeg_bin("nonexistent_ffmpeg_bin_xyz");

    let err = consolidate_video(&target, &chunks, &options)
        .await
        .expect_err("Consolidation with nonexistent ffmpeg binary must fail");

    assert!(err.to_string().contains("nonexistent_ffmpeg_bin_xyz"));

    assert!(
        !temp_dir.join("consolidated.mp4.part").exists(),
        ".part file must be cleaned up on failure"
    );
    assert!(
        chunk0.exists(),
        "Original chunk must remain untouched after failure"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_local_with_real_ffmpeg() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping real remux test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_real_ffmpeg");

    // Generate 2 valid synthetic MPEG-TS chunks using real FFmpeg
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    let status0 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=0.5:size=160x120:rate=10",
            "-c:v",
            "libx264",
            "-f",
            "mpegts",
            chunk0.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk0");
    assert!(status0.status.success(), "Failed to generate chunk0");

    let status1 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=0.5:size=160x120:rate=10",
            "-c:v",
            "libx264",
            "-f",
            "mpegts",
            chunk1.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk1");
    assert!(status1.status.success(), "Failed to generate chunk1");

    let size0 = fs::metadata(&chunk0).unwrap().len();
    let size1 = fs::metadata(&chunk1).unwrap().len();

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chunk_0000.ts".to_string(),
            size: size0,
        },
        ConsolidationChunk {
            index: 1,
            name: "chunk_0001.ts".to_string(),
            size: size1,
        },
    ];

    let target = TargetLocation::Local(temp_dir.clone());
    let options = VideoConsolidationOptions::default();

    let result = consolidate_video(&target, &chunks, &options)
        .await
        .expect("Consolidation with real ffmpeg must succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let final_mp4 = temp_dir.join("consolidated.mp4");
    assert!(final_mp4.exists(), "Final consolidated.mp4 must exist");
    assert!(
        !temp_dir.join("consolidated.mp4.part").exists(),
        "Staged .part file must be renamed"
    );
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original chunks must remain untouched during video consolidation"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_staged_video_atomic_finalization_remote() {
    if !ensure_rclone_available() {
        eprintln!("[SKIP] rclone not available, skipping remote finalization test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_finalize_remote");
    let staged_file = temp_dir.join("consolidated.mp4.part");
    let final_file = temp_dir.join("consolidated.mp4");

    tokio::fs::write(&staged_file, b"remote-mp4-payload")
        .await
        .unwrap();

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_name = format!("tstremfin{}", rand::random::<u16>());
    unsafe {
        std::env::set_var(
            format!("RCLONE_CONFIG_{}_TYPE", remote_name.to_ascii_uppercase()),
            "local",
        );
    }

    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    finalize_staged_video(&target, None)
        .await
        .expect("Remote finalization should succeed");

    assert!(!staged_file.exists(), ".part file must be moved/renamed");
    assert!(final_file.exists(), "Final file must exist");
    let content = tokio::fs::read(&final_file).await.unwrap();
    assert_eq!(content, b"remote-mp4-payload");

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_staged_video_cleanup_on_abort_remote() {
    if !ensure_rclone_available() {
        eprintln!("[SKIP] rclone not available, skipping remote cleanup test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cleanup_remote");
    let staged_file = temp_dir.join("consolidated.mp4.part");
    let chunk0 = temp_dir.join("chunk_0000.ts");

    tokio::fs::write(&staged_file, b"remote-garbage")
        .await
        .unwrap();
    tokio::fs::write(&chunk0, b"original-remote-chunk")
        .await
        .unwrap();

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_name = format!("tstremcln{}", rand::random::<u16>());
    unsafe {
        std::env::set_var(
            format!("RCLONE_CONFIG_{}_TYPE", remote_name.to_ascii_uppercase()),
            "local",
        );
    }

    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    cleanup_staged_video(&target, None)
        .await
        .expect("Remote cleanup should succeed");

    assert!(!staged_file.exists(), ".part file must be removed");
    assert!(chunk0.exists(), "Original chunk must remain untouched");

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_with_rclone() {
    if !ensure_ffmpeg_available() || !ensure_rclone_available() {
        eprintln!("[SKIP] ffmpeg or rclone not available, skipping remote consolidation test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_remote");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    // Generate 2 valid synthetic MPEG-TS chunks
    let status0 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=0.5:size=160x120:rate=10",
            "-c:v",
            "libx264",
            "-f",
            "mpegts",
            chunk0.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk0");
    assert!(status0.status.success());

    let status1 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=0.5:size=160x120:rate=10",
            "-c:v",
            "libx264",
            "-f",
            "mpegts",
            chunk1.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk1");
    assert!(status1.status.success());

    let size0 = fs::metadata(&chunk0).unwrap().len();
    let size1 = fs::metadata(&chunk1).unwrap().len();

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chunk_0000.ts".to_string(),
            size: size0,
        },
        ConsolidationChunk {
            index: 1,
            name: "chunk_0001.ts".to_string(),
            size: size1,
        },
    ];

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_name = format!("tstremcons{}", rand::random::<u16>());
    unsafe {
        std::env::set_var(
            format!("RCLONE_CONFIG_{}_TYPE", remote_name.to_ascii_uppercase()),
            "local",
        );
    }

    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    let options = VideoConsolidationOptions::default();

    let result = consolidate_video(&target, &chunks, &options)
        .await
        .expect("Remote video consolidation should succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let final_mp4 = temp_dir.join("consolidated.mp4");
    assert!(
        final_mp4.exists(),
        "Final consolidated.mp4 must exist on remote"
    );
    assert!(
        !temp_dir.join("consolidated.mp4.part").exists(),
        "Staged .part file must be removed/renamed"
    );
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original remote chunks must remain untouched"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_failure_cleans_up_part() {
    if !ensure_rclone_available() {
        eprintln!("[SKIP] rclone not available, skipping remote failure test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_rem_fail");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    tokio::fs::write(&chunk0, b"corrupted-ts-payload")
        .await
        .unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chunk_0000.ts".to_string(),
        size: 20,
    }];

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_name = format!("tstremfail{}", rand::random::<u16>());
    unsafe {
        std::env::set_var(
            format!("RCLONE_CONFIG_{}_TYPE", remote_name.to_ascii_uppercase()),
            "local",
        );
    }

    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    let options =
        VideoConsolidationOptions::default().with_ffmpeg_bin("nonexistent_ffmpeg_bin_abc");

    let err = consolidate_video(&target, &chunks, &options)
        .await
        .expect_err("Remote consolidation with invalid ffmpeg must fail");

    assert!(err.to_string().contains("nonexistent_ffmpeg_bin_abc"));
    assert!(
        !temp_dir.join("consolidated.mp4.part").exists(),
        ".part file must be cleaned up on remote failure"
    );
    assert!(
        chunk0.exists(),
        "Original chunk must remain untouched after remote failure"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
