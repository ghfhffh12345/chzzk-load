use chzzk_load::consolidation::manifest::{ConsolidationChunk, TargetLocation};
use chzzk_load::consolidation::video::{
    VideoConsolidationOptions, build_ffmpeg_remux_args, build_ffmpeg_remux_command,
    build_local_concat_ffmpeg_args, build_local_concat_ffmpeg_command, cleanup_staged_video,
    consolidate_video, feed_video_chunks, finalize_staged_video, join_remote_path,
    resolve_ffmpeg_bin,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tokio_util::sync::CancellationToken;

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

fn ensure_ffprobe_available() -> bool {
    Command::new("ffprobe")
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{}_{}", prefix, rand::random::<u32>()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn inspect_mp4_box_order(path: &std::path::Path) -> Vec<String> {
    let bytes = fs::read(path).expect("read mp4 file");
    let mut offset = 0;
    let mut boxes = Vec::new();
    while offset + 8 <= bytes.len() {
        let size = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let box_type = std::str::from_utf8(&bytes[offset + 4..offset + 8])
            .unwrap_or("????")
            .to_string();
        boxes.push(box_type);
        if size == 0 || size > bytes.len() - offset {
            break;
        }
        offset += size;
    }
    boxes
}

fn probe_video_frame_count(path: &std::path::Path) -> u64 {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-count_frames",
            "-show_entries",
            "stream=nb_read_frames",
            "-of",
            "default=nokey=1:noprint_wrappers=1",
            path.to_str().unwrap(),
        ])
        .output()
        .expect("probe frame count");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .map(|l| l.trim())
        .find(|l| !l.is_empty() && *l != "N/A")
        .and_then(|l| l.parse::<u64>().ok())
        .expect("parse frame count")
}

fn probe_video_packet_count(path: &std::path::Path) -> u64 {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-count_packets",
            "-show_entries",
            "stream=nb_read_packets",
            "-of",
            "default=nokey=1:noprint_wrappers=1",
            path.to_str().unwrap(),
        ])
        .output()
        .expect("probe packet count");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .map(|l| l.trim())
        .find(|l| !l.is_empty() && *l != "N/A")
        .and_then(|l| l.parse::<u64>().ok())
        .expect("parse packet count")
}

fn probe_format_start_time(path: &std::path::Path) -> f64 {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=start_time",
            "-of",
            "default=nokey=1:noprint_wrappers=1",
            path.to_str().unwrap(),
        ])
        .output()
        .expect("probe start time");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .map(|l| l.trim())
        .find(|l| !l.is_empty() && *l != "N/A")
        .and_then(|l| l.parse::<f64>().ok())
        .expect("parse start time")
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
    assert!(args.contains(&"-bsf:a".to_string()));
    assert!(args.contains(&"aac_adtstoasc".to_string()));

    // Verify progress telemetry flags
    assert!(args.contains(&"-progress".to_string()));
    assert!(args.contains(&"pipe:2".to_string()));

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
fn test_build_local_concat_ffmpeg_args_contains_required_flags() {
    let manifest_path = PathBuf::from("temp/concat_manifest.txt");
    let output_path = PathBuf::from("recordings/consolidated.mp4.part");
    let args = build_local_concat_ffmpeg_args(&manifest_path, &output_path);

    // Verify Concat Demuxer input flags
    assert!(args.contains(&"-safe".to_string()));
    assert!(args.contains(&"0".to_string()));
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"concat".to_string()));
    assert!(args.contains(&"-i".to_string()));
    let i_pos = args.iter().position(|a| a == "-i").expect("-i present");
    assert_eq!(args[i_pos + 1], manifest_path.to_string_lossy().to_string());

    // Verify lossless copy & ADTS-to-ASC bitstream filter
    assert!(args.contains(&"-c".to_string()));
    assert!(args.contains(&"copy".to_string()));
    assert!(args.contains(&"-bsf:a".to_string()));
    assert!(args.contains(&"aac_adtstoasc".to_string()));

    // Verify timestamp normalization & faststart seek table
    assert!(args.contains(&"-avoid_negative_ts".to_string()));
    assert!(args.contains(&"make_zero".to_string()));
    assert!(args.contains(&"-movflags".to_string()));
    assert!(args.contains(&"+faststart".to_string()));

    // Verify progress telemetry
    assert!(args.contains(&"-progress".to_string()));
    assert!(args.contains(&"pipe:2".to_string()));

    // Verify output destination is staged file directly with mp4 container format
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"mp4".to_string()));
    assert_eq!(
        args.last().unwrap(),
        &output_path.to_string_lossy().to_string()
    );

    // Verify command builder produces configured command with null stdin
    let cmd = build_local_concat_ffmpeg_command("test_ffmpeg", &manifest_path, &output_path);
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
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(
        &target,
        &[],
        &VideoConsolidationOptions::default(),
        cancel_token,
    )
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
    let cancel_token = CancellationToken::new();

    let err = consolidate_video(&target, &chunks, &options, cancel_token)
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
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=0.5",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
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
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=0.5",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
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
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Consolidation with real ffmpeg must succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let part_mp4 = temp_dir.join("consolidated.mp4.part");
    let final_mp4 = temp_dir.join("consolidated.mp4");
    assert!(
        part_mp4.exists(),
        "Staged .part file must exist before finalization"
    );
    assert!(
        !final_mp4.exists(),
        "Final consolidated.mp4 must not exist before finalization"
    );

    // Atomically finalize staged video
    finalize_staged_video(&target, None)
        .await
        .expect("finalize_staged_video must succeed");

    assert!(final_mp4.exists(), "Final consolidated.mp4 must exist");
    assert!(!part_mp4.exists(), "Staged .part file must be renamed");
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
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Remote video consolidation should succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let part_mp4 = temp_dir.join("consolidated.mp4.part");
    let final_mp4 = temp_dir.join("consolidated.mp4");
    assert!(
        part_mp4.exists(),
        "Staged .part file must exist on remote before finalization"
    );
    assert!(
        !final_mp4.exists(),
        "Final file must not exist on remote before finalization"
    );

    // Atomically finalize staged video on remote
    finalize_staged_video(&target, None)
        .await
        .expect("Remote finalize_staged_video must succeed");

    assert!(
        final_mp4.exists(),
        "Final consolidated.mp4 must exist on remote"
    );
    assert!(
        !part_mp4.exists(),
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
    let cancel_token = CancellationToken::new();

    let err = consolidate_video(&target, &chunks, &options, cancel_token)
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

#[tokio::test]
async fn test_consolidate_video_cooperative_cancellation() {
    let temp_dir = create_temp_test_dir("test_cons_vid_cancel");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    tokio::fs::write(&chunk0, b"video-cancellation-chunk")
        .await
        .unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chunk_0000.ts".to_string(),
        size: 24,
    }];

    let target = TargetLocation::Local(temp_dir.clone());
    let options = VideoConsolidationOptions::default();
    let cancel_token = CancellationToken::new();
    cancel_token.cancel(); // Cancel immediately prior to execution

    let result = consolidate_video(&target, &chunks, &options, cancel_token).await;
    assert!(
        result.is_err(),
        "Cancelled video consolidation must fail with error"
    );

    assert!(
        !temp_dir.join("consolidated.mp4.part").exists(),
        "Staged .part file must be cleaned up on cancellation"
    );
    assert!(
        !temp_dir.join("consolidated.mp4").exists(),
        "Final file must not exist on cancellation"
    );
    assert!(
        chunk0.exists(),
        "Original chunk must remain untouched on cancellation"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_local_faststart_seek_table_and_preserved_frames() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping faststart & preserved frames test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_faststart");

    // Generate 2 valid synthetic MPEG-TS chunks without B-frames (-bf 0): 2.0s each at 10fps (20 frames each = 40 frames total)
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    let status0 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=2.0:size=160x120:rate=10",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=2.0",
            "-c:v",
            "libx264",
            "-bf",
            "0",
            "-c:a",
            "aac",
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
            "testsrc=duration=2.0:size=160x120:rate=10",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=2.0",
            "-c:v",
            "libx264",
            "-bf",
            "0",
            "-c:a",
            "aac",
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

    let has_ffprobe = ensure_ffprobe_available();
    if has_ffprobe {
        // Verify source chunk frames and packets before consolidation
        assert_eq!(probe_video_frame_count(&chunk0), 20);
        assert_eq!(probe_video_frame_count(&chunk1), 20);
        assert_eq!(probe_video_packet_count(&chunk0), 20);
        assert_eq!(probe_video_packet_count(&chunk1), 20);
    }

    let target = TargetLocation::Local(temp_dir.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let options = VideoConsolidationOptions::default().with_progress_sender(tx);
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Consolidation with real ffmpeg must succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    // Drain progress updates
    let mut received_chunk_fed = Vec::new();
    while let Ok(update) = rx.try_recv() {
        if let chzzk_load::consolidation::progress::VideoProgressUpdate::ChunkFed {
            chunks_fed,
            bytes_fed,
        } = update
        {
            received_chunk_fed.push((chunks_fed, bytes_fed));
        }
    }
    assert!(
        !received_chunk_fed.is_empty(),
        "Must receive progress ChunkFed updates"
    );
    let last_chunk_fed = received_chunk_fed.last().unwrap();
    assert_eq!(
        last_chunk_fed.0, 2,
        "Final chunks_fed must be total chunks count"
    );
    assert!(last_chunk_fed.1 > 0, "Final bytes_fed must be positive");

    let part_mp4 = temp_dir.join("consolidated.mp4.part");
    let final_mp4 = temp_dir.join("consolidated.mp4");
    assert!(
        part_mp4.exists(),
        "Staged .part file must exist before finalization"
    );
    assert!(
        !final_mp4.exists(),
        "Final .mp4 must not exist before finalization"
    );

    // Finalize staged video
    finalize_staged_video(&target, None)
        .await
        .expect("finalize_staged_video must succeed");

    assert!(final_mp4.exists(), "Final consolidated.mp4 must exist");
    assert!(!part_mp4.exists(), "Staged .part file must no longer exist");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original chunks must remain untouched"
    );

    // 1. Verify faststart seek table: moov atom MUST appear before mdat atom
    let boxes = inspect_mp4_box_order(&final_mp4);
    let moov_pos = boxes
        .iter()
        .position(|b| b == "moov")
        .expect("moov box present");
    let mdat_pos = boxes
        .iter()
        .position(|b| b == "mdat")
        .expect("mdat box present");
    assert!(
        moov_pos < mdat_pos,
        "moov atom (index {moov_pos}) must appear before mdat atom (index {mdat_pos}) for instant faststart seeking"
    );

    // 2. If ffprobe is available, verify frame preservation, zero boundary packet drops, and start time
    if has_ffprobe {
        let final_frames = probe_video_frame_count(&final_mp4);
        assert_eq!(
            final_frames, 40,
            "Must preserve 100% of video frames (20 + 20 = 40) across chunk seams"
        );

        let final_packets = probe_video_packet_count(&final_mp4);
        assert_eq!(
            final_packets, 40,
            "Must preserve all video packets with zero dropped boundary packets"
        );

        let format_start = probe_format_start_time(&final_mp4);
        assert!(
            (format_start - 0.0).abs() < 0.001,
            "Container start time must be normalized to 0.0s (got {format_start})"
        );
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_local_active_cancellation_cleans_up_part() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping active cancellation test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_active_cancel");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    let status0 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=4.0:size=320x240:rate=30",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=4.0",
            "-c:v",
            "libx264",
            "-bf",
            "0",
            "-c:a",
            "aac",
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
            "testsrc=duration=4.0:size=320x240:rate=30",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=4.0",
            "-c:v",
            "libx264",
            "-bf",
            "0",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            chunk1.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk1");
    assert!(status1.status.success());

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chunk_0000.ts".to_string(),
            size: fs::metadata(&chunk0).unwrap().len(),
        },
        ConsolidationChunk {
            index: 1,
            name: "chunk_0001.ts".to_string(),
            size: fs::metadata(&chunk1).unwrap().len(),
        },
    ];

    let target = TargetLocation::Local(temp_dir.clone());
    let options = VideoConsolidationOptions::default();
    let cancel_token = CancellationToken::new();

    let cancel_token_clone = cancel_token.clone();
    let target_clone = target.clone();
    let chunks_clone = chunks.clone();

    let handle = tokio::spawn(async move {
        consolidate_video(&target_clone, &chunks_clone, &options, cancel_token_clone).await
    });

    // Let FFmpeg start, then cooperatively cancel
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    cancel_token.cancel();

    let result = handle.await.unwrap();
    assert!(result.is_err(), "Active cancellation must return error");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("cancelled by cooperative cancellation token")
    );

    let staged_file = temp_dir.join("consolidated.mp4.part");
    let final_file = temp_dir.join("consolidated.mp4");
    assert!(
        !staged_file.exists(),
        "Staged .part file must be cleaned up on active cancellation"
    );
    assert!(!final_file.exists(), "Final file must not exist");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original chunks must remain untouched on active cancellation"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_local_path_with_korean_and_special_characters() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping special characters test");
        return;
    }

    let parent_dir = std::env::temp_dir();
    let special_name = format!(
        "test_cons_[2026-10-09]_[스트리머]_방송's_test_{}",
        rand::random::<u32>()
    );
    let temp_dir = parent_dir.join(special_name);
    fs::create_dir_all(&temp_dir).expect("create special dir");

    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    let status0 = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=0.5:size=160x120:rate=10",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=0.5",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
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
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:duration=0.5",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            chunk1.to_str().unwrap(),
        ])
        .output()
        .expect("generate chunk1");
    assert!(status1.status.success());

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chunk_0000.ts".to_string(),
            size: fs::metadata(&chunk0).unwrap().len(),
        },
        ConsolidationChunk {
            index: 1,
            name: "chunk_0001.ts".to_string(),
            size: fs::metadata(&chunk1).unwrap().len(),
        },
    ];

    let target = TargetLocation::Local(temp_dir.clone());
    let options = VideoConsolidationOptions::default();
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Consolidation in special character directory must succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let final_mp4 = temp_dir.join("consolidated.mp4");
    finalize_staged_video(&target, None)
        .await
        .expect("finalize_staged_video must succeed");

    assert!(final_mp4.exists());
    let boxes = inspect_mp4_box_order(&final_mp4);
    let moov_pos = boxes
        .iter()
        .position(|b| b == "moov")
        .expect("moov box present");
    let mdat_pos = boxes
        .iter()
        .position(|b| b == "mdat")
        .expect("mdat box present");
    assert!(moov_pos < mdat_pos);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
