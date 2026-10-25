mod common;

use chzzk_load::consolidation::manifest::{ConsolidationChunk, TargetLocation};
use chzzk_load::consolidation::video::{
    AUDIO_MONOTONIC_DTS_BSF, VIDEO_MONOTONIC_DTS_BSF, VideoConsolidationOptions,
    build_local_concat_ffmpeg_args, build_local_concat_ffmpeg_command,
    build_remote_concat_ffmpeg_args, build_remote_concat_ffmpeg_command, cleanup_staged_video,
    consolidate_video, consolidate_video_local, consolidate_video_remote, finalize_staged_video,
    join_remote_path, resolve_ffmpeg_bin,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

static VIDEO_REMOTE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn assert_staging_zero_ts_and_faststart(
    staging_dir: &std::path::Path,
    mp4_path: &std::path::Path,
) {
    let mut entries = tokio::fs::read_dir(staging_dir)
        .await
        .expect("read staging dir");
    let mut found_ts = false;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.file_name().to_string_lossy().ends_with(".ts") {
            found_ts = true;
        }
    }
    assert!(
        !found_ts,
        "0x chunk storage: No .ts chunks should be written to local staging directory"
    );

    let boxes = inspect_mp4_box_order(mp4_path);
    assert_eq!(boxes[0], "ftyp", "First atom must be ftyp");
    assert_eq!(
        boxes[1], "moov",
        "Second atom must be moov (+faststart centralized seek table)"
    );
    assert!(
        !boxes.contains(&"moof".to_string()),
        "Output must be seekable MP4, not fragmented with moof: {boxes:?}"
    );
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PacketTimestamps {
    dts: i64,
    pts: i64,
}

fn probe_video_packets_dts_and_pts(path: &std::path::Path) -> Vec<PacketTimestamps> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "packet=pts,dts",
            "-of",
            "csv=p=0",
            path.to_str().unwrap(),
        ])
        .output()
        .expect("probe dts and pts");
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .filter_map(|l| {
            let trimmed = l.trim().trim_end_matches(',');
            let parts: Vec<&str> = trimmed.split(',').collect();
            if parts.len() >= 2 {
                let pts = parts[0].parse::<i64>().ok()?;
                let dts = parts[1].parse::<i64>().ok()?;
                Some(PacketTimestamps { dts, pts })
            } else {
                None
            }
        })
        .collect()
}

/// Verifies defensive stream mapping, discardcorrupt fflags, and monotonic bitstream filter parity.
fn assert_concat_stream_and_bsf_args(args: &[String], i_pos: usize) {
    // Verify explicit stream mapping immediately follows input manifest
    assert_eq!(args[i_pos + 2], "-map");
    assert_eq!(args[i_pos + 3], "0:v:0");
    assert_eq!(args[i_pos + 4], "-map");
    assert_eq!(args[i_pos + 5], "0:a:0?");

    // Verify fflags discards corrupt packets without synthesized +genpts
    let fflags_pos = args
        .iter()
        .position(|a| a == "-fflags")
        .expect("-fflags present");
    assert_eq!(args[fflags_pos + 1], "+discardcorrupt");
    assert!(!args.iter().any(|a| a.contains("genpts")));

    // Verify lossless copy & bitstream filters
    assert!(args.contains(&"-c".to_string()));
    assert!(args.contains(&"copy".to_string()));
    let bsf_v_pos = args
        .iter()
        .position(|a| a == "-bsf:v")
        .expect("-bsf:v present");
    assert_eq!(args[bsf_v_pos + 1], VIDEO_MONOTONIC_DTS_BSF);
    let bsf_a_pos = args
        .iter()
        .position(|a| a == "-bsf:a")
        .expect("-bsf:a present");
    assert_eq!(args[bsf_a_pos + 1], AUDIO_MONOTONIC_DTS_BSF);
}

#[test]
fn test_ffmpeg_bin_resolution() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.blocking_lock();
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
fn test_build_local_concat_ffmpeg_args_contains_required_flags() {
    let manifest_path = PathBuf::from("temp/concat_manifest.txt");
    let output_path = PathBuf::from("recordings/livestream.mp4.part");
    let args = build_local_concat_ffmpeg_args(&manifest_path, &output_path);

    // Verify Concat Demuxer input flags
    assert!(args.contains(&"-safe".to_string()));
    assert!(args.contains(&"0".to_string()));
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"concat".to_string()));
    assert!(args.contains(&"-i".to_string()));
    let i_pos = args.iter().position(|a| a == "-i").expect("-i present");
    assert_eq!(args[i_pos + 1], manifest_path.to_string_lossy().to_string());

    // Verify stream mapping and monotonic bitstream filters
    assert_concat_stream_and_bsf_args(&args, i_pos);

    // Verify timestamp normalization & faststart seek table
    assert!(args.contains(&"-avoid_negative_ts".to_string()));
    assert!(args.contains(&"make_zero".to_string()));
    assert!(args.contains(&"-movflags".to_string()));
    assert!(args.contains(&"+faststart".to_string()));
    assert!(
        !args.contains(
            &"frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets".to_string()
        ),
        "Must NOT contain legacy fragmented MP4 movflags"
    );

    // Verify progress telemetry
    assert!(args.contains(&"-progress".to_string()));
    assert!(args.contains(&"pipe:2".to_string()));

    // Verify output destination is staged file directly with mp4 container format (not pipe:1)
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"mp4".to_string()));
    assert_eq!(
        args.last().unwrap(),
        &output_path.to_string_lossy().to_string()
    );
    assert!(
        !args.contains(&"pipe:1".to_string()),
        "Must NOT output to stdout pipe:1"
    );

    // Verify command builder produces configured command with null stdin
    let cmd = build_local_concat_ffmpeg_command("test_ffmpeg", &manifest_path, &output_path);
    let program = cmd.as_std().get_program().to_string_lossy();
    assert_eq!(program, "test_ffmpeg");
}

#[test]
fn test_build_remote_concat_ffmpeg_args_contains_required_flags() {
    let manifest_path = PathBuf::from("temp/concat_manifest.txt");
    let output_path = PathBuf::from("temp_staging/livestream.mp4.part");
    let args = build_remote_concat_ffmpeg_args(&manifest_path, &output_path);

    // Verify Concat Demuxer input flags
    assert!(args.contains(&"-protocol_whitelist".to_string()));
    assert!(args.contains(&"file,http,tcp".to_string()));
    assert!(args.contains(&"-safe".to_string()));
    assert!(args.contains(&"0".to_string()));
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"concat".to_string()));
    assert!(args.contains(&"-i".to_string()));
    let i_pos = args.iter().position(|a| a == "-i").expect("-i present");
    assert_eq!(args[i_pos + 1], manifest_path.to_string_lossy().to_string());

    // Verify protocol whitelist precedes -i
    let pw_pos = args
        .iter()
        .position(|a| a == "-protocol_whitelist")
        .expect("-protocol_whitelist present");
    assert!(pw_pos < i_pos, "-protocol_whitelist must precede -i");

    // Verify stream mapping and monotonic bitstream filters
    assert_concat_stream_and_bsf_args(&args, i_pos);

    // Verify timestamp normalization & faststart seek table flags (ADR 0011, Ticket #70)
    assert!(args.contains(&"-avoid_negative_ts".to_string()));
    assert!(args.contains(&"make_zero".to_string()));
    assert!(args.contains(&"-movflags".to_string()));
    assert!(
        args.contains(&"+faststart".to_string()),
        "Must configure +faststart for front-positioned moov atom seek table"
    );
    assert!(
        !args.contains(
            &"frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets".to_string()
        ),
        "Must NOT contain legacy fragmented MP4 movflags"
    );

    // Verify progress telemetry
    assert!(args.contains(&"-progress".to_string()));
    assert!(args.contains(&"pipe:2".to_string()));

    // Verify output destination is the direct seekable staging file (not pipe:1)
    assert!(args.contains(&"-f".to_string()));
    assert!(args.contains(&"mp4".to_string()));
    assert_eq!(
        args.last().unwrap(),
        &output_path.to_string_lossy().to_string(),
        "Must output directly to staged file path"
    );
    assert!(
        !args.contains(&"pipe:1".to_string()),
        "Must NOT output to stdout pipe:1"
    );

    // Verify command builder produces configured command with null stdin and null stdout
    let cmd = build_remote_concat_ffmpeg_command("test_ffmpeg", &manifest_path, &output_path);
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
    let staged_file = temp_dir.join("livestream.mp4.part");
    let final_file = temp_dir.join("livestream.mp4");

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
    let staged_file = temp_dir.join("livestream.mp4.part");
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
    assert!(!temp_dir.join("livestream.mp4.part").exists());
    assert!(!temp_dir.join("livestream.mp4").exists());

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
        !temp_dir.join("livestream.mp4.part").exists(),
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

    let part_mp4 = temp_dir.join("livestream.mp4.part");
    let final_mp4 = temp_dir.join("livestream.mp4");
    assert!(
        part_mp4.exists(),
        "Staged .part file must exist before finalization"
    );
    assert!(
        !final_mp4.exists(),
        "Final livestream.mp4 must not exist before finalization"
    );

    // Atomically finalize staged video
    finalize_staged_video(&target, None)
        .await
        .expect("finalize_staged_video must succeed");

    assert!(final_mp4.exists(), "Final livestream.mp4 must exist");
    assert!(!part_mp4.exists(), "Staged .part file must be renamed");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original chunks must remain untouched during video consolidation"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_staged_video_atomic_finalization_remote() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    let mock_bin = common::mock_rclone::get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_finalize_remote");
    let staged_file = temp_dir.join("livestream.mp4.part");
    let final_file = temp_dir.join("livestream.mp4");

    tokio::fs::write(&staged_file, b"remote-mp4-payload")
        .await
        .unwrap();

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let target = TargetLocation::Remote(format!("mockremfin:{remote_dir_str}"));
    finalize_staged_video(&target, Some(mock_bin.to_str().unwrap()))
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
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    let mock_bin = common::mock_rclone::get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_cleanup_remote");
    let staged_file = temp_dir.join("livestream.mp4.part");
    let chunk0 = temp_dir.join("chunk_0000.ts");

    tokio::fs::write(&staged_file, b"remote-garbage")
        .await
        .unwrap();
    tokio::fs::write(&chunk0, b"original-remote-chunk")
        .await
        .unwrap();

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let target = TargetLocation::Remote(format!("mockremcln:{remote_dir_str}"));
    cleanup_staged_video(&target, Some(mock_bin.to_str().unwrap()))
        .await
        .expect("Remote cleanup should succeed");

    assert!(!staged_file.exists(), ".part file must be removed");
    assert!(chunk0.exists(), "Original chunk must remain untouched");

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_with_rclone() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
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

    let staging_dir = create_temp_test_dir("test_cons_remote_staging");
    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    let options = VideoConsolidationOptions::default().with_staging_dir(&staging_dir);
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Remote video consolidation should succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let part_mp4 = staging_dir.join("livestream.mp4.part");
    let final_mp4 = staging_dir.join("livestream.mp4");
    assert!(
        final_mp4.exists(),
        "Final livestream.mp4 must exist locally in staging directory upon zero-exit FFmpeg completion"
    );
    assert!(
        !part_mp4.exists(),
        "Staged .part file must be atomically finalized locally to livestream.mp4"
    );

    // Verify 0x chunk storage and faststart seek table
    assert_staging_zero_ts_and_faststart(&staging_dir, &final_mp4).await;

    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original remote chunks must remain untouched"
    );

    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_failure_cleans_up_part() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    let mock_bin = common::mock_rclone::get_mock_rclone_bin();

    let temp_dir = create_temp_test_dir("test_cons_rem_fail");
    let staging_dir = create_temp_test_dir("test_cons_rem_fail_staging");
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
    let target = TargetLocation::Remote(format!("mockremfail:{remote_dir_str}"));
    let options = VideoConsolidationOptions::default()
        .with_rclone_bin(mock_bin.to_str().unwrap())
        .with_ffmpeg_bin("nonexistent_ffmpeg_bin_abc")
        .with_staging_dir(&staging_dir);
    let cancel_token = CancellationToken::new();

    let err = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect_err("Remote consolidation with invalid ffmpeg must fail");

    assert!(
        err.to_string().contains("nonexistent_ffmpeg_bin_abc"),
        "Expected error to contain 'nonexistent_ffmpeg_bin_abc', but got: {err:#}"
    );
    assert!(
        !staging_dir.join("livestream.mp4.part").exists(),
        ".part file must be cleaned up in staging directory on remote failure"
    );
    assert!(
        !staging_dir.join("livestream.mp4").exists(),
        "final .mp4 must not exist in staging directory on remote failure"
    );
    assert!(
        chunk0.exists(),
        "Original chunk must remain untouched after remote failure"
    );

    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
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
        !temp_dir.join("livestream.mp4.part").exists(),
        "Staged .part file must be cleaned up on cancellation"
    );
    assert!(
        !temp_dir.join("livestream.mp4").exists(),
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

    let part_mp4 = temp_dir.join("livestream.mp4.part");
    let final_mp4 = temp_dir.join("livestream.mp4");
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

    assert!(final_mp4.exists(), "Final livestream.mp4 must exist");
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

    let staged_file = temp_dir.join("livestream.mp4.part");
    let final_file = temp_dir.join("livestream.mp4");
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

    let final_mp4 = temp_dir.join("livestream.mp4");
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

#[tokio::test]
async fn test_consolidate_video_remote_concat_demuxer_zero_frame_drops() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    if !ensure_ffmpeg_available() || !ensure_rclone_available() {
        eprintln!("[SKIP] ffmpeg or rclone not available, skipping remote zero frame drops test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_remote_demux");
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    // Generate 2 valid synthetic MPEG-TS chunks without B-frames (-bf 0): 2.0s each at 10fps (20 frames each = 40 frames total)
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

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_name = format!("tstremdemux{}", rand::random::<u16>());
    unsafe {
        std::env::set_var(
            format!("RCLONE_CONFIG_{}_TYPE", remote_name.to_ascii_uppercase()),
            "local",
        );
    }

    let staging_dir = create_temp_test_dir("test_cons_remote_demux_staging");
    let target = TargetLocation::Remote(format!("{remote_name}:{remote_dir_str}"));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let options = VideoConsolidationOptions::default()
        .with_staging_dir(&staging_dir)
        .with_progress_sender(tx);
    let cancel_token = CancellationToken::new();

    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Remote consolidation with Concat Demuxer and Loopback Server must succeed");

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

    let part_mp4 = staging_dir.join("livestream.mp4.part");
    let final_mp4 = staging_dir.join("livestream.mp4");
    assert!(
        final_mp4.exists(),
        "Final livestream.mp4 must exist locally in staging directory upon zero-exit FFmpeg completion"
    );
    assert!(!part_mp4.exists(), "Staged .part file must no longer exist");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original remote chunks must remain untouched"
    );

    // Verify 0x chunk storage and faststart seek table
    assert_staging_zero_ts_and_faststart(&staging_dir, &final_mp4).await;

    // If ffprobe is available, verify 100% preserved frames and normalized start time
    if has_ffprobe {
        let final_frames = probe_video_frame_count(&final_mp4);
        assert_eq!(
            final_frames, 40,
            "Must preserve 100% of video frames (20 + 20 = 40) across remote chunk seams"
        );

        let final_packets = probe_video_packet_count(&final_mp4);
        assert_eq!(
            final_packets, 40,
            "Must preserve all video packets with zero dropped boundary packets on remote"
        );

        let format_start = probe_format_start_time(&final_mp4);
        assert!(
            (format_start - 0.0).abs() < 0.001,
            "Container start time must be normalized to 0.0s (got {format_start})"
        );
    }

    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_active_cancellation_cleans_up_part() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping remote active cancellation test");
        return;
    }

    let mock_bin = common::mock_rclone::get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_cons_rem_act_cancel");
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

    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let staging_dir = create_temp_test_dir("test_cons_rem_act_cancel_staging");
    let target = TargetLocation::Remote(format!("mockremactc:{remote_dir_str}"));
    let options = VideoConsolidationOptions::default()
        .with_rclone_bin(mock_bin.to_str().unwrap())
        .with_staging_dir(&staging_dir);
    let cancel_token = CancellationToken::new();

    let cancel_token_clone = cancel_token.clone();
    let target_clone = target.clone();
    let chunks_clone = chunks.clone();

    let handle = tokio::spawn(async move {
        consolidate_video(&target_clone, &chunks_clone, &options, cancel_token_clone).await
    });

    // Let loopback server and FFmpeg start, then cooperatively cancel
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel_token.cancel();

    let result = handle.await.unwrap();
    assert!(
        result.is_err(),
        "Active remote cancellation must return error"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("cancelled by cooperative cancellation token")
    );

    let staged_file = staging_dir.join("livestream.mp4.part");
    let final_file = staging_dir.join("livestream.mp4");
    assert!(
        !staged_file.exists(),
        "Staged .part file must be cleaned up on active remote cancellation"
    );
    assert!(!final_file.exists(), "Final file must not exist on remote");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original remote chunks must remain untouched on active cancellation"
    );

    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_consolidate_video_remote_hermetic_with_mock_rclone() {
    let _lock = VIDEO_REMOTE_TEST_MUTEX.lock().await;
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping test");
        return;
    }

    let mock_bin = common::mock_rclone::get_mock_rclone_bin();
    let temp_dir = create_temp_test_dir("test_cons_rem_hermetic");
    let staging_dir = create_temp_test_dir("test_cons_rem_hermetic_staging");
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
    let remote_base = format!("mockremote:{remote_dir_str}");
    let options = VideoConsolidationOptions::default()
        .with_rclone_bin(mock_bin.to_str().unwrap())
        .with_staging_dir(&staging_dir);
    let cancel_token = CancellationToken::new();

    let result = consolidate_video_remote(&remote_base, &chunks, &options, cancel_token).await;
    assert!(
        result.is_ok(),
        "Hermetic remote consolidation with mock_rclone must succeed: {:?}",
        result.err()
    );

    let res = result.unwrap();
    assert_eq!(res.chunks_processed, 2);

    let final_mp4 = staging_dir.join("livestream.mp4");
    let staged_part = staging_dir.join("livestream.mp4.part");
    assert!(
        final_mp4.exists(),
        "Final livestream.mp4 must exist locally in staging directory after mock consolidation"
    );
    assert!(!staged_part.exists(), "Staged .part file must be finalized");
    assert!(
        chunk0.exists() && chunk1.exists(),
        "Original chunks must remain untouched"
    );

    // Verify 0x chunk storage and faststart seek table
    assert_staging_zero_ts_and_faststart(&staging_dir, &final_mp4).await;

    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

fn make_relative_to_cwd(target: &std::path::Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let target = std::fs::canonicalize(target).unwrap_or_else(|_| target.to_path_buf());
    let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);

    let target_prefix = target.components().next()?;
    let cwd_prefix = cwd.components().next()?;
    if target_prefix != cwd_prefix {
        return None;
    }

    let target_comps: Vec<_> = target.components().collect();
    let cwd_comps: Vec<_> = cwd.components().collect();

    let mut common_len = 0;
    while common_len < target_comps.len()
        && common_len < cwd_comps.len()
        && target_comps[common_len] == cwd_comps[common_len]
    {
        common_len += 1;
    }

    let mut rel = PathBuf::new();
    for _ in common_len..cwd_comps.len() {
        rel.push("..");
    }
    for comp in &target_comps[common_len..] {
        rel.push(comp.as_os_str());
    }
    Some(rel)
}

#[tokio::test]
async fn test_consolidate_video_local_relative_directory_path() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_rel_path");
    let rel_dir = match make_relative_to_cwd(&temp_dir) {
        Some(r) => r,
        None => {
            eprintln!(
                "[SKIP] temp_dir and cwd are on different drives, skipping relative path test"
            );
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return;
        }
    };
    assert!(rel_dir.is_relative(), "rel_dir must be relative");

    // Generate 2 valid synthetic MPEG-TS chunks using resolved FFmpeg
    let ffmpeg_bin = resolve_ffmpeg_bin(None);
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    for chunk in [&chunk0, &chunk1] {
        let status = Command::new(&ffmpeg_bin)
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
                chunk.to_str().unwrap(),
            ])
            .output()
            .expect("generate synthetic chunk");
        assert!(
            status.status.success(),
            "Failed to generate chunk: {chunk:?}"
        );
    }

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

    let options = VideoConsolidationOptions::default();
    let cancel_token = CancellationToken::new();

    let result = consolidate_video_local(&rel_dir, &chunks, &options, cancel_token)
        .await
        .expect("Consolidation with relative directory path must succeed without 'Impossible to open' error");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    let part_mp4 = temp_dir.join("livestream.mp4.part");
    assert!(
        part_mp4.exists(),
        "Staged .part file must exist after local consolidation"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

fn encode_pes_timestamp(ts: u64, marker: u8) -> [u8; 5] {
    [
        (marker << 4) | ((((ts >> 30) & 0x07) as u8) << 1) | 1,
        ((ts >> 22) & 0xFF) as u8,
        ((((ts >> 15) & 0x7F) as u8) << 1) | 1,
        ((ts >> 7) & 0xFF) as u8,
        (((ts & 0x7F) as u8) << 1) | 1,
    ]
}

fn force_dts_collision_in_ts(data: &mut [u8], forced_dts: u64) {
    let mut pes_offsets = Vec::new();
    let mut i = 0;
    while i + 188 <= data.len() {
        if data[i] == 0x47 {
            let pusi = (data[i + 1] & 0x40) != 0;
            let afc = (data[i + 3] & 0x30) >> 4;
            let mut off = i + 4;
            if afc == 2 {
                i += 188;
                continue;
            }
            if afc == 3 {
                off += 1 + data[i + 4] as usize;
            }
            if pusi
                && off + 9 <= i + 188
                && data[off..off + 3] == [0x00, 0x00, 0x01]
                && data[off + 3] == 0xE0
            {
                let flags = data[off + 7];
                let pts_dts = (flags & 0xC0) >> 6;
                pes_offsets.push((off, pts_dts));
            }
        }
        i += 188;
    }

    assert!(
        pes_offsets.len() >= 3,
        "Generated TS must contain at least 3 video PES packets"
    );

    // Overwrite DTS on consecutive PES packets (indices 1 and 2) to forced_dts
    for &(off, pts_dts) in &pes_offsets[1..=2] {
        if pts_dts == 3 {
            let dts_bytes = encode_pes_timestamp(forced_dts, 1);
            data[off + 14..off + 19].copy_from_slice(&dts_bytes);
        } else if pts_dts == 2 {
            let pts_bytes = encode_pes_timestamp(forced_dts, 2);
            data[off + 9..off + 14].copy_from_slice(&pts_bytes);
        }
    }
}

#[tokio::test]
async fn test_consolidate_video_monotonic_dts_collision_resolution() {
    if !ensure_ffmpeg_available() {
        eprintln!("[SKIP] ffmpeg not available, skipping test");
        return;
    }

    let temp_dir = create_temp_test_dir("test_cons_dts_collision");
    let ffmpeg_bin = resolve_ffmpeg_bin(None);
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");

    // 1. Generate a valid MPEG-TS chunk with B-frames (-bf 2)
    let raw_chunk = temp_dir.join("raw_chunk.ts");
    let status = Command::new(&ffmpeg_bin)
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
            "-bf",
            "2",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            raw_chunk.to_str().unwrap(),
        ])
        .output()
        .expect("generate base chunk");
    assert!(status.status.success(), "Failed to generate base chunk");

    // 2. Synthesize identical consecutive DTS (8906 >= 8906) across packets in chunk0
    let mut chunk0_bytes = fs::read(&raw_chunk).expect("read raw chunk");
    force_dts_collision_in_ts(&mut chunk0_bytes, 8906);
    fs::write(&chunk0, &chunk0_bytes).expect("write chunk0");

    // chunk1 is also generated from the modified collision data
    fs::write(&chunk1, &chunk0_bytes).expect("write chunk1");
    let _ = fs::remove_file(&raw_chunk);

    // 3. Verify that chunk0 contains forced consecutive DTS (8906 >= 8906)
    if ensure_ffprobe_available() {
        let input_packets = probe_video_packets_dts_and_pts(&chunk0);
        let has_collision = input_packets
            .windows(2)
            .any(|w| w[0].dts == 8906 && w[1].dts == 8906);
        assert!(
            has_collision,
            "Input chunk must contain consecutive packets with identical DTS (8906 >= 8906)"
        );
    }

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

    // 4. Consolidate: FFmpeg must remux with exit code 0 under monotonic bitstream filtering
    let result = consolidate_video(&target, &chunks, &options, cancel_token)
        .await
        .expect("Consolidation with forced identical DTS collision must succeed");

    assert_eq!(result.chunks_processed, 2);
    assert!(result.bytes_written > 0);

    // 5. Finalize staged video
    finalize_staged_video(&target, None)
        .await
        .expect("finalize_staged_video must succeed");

    let final_mp4 = temp_dir.join("livestream.mp4");
    assert!(final_mp4.exists(), "Final livestream.mp4 must exist");

    // 6. Verify container start time normalization and strictly monotonic output DTS
    if ensure_ffprobe_available() {
        let start_time = probe_format_start_time(&final_mp4);
        assert!(
            (start_time - 0.0).abs() < 0.001,
            "Container start time must be normalized to 0.0s (got {start_time})"
        );

        let out_packets = probe_video_packets_dts_and_pts(&final_mp4);
        assert!(
            !out_packets.is_empty(),
            "Consolidated video must contain packets"
        );

        // Verify strictly monotonic DTS (DTS strictly advances for every consecutive packet)
        for window in out_packets.windows(2) {
            let prev_dts = window[0].dts;
            let curr_dts = window[1].dts;
            assert!(
                curr_dts > prev_dts,
                "Output DTS must strictly increase: previous {prev_dts} < current {curr_dts}"
            );
        }

        // Verify B-frame presentation ordering (PTS >= DTS for every packet)
        for pkt in &out_packets {
            assert!(
                pkt.pts >= pkt.dts,
                "B-frame presentation ordering must be preserved: PTS {} >= DTS {}",
                pkt.pts,
                pkt.dts
            );
        }
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
