use chzzk_load::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use chzzk_load::recorder::watcher::{SegmentWatcher, detect_sealed_chunks};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[test]
fn test_n_plus_one_detection_logic() {
    let temp_dir = std::env::temp_dir().join(format!("test_watcher_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let mut enqueued = HashSet::new();

    // Initially chunk_0000.ts is being written, chunk_0001 does NOT exist yet
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let mut f0 = File::create(&chunk0).unwrap();
    f0.write_all(b"partial content").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    // Should NOT seal chunk 0 yet because chunk 1 does not exist
    assert_eq!(sealed.len(), 0);

    // Now chunk_0001.ts appears with > 0 bytes
    let chunk1 = temp_dir.join("chunk_0001.ts");
    let mut f1 = File::create(&chunk1).unwrap();
    f1.write_all(b"start of chunk 1").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    // chunk_0000.ts MUST be detected as sealed
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0], chunk0);
    assert!(enqueued.contains("chunk_0000.ts"));

    // Running again without new chunks should yield 0
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed.len(), 0);

    // When stream concludes (is_final = true), chunk_0001.ts should be sealed
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, true);
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0], chunk1);
    assert!(enqueued.contains("chunk_0001.ts"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_case_insensitive_extension() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_watcher_case_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let mut enqueued = HashSet::new();
    let chunk0 = temp_dir.join("chunk_0000.TS");
    let mut f0 = File::create(&chunk0).unwrap();
    f0.write_all(b"content 0").unwrap();

    let chunk1 = temp_dir.join("chunk_0001.Ts");
    let mut f1 = File::create(&chunk1).unwrap();
    f1.write_all(b"content 1").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0], chunk0);
    assert!(enqueued.contains("chunk_0000.TS"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_segment_watcher_struct() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_watcher_struct_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let mut watcher = SegmentWatcher::new(&temp_dir);
    assert_eq!(watcher.session_dir(), &temp_dir);
    assert!(watcher.enqueued_chunks().is_empty());

    let chunk0 = temp_dir.join("chunk_0000.ts");
    File::create(&chunk0).unwrap().write_all(b"chunk0").unwrap();

    assert_eq!(watcher.detect_sealed(false).len(), 0);

    let chunk1 = temp_dir.join("chunk_0001.ts");
    File::create(&chunk1).unwrap().write_all(b"chunk1").unwrap();

    let sealed = watcher.detect_sealed(false);
    assert_eq!(sealed, vec![chunk0]);
    assert!(watcher.enqueued_chunks().contains("chunk_0000.ts"));

    let sealed_final = watcher.detect_sealed(true);
    assert_eq!(sealed_final, vec![chunk1]);
    assert!(watcher.enqueued_chunks().contains("chunk_0001.ts"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sanitize_filename() {
    let raw = "Streamer's: Live? <Game> \"Cool\" | 100% *good* / bad \\ test";
    let sanitized = sanitize_filename(raw);
    assert_eq!(
        sanitized,
        "Streamer's_ Live? _Game_ _Cool_ _ 100% _good_ _ bad _ test"
    );
}

#[test]
fn test_build_ffmpeg_command() {
    let output_pattern = Path::new("test/output_%04d.ts");
    let cmd = build_ffmpeg_command(
        "https://example.com/live.m3u8",
        output_pattern,
        10,
        Some("NID_AUT=abc; NID_SES=xyz"),
    );
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert!(args.contains(&"-i".to_string()));
    assert!(args.contains(&"https://example.com/live.m3u8".to_string()));
    assert!(args.contains(&"-segment_time".to_string()));
    assert!(args.contains(&"10".to_string()));
    assert!(args.contains(&"-segment_format".to_string()));
    assert!(args.contains(&"mpegts".to_string()));

    let headers_idx = args
        .iter()
        .position(|a| a == "-headers")
        .expect("missing -headers");
    assert!(args[headers_idx + 1].contains("Cookie: NID_AUT=abc; NID_SES=xyz"));

    let i_idx = args.iter().position(|a| a == "-i").expect("missing -i");
    assert!(i_idx > 0);
    assert!(
        !args.contains(&"-reconnect".to_string()),
        "-reconnect must NOT be included as it causes false reconnect loops on HLS m3u8 playlist EOF"
    );
    assert!(
        !args.contains(&"-reconnect_at_eof".to_string()),
        "-reconnect_at_eof must NOT be included as it causes false reconnect loops on HLS m3u8 playlist EOF"
    );
    assert!(
        !args.contains(&"-reconnect_streamed".to_string()),
        "-reconnect_streamed must NOT be included as it treats manifest EOF as stream drop"
    );
}

#[test]
fn test_ffmpeg_omits_reconnect_flags_for_hls() {
    let out_pattern = Path::new("test_%04d.ts");
    let cmd = build_ffmpeg_command("http://example.com/live.m3u8", out_pattern, 10, None);
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert!(
        !args.iter().any(|a| a.starts_with("-reconnect")),
        "FFmpeg command must omit -reconnect flags to avoid 20-30s reconnect stalls when reading m3u8 playlists"
    );
}

#[test]
fn test_detect_sealed_chunks_ignores_subdirectories_and_non_ts() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_watcher_filter_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    // Create a directory that ends with .ts
    let dir_as_ts = temp_dir.join("subfolder.ts");
    std::fs::create_dir_all(&dir_as_ts).unwrap();

    // Create a non-ts file
    let other_file = temp_dir.join("notes.txt");
    std::fs::write(&other_file, b"some notes").unwrap();

    // Create a zero-byte .ts chunk
    let chunk0 = temp_dir.join("chunk_0000.ts");
    File::create(&chunk0).unwrap();

    let mut enqueued = HashSet::new();
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed.len(), 0);

    // Populate chunk 0
    std::fs::write(&chunk0, b"chunk 0 data").unwrap();
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed.len(), 0);

    // Create chunk 1
    let chunk1 = temp_dir.join("chunk_0001.ts");
    std::fs::write(&chunk1, b"chunk 1 data").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed, vec![chunk0]);

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, true);
    assert_eq!(sealed, vec![chunk1]);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_two_ffmpeg_processes_simultaneously() {
    let temp_dir = std::env::temp_dir().join(format!("test_ffmpeg_two_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let out1 = temp_dir.join("p1_%04d.ts");
    let out2 = temp_dir.join("p2_%04d.ts");

    let mut cmd1 = tokio::process::Command::new("ffmpeg");
    cmd1.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y")
        .arg("-f")
        .arg("lavfi")
        .arg("-i")
        .arg("testsrc=size=320x240:rate=10")
        .arg("-t")
        .arg("3")
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg("1")
        .arg("-segment_format")
        .arg("mpegts")
        .arg(&out1);

    let mut cmd2 = tokio::process::Command::new("ffmpeg");
    cmd2.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y")
        .arg("-f")
        .arg("lavfi")
        .arg("-i")
        .arg("testsrc=size=320x240:rate=10")
        .arg("-t")
        .arg("3")
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg("1")
        .arg("-segment_format")
        .arg("mpegts")
        .arg(&out2);

    let mut child1 = cmd1.spawn().unwrap();
    let mut child2 = cmd2.spawn().unwrap();

    let res1 = child1.wait().await.unwrap();
    let res2 = child2.wait().await.unwrap();

    assert!(res1.success());
    assert!(res2.success());

    let count1 = std::fs::read_dir(&temp_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("p1_"))
        .count();
    let count2 = std::fs::read_dir(&temp_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("p2_"))
        .count();

    assert!(count1 > 0, "p1 chunks must exist");
    assert!(count2 > 0, "p2 chunks must exist");

    let _ = std::fs::remove_dir_all(&temp_dir);
}
