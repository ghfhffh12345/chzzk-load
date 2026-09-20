use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use chzzk_load::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use chzzk_load::recorder::watcher::{detect_sealed_chunks, SegmentWatcher};

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
fn test_segment_watcher_struct() {
    let temp_dir = std::env::temp_dir().join(format!("test_watcher_struct_{}", rand::random::<u32>()));
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
    assert_eq!(sanitized, "Streamer's_ Live_ _Game_ _Cool_ _ 100% _good_ _ bad _ test");
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
    let args: Vec<String> = std_cmd.get_args().map(|s| s.to_string_lossy().to_string()).collect();

    assert!(args.contains(&"-i".to_string()));
    assert!(args.contains(&"https://example.com/live.m3u8".to_string()));
    assert!(args.contains(&"-segment_time".to_string()));
    assert!(args.contains(&"10".to_string()));
    assert!(args.contains(&"-segment_format".to_string()));
    assert!(args.contains(&"mpegts".to_string()));

    let headers_idx = args.iter().position(|a| a == "-headers").expect("missing -headers");
    assert!(args[headers_idx + 1].contains("Cookie: NID_AUT=abc; NID_SES=xyz"));
}
