use std::fs;
use std::path::PathBuf;
use std::process::Command;

use chzzk_load::consolidation::manifest::{
    ChunkGap, RawManifestEntry, TargetLocation, build_manifest_from_entries, detect_index_gaps,
    discover_manifest, is_remote_path, parse_chat_chunk_index, parse_rclone_lsjson,
    parse_video_chunk_index,
};

#[test]
fn test_target_location_path_resolution() {
    // 1. Windows drive paths -> Local
    let cases_local_windows = [
        r"C:\recordings\session_1",
        r"c:\recordings\session_1",
        "C:/recordings/session_1",
        "c:/recordings/session_1",
        r"D:\",
        "D:",
        r"D:relative\path",
        r"Z:\chzzk\stream",
    ];
    for path in cases_local_windows {
        assert!(
            !is_remote_path(path),
            "Expected '{path}' to be detected as Local"
        );
        let loc = TargetLocation::parse(path);
        assert_eq!(
            loc,
            TargetLocation::Local(PathBuf::from(path)),
            "TargetLocation::parse for '{path}' should be Local"
        );
        assert!(loc.is_local());
        assert!(!loc.is_remote());
    }

    // 2. POSIX / relative paths -> Local
    let cases_local_posix = [
        "recordings/session_1",
        "./recordings/session_1",
        "../recordings",
        "/var/recordings/session",
        "session_folder",
        "nested/dir/session",
    ];
    for path in cases_local_posix {
        assert!(
            !is_remote_path(path),
            "Expected '{path}' to be detected as Local"
        );
        let loc = TargetLocation::parse(path);
        assert!(loc.is_local(), "Expected '{path}' to be Local");
    }

    // 3. UNC and extended paths -> Local
    let cases_local_unc = [
        r"\\server\share\recordings",
        "//server/share/recordings",
        r"\\?\C:\recordings\session",
    ];
    for path in cases_local_unc {
        assert!(
            !is_remote_path(path),
            "Expected '{path}' to be detected as Local"
        );
        let loc = TargetLocation::parse(path);
        assert!(loc.is_local(), "Expected '{path}' to be Local");
    }

    // 4. Paths with colon after slash (e.g. dir/sub:tag) -> Local
    let cases_local_colons = ["recordings/sub:stream", "./tag:1/recordings"];
    for path in cases_local_colons {
        assert!(
            !is_remote_path(path),
            "Expected '{path}' to be detected as Local"
        );
        let loc = TargetLocation::parse(path);
        assert!(loc.is_local(), "Expected '{path}' to be Local");
    }

    // 5. Remote rclone paths -> Remote
    let cases_remote = [
        "remote:bucket/recordings/session",
        "my-gdrive:chzzk_archives",
        "s3:bucket-name",
        "onedrive_backup:2026-10-08",
        "webdav:path/to/stream",
        "gdrive:",
    ];
    for path in cases_remote {
        assert!(
            is_remote_path(path),
            "Expected '{path}' to be detected as Remote"
        );
        let loc = TargetLocation::parse(path);
        assert_eq!(
            loc,
            TargetLocation::Remote(path.to_string()),
            "TargetLocation::parse for '{path}' should be Remote"
        );
        assert!(loc.is_remote());
        assert!(!loc.is_local());
    }
}

#[test]
fn test_chunk_index_parsing() {
    // Video chunk index parsing
    assert_eq!(parse_video_chunk_index("chunk_0000.ts"), Some(0));
    assert_eq!(parse_video_chunk_index("chunk_0001.ts"), Some(1));
    assert_eq!(parse_video_chunk_index("chunk_0123.ts"), Some(123));
    assert_eq!(parse_video_chunk_index("chunk_9999.ts"), Some(9999));
    assert_eq!(parse_video_chunk_index("chunk_10000.ts"), Some(10000));
    assert_eq!(parse_video_chunk_index("CHUNK_0042.TS"), Some(42));

    // Invalid video chunk names
    assert_eq!(parse_video_chunk_index("chunk_.ts"), None);
    assert_eq!(parse_video_chunk_index("chunk_abc.ts"), None);
    assert_eq!(parse_video_chunk_index("other_0001.ts"), None);
    assert_eq!(parse_video_chunk_index("chunk_0001.mp4"), None);
    assert_eq!(parse_video_chunk_index("consolidated.mp4"), None);

    // Chat chunk index parsing
    assert_eq!(parse_chat_chunk_index("chat_0000.jsonl"), Some(0));
    assert_eq!(parse_chat_chunk_index("chat_0001.jsonl"), Some(1));
    assert_eq!(parse_chat_chunk_index("chat_0055.jsonl"), Some(55));
    assert_eq!(parse_chat_chunk_index("CHAT_0099.JSONL"), Some(99));

    // Invalid chat chunk names
    assert_eq!(parse_chat_chunk_index("chat_.jsonl"), None);
    assert_eq!(parse_chat_chunk_index("chat_xyz.jsonl"), None);
    assert_eq!(parse_chat_chunk_index("metadata.jsonl"), None);
    assert_eq!(parse_chat_chunk_index("consolidated.jsonl"), None);
}

#[test]
fn test_contiguity_gap_detection() {
    // 1. Contiguous starting from 0 -> no gaps
    assert_eq!(detect_index_gaps(&[0, 1, 2, 3]), Vec::<ChunkGap>::new());
    assert_eq!(detect_index_gaps(&[0]), Vec::<ChunkGap>::new());
    assert_eq!(detect_index_gaps(&[]), Vec::<ChunkGap>::new());

    // 2. Missing intermediate chunk
    let gaps = detect_index_gaps(&[0, 1, 3]);
    assert_eq!(gaps, vec![ChunkGap { start: 2, end: 2 }]);
    assert_eq!(gaps[0].format(), "2");

    // 3. Missing leading chunk 0
    let gaps_leading = detect_index_gaps(&[1, 2, 3]);
    assert_eq!(gaps_leading, vec![ChunkGap { start: 0, end: 0 }]);
    assert_eq!(gaps_leading[0].format(), "0");

    // 4. Multiple missing gaps and ranges
    let gaps_multi = detect_index_gaps(&[1, 4, 5, 8]);
    assert_eq!(
        gaps_multi,
        vec![
            ChunkGap { start: 0, end: 0 },
            ChunkGap { start: 2, end: 3 },
            ChunkGap { start: 6, end: 7 },
        ]
    );
    assert_eq!(gaps_multi[0].format(), "0");
    assert_eq!(gaps_multi[1].format(), "2-3");
    assert_eq!(gaps_multi[2].format(), "6-7");
}

#[test]
fn test_build_manifest_from_entries_success() {
    let target = TargetLocation::Local(PathBuf::from("recordings/test_session"));
    let entries = vec![
        RawManifestEntry {
            name: "chunk_0001.ts".to_string(),
            size: 2048,
            is_dir: false,
        },
        RawManifestEntry {
            name: "chunk_0000.ts".to_string(),
            size: 1024,
            is_dir: false,
        },
        RawManifestEntry {
            name: "chat_0000.jsonl".to_string(),
            size: 512,
            is_dir: false,
        },
        RawManifestEntry {
            name: "chat_0001.jsonl".to_string(),
            size: 600,
            is_dir: false,
        },
        RawManifestEntry {
            name: "metadata.jsonl".to_string(),
            size: 128,
            is_dir: false,
        },
        RawManifestEntry {
            name: "subdir".to_string(),
            size: 0,
            is_dir: true,
        },
    ];

    let manifest =
        build_manifest_from_entries(target.clone(), entries, false, false).expect("Build manifest");

    assert_eq!(manifest.target, target);
    assert_eq!(manifest.video_chunks.len(), 2);
    assert_eq!(manifest.chat_chunks.len(), 2);
    assert!(manifest.has_metadata);
    assert!(!manifest.pre_existing_video);
    assert!(!manifest.pre_existing_chat);
    assert!(manifest.warnings.is_empty());

    // Verify numerical sorting
    assert_eq!(manifest.video_chunks[0].index, 0);
    assert_eq!(manifest.video_chunks[1].index, 1);
    assert_eq!(manifest.chat_chunks[0].index, 0);
    assert_eq!(manifest.chat_chunks[1].index, 1);
}

#[test]
fn test_build_manifest_contiguity_validation_strict_vs_lenient() {
    let target = TargetLocation::Local(PathBuf::from("recordings/gap_session"));
    let entries = vec![
        RawManifestEntry {
            name: "chunk_0000.ts".to_string(),
            size: 1000,
            is_dir: false,
        },
        RawManifestEntry {
            name: "chunk_0001.ts".to_string(),
            size: 1000,
            is_dir: false,
        },
        RawManifestEntry {
            name: "chunk_0003.ts".to_string(), // Missing chunk 2!
            size: 1000,
            is_dir: false,
        },
    ];

    // 1. In lenient mode (strict = false): proceeds with warning recorded
    let manifest_lenient =
        build_manifest_from_entries(target.clone(), entries.clone(), false, false)
            .expect("Lenient contiguity check should succeed with warnings");
    assert_eq!(manifest_lenient.video_chunks.len(), 3);
    assert_eq!(manifest_lenient.warnings.len(), 1);
    assert!(
        manifest_lenient.warnings[0].contains("missing chunk(s) 2"),
        "Warning must detail missing chunk: {:?}",
        manifest_lenient.warnings
    );

    // 2. In strict mode (strict = true): returns Err
    let err = build_manifest_from_entries(target, entries, true, false)
        .expect_err("Strict contiguity check should fail when gaps exist");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("Contiguity validation failed"),
        "Error message must indicate contiguity validation failure: {err_msg}"
    );
    assert!(
        err_msg.contains("missing chunk(s) 2"),
        "Error message must detail missing chunk: {err_msg}"
    );
}

#[test]
fn test_build_manifest_pre_existing_files_overwrite_logic() {
    let target = TargetLocation::Local(PathBuf::from("recordings/pre_existing"));
    let entries = vec![
        RawManifestEntry {
            name: "chunk_0000.ts".to_string(),
            size: 1000,
            is_dir: false,
        },
        RawManifestEntry {
            name: "consolidated.mp4".to_string(),
            size: 5000,
            is_dir: false,
        },
    ];

    // 1. Without overwrite -> error
    let err = build_manifest_from_entries(target.clone(), entries.clone(), false, false)
        .expect_err("Should error when pre-existing consolidated.mp4 exists without overwrite");
    assert!(err.to_string().contains("consolidated.mp4"));
    assert!(err.to_string().contains("--overwrite"));

    // 2. With overwrite -> succeeds
    let manifest = build_manifest_from_entries(target, entries, false, true)
        .expect("Should succeed when overwrite is true");
    assert!(manifest.pre_existing_video);
    assert_eq!(manifest.video_chunks.len(), 1);
}

#[test]
fn test_build_manifest_single_media_handling() {
    let target = TargetLocation::Local(PathBuf::from("recordings/single_media"));

    // 1. Video-only session
    let video_only_entries = vec![RawManifestEntry {
        name: "chunk_0000.ts".to_string(),
        size: 1000,
        is_dir: false,
    }];
    let video_manifest =
        build_manifest_from_entries(target.clone(), video_only_entries, false, false)
            .expect("Video-only session should proceed cleanly");
    assert_eq!(video_manifest.video_chunks.len(), 1);
    assert!(video_manifest.chat_chunks.is_empty());

    // 2. Chat-only session
    let chat_only_entries = vec![RawManifestEntry {
        name: "chat_0000.jsonl".to_string(),
        size: 500,
        is_dir: false,
    }];
    let chat_manifest =
        build_manifest_from_entries(target.clone(), chat_only_entries, false, false)
            .expect("Chat-only session should proceed cleanly");
    assert!(chat_manifest.video_chunks.is_empty());
    assert_eq!(chat_manifest.chat_chunks.len(), 1);

    // 3. Zero chunks of both types -> error
    let empty_entries = vec![
        RawManifestEntry {
            name: "metadata.jsonl".to_string(),
            size: 200,
            is_dir: false,
        },
        RawManifestEntry {
            name: "random_log.txt".to_string(),
            size: 100,
            is_dir: false,
        },
    ];
    let err = build_manifest_from_entries(target, empty_entries, false, false)
        .expect_err("Zero chunks of both video and chat should return error");
    assert!(
        err.to_string().contains("No video chunks"),
        "Error message should mention no chunks found: {err}"
    );
}

#[tokio::test]
async fn test_discover_local_manifest_filesystem_scan() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_manifest_local_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");
    let chat0 = temp_dir.join("chat_0000.jsonl");
    let meta = temp_dir.join("metadata.jsonl");

    fs::write(&chunk0, vec![1u8; 100]).unwrap();
    fs::write(&chunk1, vec![2u8; 200]).unwrap();
    fs::write(&chat0, b"{\"time\": 100}\n").unwrap();
    fs::write(&meta, b"{\"event\": \"start\"}\n").unwrap();

    let target = TargetLocation::Local(temp_dir.clone());
    let manifest = discover_manifest(&target, false, false)
        .await
        .expect("Discover local manifest");

    assert_eq!(manifest.video_chunks.len(), 2);
    assert_eq!(manifest.chat_chunks.len(), 1);
    assert!(manifest.has_metadata);
    assert_eq!(manifest.video_chunks[0].name, "chunk_0000.ts");
    assert_eq!(manifest.video_chunks[1].name, "chunk_0001.ts");
    assert_eq!(manifest.chat_chunks[0].name, "chat_0000.jsonl");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_parse_rclone_lsjson_output() {
    let sample_json = br#"[
        {"Path": "chunk_0000.ts", "Name": "chunk_0000.ts", "Size": 1048576, "IsDir": false},
        {"Path": "chunk_0001.ts", "Name": "chunk_0001.ts", "Size": 2097152, "IsDir": false},
        {"Path": "chat_0000.jsonl", "Name": "chat_0000.jsonl", "Size": 4096, "IsDir": false},
        {"Path": "metadata.jsonl", "Name": "metadata.jsonl", "Size": 1024, "IsDir": false},
        {"Path": "subfolder", "Name": "subfolder", "Size": -1, "IsDir": true}
    ]"#;

    let entries = parse_rclone_lsjson(sample_json).expect("Parse rclone lsjson");
    assert_eq!(entries.len(), 5);

    let target = TargetLocation::Remote("remote:bucket/my_session".to_string());
    let manifest =
        build_manifest_from_entries(target, entries, false, false).expect("Build remote manifest");

    assert_eq!(manifest.video_chunks.len(), 2);
    assert_eq!(manifest.chat_chunks.len(), 1);
    assert!(manifest.has_metadata);
    assert_eq!(manifest.video_chunks[0].size, 1048576);
    assert_eq!(manifest.video_chunks[1].size, 2097152);
}

#[test]
fn test_cli_binary_smoke_consolidate_execution() {
    let temp_dir = std::env::temp_dir().join(format!("test_cli_cons_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, vec![1u8; 100]).unwrap();
    let meta = temp_dir.join("metadata.jsonl");
    fs::write(&meta, b"{\"event\":\"start\"}\n").unwrap();

    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("consolidate")
        .arg(temp_dir.to_str().unwrap())
        .output()
        .expect("Execute chzzk-load consolidate");

    assert!(
        output.status.success(),
        "Consolidate should succeed with exit code 0"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Discovered manifest"),
        "Stdout should indicate discovered manifest: {stdout}"
    );
    assert!(
        stdout.contains("1 video chunk(s)"),
        "Stdout should count 1 video chunk: {stdout}"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
