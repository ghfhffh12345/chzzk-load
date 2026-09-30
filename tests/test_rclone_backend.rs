use chzzk_load::config::RcloneConfig;
use chzzk_load::uploader::backend::UploadBackend;
use chzzk_load::uploader::rclone::{RcloneBackend, format_destination, parse_rclone_log_line};
use std::path::Path;
use std::sync::Arc;

#[test]
fn test_format_destination_standard() {
    let dest = format_destination("gdrive:Chzzk", "session_1", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/session_1/chunk_0000.ts");
}

#[test]
fn test_format_destination_trailing_slashes() {
    let dest = format_destination("gdrive:Chzzk/", "session_1/", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/session_1/chunk_0000.ts");
    assert!(!dest.contains("//"));

    let dest2 = format_destination("gdrive:Chzzk///", "session_1///", "/chunk_0000.ts");
    assert_eq!(dest2, "gdrive:Chzzk/session_1/chunk_0000.ts");
    assert!(!dest2.contains("//"));
}

#[test]
fn test_format_destination_empty_remote_dir() {
    let dest = format_destination("gdrive:Chzzk", "", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/chunk_0000.ts");

    let dest2 = format_destination("gdrive:Chzzk/", "/", "chunk_0000.ts");
    assert_eq!(dest2, "gdrive:Chzzk/chunk_0000.ts");
}

#[test]
fn test_parse_rclone_log_progress() {
    let json_line = r#"{"level":"info","msg":"Transferred","stats":{"bytes":5242880,"totalBytes":10485760,"speed":1048576.0}}"#;
    let stats = parse_rclone_log_line(json_line).expect("should parse stats from rclone json log");
    assert_eq!(stats.bytes, 5242880);
    assert_eq!(stats.total_bytes, 10485760);
    assert!((stats.speed - 1048576.0).abs() < f64::EPSILON);

    // Also verify bare stats object in json
    let json_bare = r#"{"stats":{"bytes":5242880,"totalBytes":10485760,"speed":1048576.0}}"#;
    let stats_bare = parse_rclone_log_line(json_bare).expect("should parse bare stats");
    assert_eq!(stats_bare.bytes, 5242880);
    assert_eq!(stats_bare.total_bytes, 10485760);
}

#[test]
fn test_parse_rclone_log_omits_total_bytes() {
    let json_line =
        r#"{"level":"info","msg":"Transferred","stats":{"bytes":5242880,"speed":1048576.0}}"#;
    let stats =
        parse_rclone_log_line(json_line).expect("should parse stats when totalBytes is omitted");
    assert_eq!(stats.bytes, 5242880);
    assert_eq!(stats.total_bytes, 0);
    assert!((stats.speed - 1048576.0).abs() < f64::EPSILON);
}

#[test]
fn test_parse_rclone_log_non_json_or_notice() {
    // Plain text log line
    assert!(parse_rclone_log_line("2026/09/29 14:00:00 NOTICE: Config file not found").is_none());

    // JSON without stats
    assert!(parse_rclone_log_line(r#"{"level":"info","msg":"Starting transfer"}"#).is_none());

    // Malformed JSON
    assert!(parse_rclone_log_line("{not json").is_none());
    assert!(parse_rclone_log_line("").is_none());
    assert!(parse_rclone_log_line("   ").is_none());
}

#[test]
fn test_rclone_backend_command_builder() {
    let config = RcloneConfig {
        remote_path: "remote:chzzk".to_string(),
        upload_concurrency: 3,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--fast-list".to_string(), "--transfers=4".to_string()],
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_copy_command(
        Path::new("recordings/chunk_0000.ts"),
        "remote:chzzk/session/chunk_0000.ts",
    );
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "copyto");
    assert!(args[1].ends_with("chunk_0000.ts"));
    assert_eq!(args[2], "remote:chzzk/session/chunk_0000.ts");
    assert!(args.contains(&"--use-json-log".to_string()));
    assert!(args.contains(&"--stats".to_string()));
    assert!(args.contains(&"250ms".to_string()));
    assert!(args.contains(&"--stats-log-level".to_string()));
    assert!(args.contains(&"NOTICE".to_string()));
    assert!(args.contains(&"--fast-list".to_string()));
    assert!(args.contains(&"--transfers=4".to_string()));
}

#[test]
fn test_rclone_backend_bin_resolution() {
    let config = RcloneConfig {
        remote_path: "gdrive:Test".to_string(),
        upload_concurrency: 2,
        rclone_bin: "custom_rclone".to_string(),
        extra_args: vec![],
    };
    let backend = RcloneBackend::new(config);
    let backend_with_bin = backend.with_bin("explicit_rclone");
    assert_eq!(backend_with_bin.resolve_bin(), "explicit_rclone");
}

#[test]
fn test_rclone_backend_rcat_command_builder() {
    let config = RcloneConfig {
        remote_path: "gdrive:Chzzk".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--drive-chunk-size=32M".to_string()],
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_rcat_command("gdrive:Chzzk/session/metadata.jsonl");
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "rcat");
    assert_eq!(args[1], "gdrive:Chzzk/session/metadata.jsonl");
    assert!(args.contains(&"--drive-chunk-size=32M".to_string()));
}

#[test]
fn test_rclone_backend_check_command_builder() {
    let config = RcloneConfig {
        remote_path: "gdrive:Chzzk".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_check_command();
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "lsf");
    assert!(args.contains(&"--max-depth".to_string()));
    assert!(args.contains(&"1".to_string()));
    assert!(args.contains(&"gdrive:Chzzk".to_string()));
    assert!(args.contains(&"--retries=1".to_string()));
}

#[test]
fn test_rclone_backend_object_safety() {
    let config = RcloneConfig::default();
    let backend = RcloneBackend::new(config);
    let dyn_backend: Arc<dyn UploadBackend> = Arc::new(backend);
    assert_eq!(
        dyn_backend.as_ref() as *const _,
        dyn_backend.as_ref() as *const _
    );
}
