use clap::{CommandFactory, Parser};
use std::process::Command;

use chzzk_load::cli::{Cli, Commands, ConsolidateArgs, VodArgs};
use chzzk_load::config::{ChannelConfig, Settings};

#[test]
fn test_cli_parse_help_contains_headless_and_no_tui() {
    // 1. Verify that passing --headless sets headless = true
    let cli_headless = Cli::try_parse_from(["chzzk-load", "--headless"])
        .expect("Parsing --headless should succeed");
    assert!(
        cli_headless.headless,
        "Passing --headless must set headless = true"
    );

    // 2. Verify that passing --no-tui sets headless = true
    let cli_no_tui =
        Cli::try_parse_from(["chzzk-load", "--no-tui"]).expect("Parsing --no-tui should succeed");
    assert!(
        cli_no_tui.headless,
        "Passing --no-tui must set headless = true"
    );

    // Verify default has headless = false
    let cli_default = Cli::try_parse_from(["chzzk-load"]).expect("Parsing default should succeed");
    assert!(
        !cli_default.headless,
        "Default CLI arguments should have headless = false"
    );

    // 3. Verify help output from clap::CommandFactory contains both flags
    let mut cmd = Cli::command();
    let mut help_buf = Vec::new();
    cmd.write_help(&mut help_buf)
        .expect("Writing clap help should succeed");
    let help_text = String::from_utf8_lossy(&help_buf);

    assert!(
        help_text.contains("--headless"),
        "Clap help text must contain --headless: {help_text}"
    );
    assert!(
        help_text.contains("--no-tui"),
        "Clap help text must contain --no-tui: {help_text}"
    );
}

#[test]
fn test_config_validation_rules() {
    // 1. Default settings return Ok(warnings) where warnings contains warning about SAMPLE_CHANNEL_ID
    let default_settings = Settings::default();
    let warnings = default_settings
        .validate()
        .expect("Default settings must pass validation with warnings");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains(Settings::SAMPLE_CHANNEL_ID)),
        "Default settings validation warnings must mention SAMPLE_CHANNEL_ID: {warnings:?}"
    );

    // 2. chunk_duration_seconds: 5 fails validation with error message mentioning "chunk_duration_seconds must be at least 10"
    let mut settings = Settings::default();
    settings.general.chunk_duration_seconds = 5;
    let errs = settings
        .validate()
        .expect_err("chunk_duration_seconds = 5 must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("chunk_duration_seconds must be at least 10")),
        "Error message must mention 'chunk_duration_seconds must be at least 10': {errs:?}"
    );

    // 3. poll_interval_seconds: 0 fails validation with error message mentioning "poll_interval_seconds must be at least 1"
    let mut settings = Settings::default();
    settings.general.poll_interval_seconds = 0;
    let errs = settings
        .validate()
        .expect_err("poll_interval_seconds = 0 must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("poll_interval_seconds must be at least 1")),
        "Error message must mention 'poll_interval_seconds must be at least 1': {errs:?}"
    );

    // 4. min_free_disk_gb: 0.05 fails validation with error message mentioning "min_free_disk_gb must be at least 0.1"
    let mut settings = Settings::default();
    settings.general.min_free_disk_gb = 0.05;
    let errs = settings
        .validate()
        .expect_err("min_free_disk_gb = 0.05 must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("min_free_disk_gb must be at least 0.1")),
        "Error message must mention 'min_free_disk_gb must be at least 0.1': {errs:?}"
    );

    // 5. min_free_disk_gb: 0.0 is accepted (disabling disk space checks)
    let mut settings = Settings {
        channels: vec![ChannelConfig::new("valid_channel_id")],
        ..Default::default()
    };
    settings.general.min_free_disk_gb = 0.0;
    let result = settings.validate();
    assert!(
        result.is_ok(),
        "min_free_disk_gb = 0.0 must be accepted, got: {result:?}"
    );

    // 6. upload_concurrency: 0 fails validation with error message mentioning "upload_concurrency must be at least 1"
    let mut settings = Settings::default();
    settings.rclone.upload_concurrency = 0;
    let errs = settings
        .validate()
        .expect_err("upload_concurrency = 0 must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("upload_concurrency must be at least 1")),
        "Error message must mention 'upload_concurrency must be at least 1': {errs:?}"
    );

    // 7. Empty channel ID fails validation
    let settings = Settings {
        channels: vec![ChannelConfig::new("")],
        ..Default::default()
    };
    let errs = settings
        .validate()
        .expect_err("Empty channel ID must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("Channel ID cannot be empty")),
        "Error message must mention 'Channel ID cannot be empty': {errs:?}"
    );

    let settings = Settings {
        channels: vec![ChannelConfig::new("   ")],
        ..Default::default()
    };
    let errs = settings
        .validate()
        .expect_err("Whitespace-only channel ID must fail validation");
    assert!(
        errs.iter()
            .any(|e| e.contains("Channel ID cannot be empty")),
        "Error message must mention 'Channel ID cannot be empty': {errs:?}"
    );

    // Optional file roundtrip test using std::env::temp_dir()
    let temp_file = std::env::temp_dir().join(format!(
        "chzzk_validation_test_{}.toml",
        rand::random::<u32>()
    ));
    let default_cfg = Settings::default();
    let toml_str = toml::to_string_pretty(&default_cfg).expect("Serialize default settings");
    std::fs::write(&temp_file, toml_str).expect("Write settings to temp file");
    let loaded =
        Settings::load_or_create_default(&temp_file).expect("Load settings from temp file");
    assert_eq!(loaded, default_cfg);
    let _ = std::fs::remove_file(temp_file);
}

#[test]
fn test_config_consolidate_extra_args_parsing_and_defaults() {
    // 1. Default RcloneConfig has empty consolidate_extra_args
    let default_rclone = chzzk_load::config::RcloneConfig::default();
    assert!(default_rclone.consolidate_extra_args.is_empty());

    // 2. Parse TOML with consolidate_extra_args
    let toml_with_args = r#"
[rclone]
remote_path = "remote:test"
consolidate_extra_args = ["--drive-chunk-size", "256M", "--fast-list"]
"#;
    let settings: Settings =
        toml::from_str(toml_with_args).expect("Should parse TOML with consolidate_extra_args");
    assert_eq!(
        settings.rclone.consolidate_extra_args,
        vec![
            "--drive-chunk-size".to_string(),
            "256M".to_string(),
            "--fast-list".to_string()
        ]
    );

    // 3. Parse TOML omitting consolidate_extra_args defaults to empty
    let toml_omitted = r#"
[rclone]
remote_path = "remote:test"
"#;
    let settings_omitted: Settings =
        toml::from_str(toml_omitted).expect("Should parse TOML omitting consolidate_extra_args");
    assert!(settings_omitted.rclone.consolidate_extra_args.is_empty());

    // 4. Default settings template includes consolidate_extra_args
    let template_settings: Settings = toml::from_str(chzzk_load::config::DEFAULT_SETTINGS_TOML)
        .expect("Default settings TOML template should parse");
    assert!(template_settings.rclone.consolidate_extra_args.is_empty());
}

#[test]
fn test_cli_binary_smoke_help() {
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("--help")
        .output()
        .expect("Failed to execute chzzk-load with --help");

    assert!(output.status.success(), "Expected exit code 0 for --help");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("chzzk-load"),
        "Help text should mention binary name"
    );
    assert!(
        stdout.contains("Real-time Chzzk stream recording and cloud storage syncing"),
        "Help text should contain description"
    );
    assert!(
        stdout.contains("--config") && stdout.contains("-c"),
        "Help text should document config flag"
    );
    assert!(
        stdout.contains("--skip-rclone-check"),
        "Help text should document --skip-rclone-check flag"
    );
    assert!(
        stdout.contains("--headless"),
        "Help text should document --headless flag: {stdout}"
    );
    assert!(
        stdout.contains("--no-tui"),
        "Help text should document --no-tui flag/alias: {stdout}"
    );
    assert!(
        stdout.contains("--help") && stdout.contains("-h"),
        "Help text should document help flag"
    );
}

#[test]
fn test_cli_short_help_flag() {
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("-h")
        .output()
        .expect("Failed to execute chzzk-load with -h");

    assert!(output.status.success(), "Expected exit code 0 for -h");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage:"));
}

#[test]
fn test_cli_version_flag() {
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("--version")
        .output()
        .expect("Failed to execute chzzk-load with --version");

    assert!(
        output.status.success(),
        "Expected exit code 0 for --version"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = format!("chzzk-load {}", env!("CARGO_PKG_VERSION"));
    assert!(
        stdout.contains(&expected),
        "Version output should match package version: {stdout}"
    );
}

#[test]
fn test_cli_short_version_flag() {
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("-V")
        .output()
        .expect("Failed to execute chzzk-load with -V");

    assert!(output.status.success(), "Expected exit code 0 for -V");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = format!("chzzk-load {}", env!("CARGO_PKG_VERSION"));
    assert!(
        stdout.contains(&expected),
        "Short version output should match package version: {stdout}"
    );
}

#[test]
fn test_cli_invalid_argument() {
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("--unrecognized-argument-xyz")
        .output()
        .expect("Failed to execute chzzk-load with invalid argument");

    assert!(
        !output.status.success(),
        "Expected non-zero exit code for invalid arguments"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unexpected argument") || stderr.contains("error:"),
        "Stderr should indicate unrecognized argument: {stderr}"
    );
}

#[test]
fn test_cli_headless_startup_smoke() {
    let temp_dir = std::env::temp_dir().join(format!("test_smoke_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("settings.toml");
    std::fs::write(
        &config_path,
        r#"
[general]
chunk_duration_seconds = 60
poll_interval_seconds = 20
stream_cooldown_seconds = 0
recordings_dir = "recordings"
min_free_disk_gb = 0.0
record_chat = false

[rclone]
remote_path = ""
upload_concurrency = 1
skip_connection_check = true

[chzzk]
nid_aut = ""
nid_ses = ""

[[channels]]
id = "dummy_chan_123"
"#,
    )
    .unwrap();

    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let mut child = Command::new(bin_path)
        .arg("--config")
        .arg(&config_path)
        .arg("--headless")
        .arg("--skip-rclone-check")
        .spawn()
        .expect("Failed to spawn chzzk-load");

    // Sleep briefly (200ms) to ensure process boots event loop without panic
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Check that child has not panicked and is running
    if let Some(status) = child.try_wait().expect("try_wait failed") {
        panic!("Process exited prematurely with status: {status:?}");
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_cli_consolidate_subcommand_parsing() {
    // 1. Basic path with defaults
    let cli = Cli::try_parse_from(["chzzk-load", "consolidate", "recordings/my_stream"])
        .expect("Parsing basic consolidate command should succeed");
    assert_eq!(
        cli.command,
        Some(Commands::Consolidate(ConsolidateArgs {
            path: "recordings/my_stream".to_string(),
            keep_original: false,
            overwrite: false,
            strict: false,
            delete_concurrency: 16,
            temp_dir: None,
            config: None,
        }))
    );

    // 2. Full flags enabled
    let cli_full = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "remote:bucket/path",
        "--keep-original",
        "--overwrite",
        "--strict",
        "--delete-concurrency",
        "8",
        "--temp-dir",
        "/mnt/staging",
    ])
    .expect("Parsing consolidate command with all flags should succeed");
    assert_eq!(
        cli_full.command,
        Some(Commands::Consolidate(ConsolidateArgs {
            path: "remote:bucket/path".to_string(),
            keep_original: true,
            overwrite: true,
            strict: true,
            delete_concurrency: 8,
            temp_dir: Some(std::path::PathBuf::from("/mnt/staging")),
            config: None,
        }))
    );

    // 3. Custom --temp-dir flag alone
    let cli_temp = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/my_stream",
        "--temp-dir",
        "/custom/staging",
    ])
    .expect("Parsing consolidate command with --temp-dir should succeed");
    assert_eq!(
        cli_temp.command,
        Some(Commands::Consolidate(ConsolidateArgs {
            path: "recordings/my_stream".to_string(),
            keep_original: false,
            overwrite: false,
            strict: false,
            delete_concurrency: 16,
            temp_dir: Some(std::path::PathBuf::from("/custom/staging")),
            config: None,
        }))
    );

    // 4. Binary smoke --help contains consolidate subcommand
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("--help")
        .output()
        .expect("Failed to execute chzzk-load --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("consolidate"),
        "Top-level --help must list consolidate subcommand: {stdout}"
    );

    // 4. Binary smoke consolidate --help
    let sub_output = Command::new(bin_path)
        .args(["consolidate", "--help"])
        .output()
        .expect("Failed to execute chzzk-load consolidate --help");
    assert!(sub_output.status.success());
    let sub_stdout = String::from_utf8_lossy(&sub_output.stdout);
    assert!(
        sub_stdout.contains("--keep-original"),
        "Subcommand help must contain --keep-original: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--overwrite"),
        "Subcommand help must contain --overwrite: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--strict"),
        "Subcommand help must contain --strict: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--delete-concurrency"),
        "Subcommand help must contain --delete-concurrency: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--temp-dir"),
        "Subcommand help must contain --temp-dir: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--config"),
        "Subcommand help must contain --config: {sub_stdout}"
    );
}

#[test]
fn test_cli_vod_subcommand_parsing() {
    // 1. Basic URL with defaults
    let cli = Cli::try_parse_from(["chzzk-load", "vod", "https://chzzk.naver.com/video/4531238"])
        .expect("Parsing basic vod command should succeed");
    assert_eq!(
        cli.command,
        Some(Commands::Vod(VodArgs {
            target: "https://chzzk.naver.com/video/4531238".to_string(),
            quality: "best".to_string(),
            output_dir: None,
            no_chat: false,
            headless: false,
            config: None,
        }))
    );

    // 2. Numeric ID with all flags including headless
    let cli_full = Cli::try_parse_from([
        "chzzk-load",
        "vod",
        "4531238",
        "-q",
        "720p",
        "-o",
        "/custom/vods",
        "--no-chat",
        "--headless",
        "-c",
        "custom_settings.toml",
    ])
    .expect("Parsing vod command with flags should succeed");
    assert_eq!(
        cli_full.command,
        Some(Commands::Vod(VodArgs {
            target: "4531238".to_string(),
            quality: "720p".to_string(),
            output_dir: Some(std::path::PathBuf::from("/custom/vods")),
            no_chat: true,
            headless: true,
            config: Some(std::path::PathBuf::from("custom_settings.toml")),
        }))
    );

    // 3. Binary smoke --help contains vod subcommand
    let bin_path = env!("CARGO_BIN_EXE_chzzk-load");
    let output = Command::new(bin_path)
        .arg("--help")
        .output()
        .expect("Failed to execute chzzk-load --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("vod"),
        "Top-level --help must list vod subcommand: {stdout}"
    );

    // 4. Binary smoke vod --help
    let sub_output = Command::new(bin_path)
        .args(["vod", "--help"])
        .output()
        .expect("Failed to execute chzzk-load vod --help");
    assert!(sub_output.status.success());
    let sub_stdout = String::from_utf8_lossy(&sub_output.stdout);
    assert!(
        sub_stdout.contains("--quality"),
        "Subcommand help must contain --quality: {sub_stdout}"
    );
    assert!(
        sub_stdout.contains("--output-dir"),
        "Subcommand help must contain --output-dir: {sub_stdout}"
    );
}

#[test]
fn test_cli_consolidate_delete_concurrency_validation() {
    // Value of 0 must fail validation
    let err_zero = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/stream",
        "--delete-concurrency",
        "0",
    ])
    .expect_err("delete_concurrency=0 must fail");
    let err_str = err_zero.to_string();
    assert!(
        err_str.contains("delete concurrency must be greater than 0"),
        "Expected error message about > 0, got: {err_str}"
    );

    // Non-integer string must fail validation
    let err_invalid = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/stream",
        "--delete-concurrency",
        "abc",
    ])
    .expect_err("delete_concurrency=abc must fail");
    assert!(err_invalid.to_string().contains("invalid"));

    // Valid positive value
    let cli = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/stream",
        "--delete-concurrency",
        "32",
    ])
    .expect("delete_concurrency=32 must succeed");
    if let Some(Commands::Consolidate(args)) = cli.command {
        assert_eq!(args.delete_concurrency, 32);
    } else {
        panic!("Expected Commands::Consolidate");
    }
}

#[test]
fn test_cli_consolidate_config_flag_parsing() {
    // 1. Long flag: --config
    let cli_long = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/stream",
        "--config",
        "custom/settings.toml",
    ])
    .expect("--config should parse successfully on consolidate subcommand");
    if let Some(Commands::Consolidate(args)) = cli_long.command {
        assert_eq!(
            args.config,
            Some(std::path::PathBuf::from("custom/settings.toml"))
        );
    } else {
        panic!("Expected Commands::Consolidate");
    }

    // 2. Short flag: -c
    let cli_short = Cli::try_parse_from([
        "chzzk-load",
        "consolidate",
        "recordings/stream",
        "-c",
        "my_config.toml",
    ])
    .expect("-c should parse successfully on consolidate subcommand");
    if let Some(Commands::Consolidate(args)) = cli_short.command {
        assert_eq!(
            args.config,
            Some(std::path::PathBuf::from("my_config.toml"))
        );
    } else {
        panic!("Expected Commands::Consolidate");
    }
}

#[test]
fn test_cli_consolidate_with_headless_and_no_tui() {
    let cli_headless = Cli::try_parse_from([
        "chzzk-load",
        "--headless",
        "consolidate",
        "recordings/stream",
    ])
    .expect("Parsing --headless consolidate should succeed");
    assert!(cli_headless.headless);
    assert_eq!(
        cli_headless.command,
        Some(Commands::Consolidate(ConsolidateArgs {
            path: "recordings/stream".to_string(),
            keep_original: false,
            overwrite: false,
            strict: false,
            delete_concurrency: 16,
            temp_dir: None,
            config: None,
        }))
    );

    let cli_no_tui =
        Cli::try_parse_from(["chzzk-load", "--no-tui", "consolidate", "recordings/stream"])
            .expect("Parsing --no-tui consolidate should succeed");
    assert!(cli_no_tui.headless);
    assert_eq!(
        cli_no_tui.command,
        Some(Commands::Consolidate(ConsolidateArgs {
            path: "recordings/stream".to_string(),
            keep_original: false,
            overwrite: false,
            strict: false,
            delete_concurrency: 16,
            temp_dir: None,
            config: None,
        }))
    );
}
