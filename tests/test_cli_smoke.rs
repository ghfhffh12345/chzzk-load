use clap::{CommandFactory, Parser};
use std::process::Command;

use chzzk_load::cli::Cli;
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
