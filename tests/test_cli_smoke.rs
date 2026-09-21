use std::process::Command;

#[test]
fn test_cli_help_flag() {
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
        stdout.contains("Real-time Chzzk stream recording and Google Drive syncing"),
        "Help text should contain description"
    );
    assert!(
        stdout.contains("--config") && stdout.contains("-c"),
        "Help text should document config flag"
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
    assert!(
        stdout.contains("chzzk-load 0.1.0"),
        "Version output should match package version: {}",
        stdout
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
    assert!(stdout.contains("chzzk-load 0.1.0"));
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
        "Stderr should indicate unrecognized argument: {}",
        stderr
    );
}
