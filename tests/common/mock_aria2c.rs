use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static MOCK_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Provides a shared mock aria2c binary for subprocess testing.
///
/// Compiles once per test process into a temporary directory and registers
/// an atexit cleanup hook.
///
/// Behavior modes:
/// - `--version`: prints mock version and exits with 0
/// - contains `fail`: prints error to stderr and exits with 1
/// - contains `hang`: sleeps for 60 seconds
/// - default: reads `--input-file` and writes dummy segment files to `--dir`
pub fn get_mock_aria2c_bin() -> &'static Path {
    MOCK_BIN.get_or_init(|| {
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_aria2c_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        super::register_mock_temp_dir(temp_dir.clone());

        let bin_path = temp_dir.join(if cfg!(windows) {
            "mock_aria2c.exe"
        } else {
            "mock_aria2c"
        });
        let src_path = temp_dir.join("mock_aria2c.rs");

        std::fs::write(
            &src_path,
            r#"
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-v" || a == "--version") {
        println!("aria2 version 1.36.0-mock");
        std::process::exit(0);
    }

    if args.iter().any(|a| a.contains("fail") || a.contains("mock_fail")) {
        eprintln!("[ERROR] aria2c download failed (mock error)");
        std::process::exit(1);
    }

    if args.iter().any(|a| a.contains("hang")) {
        std::thread::sleep(std::time::Duration::from_secs(60));
        std::process::exit(0);
    }

    let mut dir = PathBuf::from(".");
    let mut input_file: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--dir" || args[i] == "-d" {
            if i + 1 < args.len() {
                dir = PathBuf::from(&args[i + 1]);
                i += 2;
                continue;
            }
        } else if args[i].starts_with("--dir=") {
            dir = PathBuf::from(&args[i]["--dir=".len()..]);
        } else if args[i] == "--input-file" || args[i] == "-i" {
            if i + 1 < args.len() {
                input_file = Some(PathBuf::from(&args[i + 1]));
                i += 2;
                continue;
            }
        } else if args[i].starts_with("--input-file=") {
            input_file = Some(PathBuf::from(&args[i]["--input-file=".len()..]));
        }
        i += 1;
    }

    let _ = std::fs::create_dir_all(&dir);

    // Dummy MPEG-TS sync byte 0x47 payload
    let dummy_ts = [0x47u8; 188];

    if let Some(input_path) = input_file {
        if let Ok(content) = std::fs::read_to_string(&input_path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if let Some(out_name) = trimmed.strip_prefix("out=") {
                    let out_path = dir.join(out_name.trim());
                    let _ = std::fs::write(&out_path, &dummy_ts);
                }
            }
        }
    }

    std::process::exit(0);
}
"#,
        )
        .unwrap();

        let status = std::process::Command::new("rustc")
            .arg(&src_path)
            .arg("-o")
            .arg(&bin_path)
            .status()
            .expect("Failed to compile mock_aria2c");
        assert!(status.success(), "mock_aria2c compilation failed");

        // Set environment variable for tests
        // SAFETY: Test setup initializing environment variable
        unsafe {
            std::env::set_var("CHZZK_LOAD_ARIA2C_BIN", &bin_path);
        }

        bin_path
    })
}
