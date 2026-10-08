use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static MOCK_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Provides a shared mock rclone binary for remote subprocess pipeline testing.
///
/// Compiles once per test process into a temporary directory and registers
/// an atexit cleanup hook.
///
/// Supported subcommands:
/// - `cat <remote_path>`: extracts local path after first `:` and streams to stdout.
///   If path contains "fail_cat", exits with code 1.
/// - `rcat <remote_path>`: reads stdin and writes to local path after first `:`.
///   If path contains "fail_rcat", exits with code 1.
/// - `moveto <src> <dst>`: renames local path src to dst.
///   If path contains "fail_move", exits with code 1.
/// - `deletefile <remote_path>`: removes local path.
pub fn get_mock_rclone_bin() -> &'static Path {
    MOCK_BIN.get_or_init(|| {
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_rclone_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        super::register_mock_temp_dir(temp_dir.clone());
        let bin_path = temp_dir.join(if cfg!(windows) {
            "mock_rclone.exe"
        } else {
            "mock_rclone"
        });
        let src_path = temp_dir.join("mock_rclone.rs");
        std::fs::write(
            &src_path,
            r#"
use std::io::{Read, Write};

fn resolve_path(arg: &str) -> String {
    // If format is remote:C:/path or remote:/path, split on first colon
    if let Some((_, rest)) = arg.split_once(':') {
        rest.to_string()
    } else {
        arg.to_string()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        std::process::exit(0);
    }

    match args[1].as_str() {
        "version" | "--version" => {
            println!("rclone v1.65.0-mock");
            std::process::exit(0);
        }
        "cat" => {
            let target = &args[2];
            if target.contains("fail_cat") {
                eprintln!("mock error: failed to cat {}", target);
                std::process::exit(1);
            }
            let local_path = resolve_path(target);
            match std::fs::read(&local_path) {
                Ok(bytes) => {
                    let mut stdout = std::io::stdout().lock();
                    let _ = stdout.write_all(&bytes);
                    let _ = stdout.flush();
                    std::process::exit(0);
                }
                Err(err) => {
                    eprintln!("mock error: cannot read {}: {}", local_path, err);
                    std::process::exit(1);
                }
            }
        }
        "rcat" => {
            let target = &args[2];
            if target.contains("fail_rcat") {
                eprintln!("mock error: failed to rcat {}", target);
                std::process::exit(1);
            }
            let local_path = resolve_path(target);
            let mut stdin = std::io::stdin().lock();
            let mut buffer = Vec::new();
            if let Err(err) = stdin.read_to_end(&mut buffer) {
                eprintln!("mock error: stdin read failed: {}", err);
                std::process::exit(1);
            }
            if let Err(err) = std::fs::write(&local_path, &buffer) {
                eprintln!("mock error: cannot write {}: {}", local_path, err);
                std::process::exit(1);
            }
            std::process::exit(0);
        }
        "moveto" => {
            let src = &args[2];
            let dst = &args[3];
            if src.contains("fail_move") || dst.contains("fail_move") {
                eprintln!("mock error: failed to moveto {} to {}", src, dst);
                std::process::exit(1);
            }
            let local_src = resolve_path(src);
            let local_dst = resolve_path(dst);
            if let Err(err) = std::fs::rename(&local_src, &local_dst) {
                eprintln!("mock error: rename {} to {} failed: {}", local_src, local_dst, err);
                std::process::exit(1);
            }
            std::process::exit(0);
        }
        "deletefile" => {
            let target = &args[2];
            let local_path = resolve_path(target);
            let _ = std::fs::remove_file(&local_path);
            std::process::exit(0);
        }
        _ => {
            eprintln!("unknown mock rclone command: {:?}", args);
            std::process::exit(1);
        }
    }
}
"#,
        )
        .unwrap();

        let status = std::process::Command::new("rustc")
            .arg(&src_path)
            .arg("-o")
            .arg(&bin_path)
            .status()
            .expect("Failed to compile mock_rclone");
        assert!(status.success(), "mock_rclone compilation failed");

        bin_path
    })
}
