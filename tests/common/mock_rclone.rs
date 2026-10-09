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
    // If format is remote:C:/path or remote:/path, split on first colon,
    // but preserve Windows drive specifiers (e.g. C:\path or C:/path).
    let chars: Vec<char> = arg.chars().take(2).collect();
    if chars.len() == 2 && chars[0].is_ascii_alphabetic() && chars[1] == ':' {
        return arg.to_string();
    }
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
            if target.contains("fail_delete") {
                eprintln!("mock error: failed to deletefile {}", target);
                std::process::exit(1);
            }
            let local_path = resolve_path(target);
            if !std::path::Path::new(&local_path).exists() {
                eprintln!("mock error: {} is a directory or doesn't exist: object not found", local_path);
                std::process::exit(4);
            }
            let _ = std::fs::remove_file(&local_path);
            std::process::exit(0);
        }
        "serve" => {
            if args.len() < 3 || args[2] != "http" {
                eprintln!("mock error: expected serve http");
                std::process::exit(1);
            }
            let remote_base = if args.len() > 3 { &args[3] } else { "" };
            if remote_base.contains("fail_serve") {
                eprintln!("mock error: failed to serve http for {}", remote_base);
                std::process::exit(1);
            }
            if remote_base.contains("hang_serve") {
                std::thread::sleep(std::time::Duration::from_secs(60));
                std::process::exit(0);
            }
            let local_dir = resolve_path(remote_base);
            let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("mock error: failed to bind loopback: {}", e);
                    std::process::exit(1);
                }
            };
            let port = listener.local_addr().unwrap().port();
            eprintln!("NOTICE: HTTP Server started on [http://127.0.0.1:{}/]", port);

            for stream in listener.incoming() {
                if let Ok(mut stream) = stream {
                    let local_dir = local_dir.clone();
                    std::thread::spawn(move || {
                        let mut buf = [0u8; 4096];
                        let n = match stream.read(&mut buf) {
                            Ok(n) => n,
                            Err(_) => return,
                        };
                        if n == 0 {
                            return;
                        }
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let first_line = req.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        if parts.len() < 2 {
                            return;
                        }
                        let method = parts[0];
                        let raw_path = parts[1];
                        let path = raw_path.split('?').next().unwrap_or(raw_path).trim_start_matches('/');
                        let file_path = std::path::Path::new(&local_dir).join(path);
                        if file_path.exists() && file_path.is_file() {
                            if let Ok(bytes) = std::fs::read(&file_path) {
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: video/mp2t\r\nConnection: close\r\n\r\n",
                                    bytes.len()
                                );
                                let _ = stream.write_all(header.as_bytes());
                                if method != "HEAD" {
                                    let _ = stream.write_all(&bytes);
                                }
                                let _ = stream.flush();
                                return;
                            }
                        }
                        let not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = stream.write_all(not_found.as_bytes());
                        let _ = stream.flush();
                    });
                }
            }
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
