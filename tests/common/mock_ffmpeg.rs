use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static MOCK_BIN: OnceLock<PathBuf> = OnceLock::new();

/// Provides a shared mock FFmpeg binary for subprocess testing.
///
/// Compiles once per test process into a temporary directory and registers
/// an atexit cleanup hook.
///
/// Behavior modes (configured via CLI arguments or HLS URL substrings):
/// - `hang`: sleeps for 60 seconds (useful for timeout / kill testing)
/// - `key_error`: writes AES 403 key forbidden error lines to stderr, then sleeps
/// - `logs_and_exit`: writes a standard FFmpeg progress line to stderr and exits with 0
/// - `mock_fail` / `video_error` or corrupted chunk bytes: writes invalid data error to stderr and exits with 1
/// - default: reads lines from stdin until receiving "q\n", then exits with 0
pub fn get_mock_ffmpeg_bin() -> &'static Path {
    MOCK_BIN.get_or_init(|| {
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_ffmpeg_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        super::register_mock_temp_dir(temp_dir.clone());
        let bin_path = temp_dir.join(if cfg!(windows) {
            "mock_ffmpeg.exe"
        } else {
            "mock_ffmpeg"
        });
        let src_path = temp_dir.join("mock_ffmpeg.rs");
        std::fs::write(
            &src_path,
            r#"
use std::io::{BufRead, Write};
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-version" || a == "--version") {
        println!("ffmpeg version 6.0-mock");
        std::process::exit(0);
    }
    let is_hang = args.iter().any(|a| a.contains("hang"));
    let is_key_error = args.iter().any(|a| a.contains("key_error"));
    let is_logs_and_exit = args.iter().any(|a| a.contains("logs_and_exit"));
    let is_mock_fail = args.iter().any(|a| a.contains("mock_fail") || a.contains("video_error"));
    let mut has_corrupt_input = false;
    if let Some(i_pos) = args.iter().position(|a| a == "-i") {
        if let Some(input_arg) = args.get(i_pos + 1) {
            let is_concat = args.iter().any(|a| a == "concat");
            if is_concat {
                if let Ok(content) = std::fs::read_to_string(input_arg) {
                    for line in content.lines() {
                        let trimmed = line.trim();
                        if let Some(entry) = trimmed.strip_prefix("file ") {
                            let unquoted = entry.trim_matches('\'').trim_matches('"');
                            if let Ok(bytes) = std::fs::read(unquoted) {
                                if bytes.starts_with(b"not valid")
                                    || bytes.starts_with(b"corrupt")
                                    || bytes.starts_with(b"invalid")
                                {
                                    has_corrupt_input = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            } else if let Ok(bytes) = std::fs::read(input_arg) {
                if bytes.starts_with(b"not valid")
                    || bytes.starts_with(b"corrupt")
                    || bytes.starts_with(b"invalid")
                {
                    has_corrupt_input = true;
                }
            }
        }
    }
    if is_hang {
        std::thread::sleep(std::time::Duration::from_secs(60));
    } else if is_key_error {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = writeln!(handle, "[in#0 @ 0xaaaaebdbabe0] Unable to open key file https://api.chzzk.naver.com/service/v1/encryption/lives/21326414/aes_key, Server returned 403 Forbidden (access denied)");
        let _ = writeln!(handle, "segment 0001 skipping due to encryption error");
        let _ = handle.flush();
        std::thread::sleep(std::time::Duration::from_secs(60));
    } else if is_logs_and_exit {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = writeln!(handle, "frame=  100 fps=30 q=-1.0 size=    1024kB");
        let _ = handle.flush();
        std::process::exit(0);
    } else if is_mock_fail || has_corrupt_input {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = writeln!(handle, "[h264 @ 0xmock] Invalid data found when processing input");
        let _ = handle.flush();
        std::process::exit(1);
    } else {
        let mock_mp4: &[u8] = &[
            0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p',
            b'i', b's', b'o', b'm', 0x00, 0x00, 0x02, 0x00,
            b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
            0x00, 0x00, 0x00, 0x10, b'm', b'o', b'o', b'v',
            b'm', b'v', b'h', b'd', 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x10, b'm', b'd', b'a', b't',
            b'm', b'o', b'c', b'k', b'd', b'a', b't', b'a',
        ];
        if let Some(last_arg) = args.last() {
            if !last_arg.starts_with('-')
                && !last_arg.starts_with("pipe:")
                && !last_arg.contains('%')
                && !last_arg.ends_with("mock_ffmpeg")
                && !last_arg.ends_with("mock_ffmpeg.exe")
            {
                let _ = std::fs::write(last_arg, mock_mp4);
            }
        }
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(l) = line {
                if l.trim() == "q" {
                    break;
                }
            }
        }
        if args.iter().any(|a| a == "pipe:1") {
            let mut stdout = std::io::stdout().lock();
            let _ = stdout.write_all(mock_mp4);
            let _ = stdout.flush();
        }
        std::process::exit(0);
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
            .expect("Failed to compile mock_ffmpeg");
        assert!(status.success(), "mock_ffmpeg compilation failed");

        // Automatically configure the environment variable so all test harnesses and
        // orchestrator sessions default to this mock binary hermetically.
        unsafe {
            std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", &bin_path);
        }

        bin_path
    })
}
