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

    if args.iter().any(|a| (!a.starts_with("--") || a.starts_with("--dir") || a.starts_with("--out")) && (a.contains("fail") || a.contains("mock_fail"))) {
        eprintln!("[ERROR] aria2c download failed (mock error)");
        std::process::exit(1);
    }

    if args.iter().any(|a| a.contains("hang")) {
        std::thread::sleep(std::time::Duration::from_secs(60));
        std::process::exit(0);
    }

    let mut dir = PathBuf::from(".");
    let mut input_file: Option<PathBuf> = None;
    let mut out_file: Option<String> = None;

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
        } else if args[i] == "--out" || args[i] == "-o" {
            if i + 1 < args.len() {
                out_file = Some(args[i + 1].clone());
                i += 2;
                continue;
            }
        } else if args[i].starts_with("--out=") {
            out_file = Some(args[i]["--out=".len()..].to_string());
        }
        i += 1;
    }

    let _ = std::fs::create_dir_all(&dir);

    // Dummy MPEG-TS sync byte 0x47 payload
    let dummy_ts = [0x47u8; 188];

    if let Some(out_name) = out_file {
        let is_non_faststart = args.iter().any(|a| a.contains("non_faststart"));
        let is_corrupted = args.iter().any(|a| a.contains("corrupted_mp4") || a.contains("trailing_garbage"));

        let bytes: &[u8] = if is_corrupted {
            // Valid MP4 boxes followed by trailing unindexed bytes that fail container validation
            &[
                0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p',
                b'i', b's', b'o', b'm', 0x00, 0x00, 0x02, 0x00,
                b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
                0x00, 0x00, 0x00, 0x10, b'm', b'o', b'o', b'v',
                b'm', b'v', b'h', b'd', 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x10, b'm', b'd', b'a', b't',
                b'm', b'o', b'c', b'k', b'd', b'a', b't', b'a',
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            ]
        } else if is_non_faststart {
            // Non-faststart MP4: ftyp (24 bytes), mdat (16 bytes), moov (16 bytes)
            &[
                0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p',
                b'i', b's', b'o', b'm', 0x00, 0x00, 0x02, 0x00,
                b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
                0x00, 0x00, 0x00, 0x10, b'm', b'd', b'a', b't',
                b'm', b'o', b'c', b'k', b'd', b'a', b't', b'a',
                0x00, 0x00, 0x00, 0x10, b'm', b'o', b'o', b'v',
                b'm', b'v', b'h', b'd', 0x00, 0x00, 0x00, 0x00,
            ]
        } else {
            // Valid 56-byte faststart MP4 container
            &[
                0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p',
                b'i', b's', b'o', b'm', 0x00, 0x00, 0x02, 0x00,
                b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
                0x00, 0x00, 0x00, 0x10, b'm', b'o', b'o', b'v',
                b'm', b'v', b'h', b'd', 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x10, b'm', b'd', b'a', b't',
                b'm', b'o', b'c', b'k', b'd', b'a', b't', b'a',
            ]
        };

        let out_path = dir.join(&out_name);
        let _ = std::fs::write(&out_path, bytes);

        // Remove .aria2 control file upon download completion
        let aria2_path = dir.join(format!("{out_name}.aria2"));
        let _ = std::fs::remove_file(&aria2_path);

        if args.iter().any(|a| a.contains("multi_progress")) {
            println!("[#2089b0 28B/56B(50%) CN:16 DL:5.0MiB/s ETA:1s]");
            std::thread::sleep(std::time::Duration::from_millis(50));
            println!("[#2089b0 56B/56B(100%) CN:16 DL:10.0MiB/s ETA:0s]");
        } else {
            println!("[#2089b0 56B/56B(100%) CN:16 DL:10.0MiB/s ETA:0s]");
        }
    } else if let Some(input_path) = input_file {
        if let Ok(content) = std::fs::read_to_string(&input_path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if let Some(out_name) = trimmed.strip_prefix("out=") {
                    let out_trimmed = out_name.trim();
                    let out_path = dir.join(out_trimmed);
                    let payload: &[u8] = if out_trimmed.ends_with(".mp4")
                        || out_trimmed.ends_with(".m4s")
                        || out_trimmed.ends_with(".m4v")
                    {
                        &[
                            0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p',
                            b'i', b's', b'o', b'm', 0x00, 0x00, 0x02, 0x00,
                            b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
                            0x00, 0x00, 0x00, 0x10, b'm', b'o', b'o', b'v',
                            b'm', b'v', b'h', b'd', 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x10, b'm', b'd', b'a', b't',
                            b'm', b'o', b'c', b'k', b'd', b'a', b't', b'a',
                        ]
                    } else {
                        &dummy_ts
                    };
                    let _ = std::fs::write(&out_path, payload);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_aria2c_handles_out_flag() {
        let mock_aria2c = get_mock_aria2c_bin();
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_aria2c_out_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let out_name = "test_vod.mp4.part";
        let status = std::process::Command::new(mock_aria2c)
            .arg("--dir")
            .arg(&temp_dir)
            .arg(format!("--out={out_name}"))
            .arg("https://example.com/stream.mp4")
            .status()
            .expect("Failed to execute mock aria2c with --out");

        assert!(status.success(), "mock aria2c should exit with 0");
        let target_file = temp_dir.join(out_name);
        assert!(target_file.exists(), "Target file must be written");
        assert_eq!(
            std::fs::metadata(&target_file).unwrap().len(),
            56,
            "Must be 56-byte valid MP4 container"
        );
        assert!(
            chzzk_load::consolidation::staging::probe_moov_atom(&target_file),
            "Must pass probe_moov_atom container integrity"
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
