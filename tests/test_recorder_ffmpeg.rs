use chzzk_load::recorder::ffmpeg::{FfmpegEvent, FfmpegExit, FfmpegSession};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

static MOCK_BIN: OnceLock<PathBuf> = OnceLock::new();

fn get_mock_ffmpeg_bin() -> &'static Path {
    MOCK_BIN.get_or_init(|| {
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_ffmpeg_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();
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
    let is_hang = args.iter().any(|a| a.contains("hang"));
    let is_key_error = args.iter().any(|a| a.contains("key_error"));
    let is_logs_and_exit = args.iter().any(|a| a.contains("logs_and_exit"));
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
    } else {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(l) = line {
                if l.trim() == "q" {
                    break;
                }
            }
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
        bin_path
    })
}

#[tokio::test]
async fn test_ffmpeg_session_clean_exit_on_stop_graceful() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_ffmpeg_session_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_pattern = temp_dir.join("chunk_%04d.ts");

    let mut session = FfmpegSession::spawn(
        "http://example.com/clean.m3u8",
        &out_pattern,
        10,
        None,
        Some(mock_bin.to_str().unwrap()),
    )
    .expect("Failed to spawn FfmpegSession");

    assert!(session.child_id().is_some());
    assert!(!session.is_key_forbidden());

    let exit = session
        .stop_graceful(Duration::from_secs(3))
        .await
        .expect("stop_graceful failed");

    match exit {
        FfmpegExit::Clean(status) => {
            assert!(status.success());
        }
        FfmpegExit::Killed(_) => {
            panic!("Expected FfmpegExit::Clean but got FfmpegExit::Killed");
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_ffmpeg_session_stop_graceful_timeout_escalates_to_kill() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir = std::env::temp_dir().join(format!("test_ffmpeg_hang_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_pattern = temp_dir.join("chunk_%04d.ts");

    let mut session = FfmpegSession::spawn(
        "http://example.com/hang.m3u8",
        &out_pattern,
        10,
        None,
        Some(mock_bin.to_str().unwrap()),
    )
    .expect("Failed to spawn FfmpegSession");

    assert!(session.child_id().is_some());

    // Stop with a short 200ms timeout
    let start = std::time::Instant::now();
    let exit = session
        .stop_graceful(Duration::from_millis(200))
        .await
        .expect("stop_graceful failed");
    let elapsed = start.elapsed();

    // Should take around 200ms, definitely less than 2s
    assert!(
        elapsed < Duration::from_secs(2),
        "Timeout escalation took too long: {elapsed:?}"
    );

    match exit {
        FfmpegExit::Killed(status) => {
            // Process was killed
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert!(status.signal().is_some() || !status.success());
            }
            #[cfg(windows)]
            {
                assert!(!status.success());
            }
        }
        FfmpegExit::Clean(_) => {
            panic!("Expected FfmpegExit::Killed but got FfmpegExit::Clean");
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_ffmpeg_session_kill_immediate() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir = std::env::temp_dir().join(format!("test_ffmpeg_kill_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_pattern = temp_dir.join("chunk_%04d.ts");

    let mut session = FfmpegSession::spawn(
        "http://example.com/hang.m3u8",
        &out_pattern,
        10,
        None,
        Some(mock_bin.to_str().unwrap()),
    )
    .expect("Failed to spawn FfmpegSession");

    assert!(session.child_id().is_some());

    let status = session.kill().await.expect("kill failed");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert!(status.signal().is_some() || !status.success());
    }
    #[cfg(windows)]
    {
        assert!(!status.success());
    }

    // Repeated kill should return the same cached status
    let status2 = session.kill().await.expect("second kill failed");
    assert_eq!(status, status2);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_ffmpeg_session_key_forbidden_and_log_suppression() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir = std::env::temp_dir().join(format!("test_ffmpeg_403_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_pattern = temp_dir.join("chunk_%04d.ts");

    let mut session = FfmpegSession::spawn(
        "http://example.com/key_error.m3u8",
        &out_pattern,
        10,
        None,
        Some(mock_bin.to_str().unwrap()),
    )
    .expect("Failed to spawn FfmpegSession");

    // The first event received should be KeyForbidden
    let event = tokio::time::timeout(Duration::from_secs(2), session.recv_event())
        .await
        .expect("Timed out waiting for event")
        .expect("Stream closed prematurely");

    assert_eq!(event, FfmpegEvent::KeyForbidden);
    assert!(session.is_key_forbidden());

    // Subsequent log lines should be suppressed to prevent spam.
    // Waiting 100ms should yield no events (or None if terminated).
    let next_event = tokio::time::timeout(Duration::from_millis(100), session.recv_event()).await;
    assert!(
        next_event.is_err(),
        "Expected no further events due to log suppression, but got: {next_event:?}"
    );

    let _ = session.kill().await;
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_ffmpeg_session_natural_exit_streams_logs_and_exited() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_ffmpeg_natural_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_pattern = temp_dir.join("chunk_%04d.ts");

    let mut session = FfmpegSession::spawn(
        "http://example.com/logs_and_exit.m3u8",
        &out_pattern,
        10,
        None,
        Some(mock_bin.to_str().unwrap()),
    )
    .expect("Failed to spawn FfmpegSession");

    let mut logs = Vec::new();
    let mut exit_status = None;

    while let Some(event) = session.recv_event().await {
        match event {
            FfmpegEvent::Log(line) => logs.push(line),
            FfmpegEvent::Exited(status) => {
                exit_status = Some(status);
                break;
            }
            FfmpegEvent::KeyForbidden => panic!("Unexpected KeyForbidden event"),
        }
    }

    assert!(!logs.is_empty(), "Expected at least one log line, got none");
    assert!(
        logs[0].contains("frame=  100"),
        "Expected frame log, got: {:?}",
        logs[0]
    );

    let status = exit_status.expect("Expected FfmpegEvent::Exited event");
    assert!(status.success());

    // Subsequent calls to recv_event() must yield None
    assert!(session.recv_event().await.is_none());

    let _ = std::fs::remove_dir_all(&temp_dir);
}
