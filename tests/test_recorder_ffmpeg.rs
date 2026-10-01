mod common;

use chzzk_load::recorder::ffmpeg::{FfmpegEvent, FfmpegExit, FfmpegSession};
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use std::time::Duration;

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

#[tokio::test]
async fn test_ffmpeg_session_repeated_stop_graceful_preserves_killed_outcome() {
    let mock_bin = get_mock_ffmpeg_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_ffmpeg_repeat_{}", rand::random::<u32>()));
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

    // First stop_graceful times out and escalates to Killed
    let first_exit = session
        .stop_graceful(Duration::from_millis(150))
        .await
        .expect("stop_graceful failed");
    assert!(matches!(first_exit, FfmpegExit::Killed(_)));

    // Second stop_graceful must return the exact same Killed outcome, never Clean
    let second_exit = session
        .stop_graceful(Duration::from_millis(150))
        .await
        .expect("second stop_graceful failed");
    assert_eq!(first_exit, second_exit);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
