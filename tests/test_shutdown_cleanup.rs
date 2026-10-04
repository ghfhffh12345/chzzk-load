use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tiny_http::{Header, Response, Server};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use chzzk_load::chzzk::source::MockLiveStreamSource;
use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::UploadBackend;

mod common;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;
use common::mock_source::make_open_detail;
use common::observability::assert_log_emitted;

fn ensure_rclone_available() -> bool {
    let bin = std::env::var("CHZZK_LOAD_RCLONE_BIN").unwrap_or_else(|_| "rclone".to_string());
    std::process::Command::new(&bin)
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn ensure_ffmpeg_available() -> bool {
    let bin = std::env::var("CHZZK_LOAD_FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());
    std::process::Command::new(&bin)
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn test_graceful_shutdown_cleans_session_folder_with_metadata_and_last_chunk() {
    if !ensure_rclone_available() {
        return;
    }
    let mock_bin = get_mock_ffmpeg_bin();

    let temp_dir =
        std::env::temp_dir().join(format!("test_shutdown_clean_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: false,
            poll_interval_seconds: 1,
            stream_cooldown_seconds: 1,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias(
            "chan_clean_test",
            "StreamerClean",
        )],
        ..Default::default()
    };

    let chzzk = Arc::new(MockLiveStreamSource::new().with_channel_state(
        "chan_clean_test",
        make_open_detail(
            "chan_clean_test",
            "StreamerClean",
            "Shutdown Cleanup Test Stream",
            1234567,
            "https://mock/master.m3u8",
        ),
    ));
    let remote_dir =
        std::env::temp_dir().join(format!("test_shutdown_remote_{}", rand::random::<u32>()));
    fs::create_dir_all(&remote_dir).unwrap();

    let rclone_config = chzzk_load::config::RcloneConfig {
        remote_path: remote_dir.to_string_lossy().replace('\\', "/"),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
        skip_connection_check: true,
    };
    let rclone_backend = Arc::new(chzzk_load::uploader::RcloneBackend::new(rclone_config));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(
        EngineOrchestrator::with_cancel_token(
            settings.clone(),
            chzzk,
            Some(rclone_backend.clone()),
            event_tx,
            cancel_token.clone(),
        )
        .with_ffmpeg_bin(mock_bin.to_string_lossy()),
    );

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait until recording starts
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(5), event_rx.recv()).await {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            if channel_id == "chan_clean_test" {
                break;
            }
        }
    }

    // Locate the session folder
    let mut session_dir: Option<PathBuf> = None;
    for _ in 0..30 {
        if let Ok(entries) = fs::read_dir(&temp_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    let path = entry.path();
                    let name = path.file_name().unwrap().to_str().unwrap();
                    if name.contains("StreamerClean") {
                        session_dir = Some(path);
                        break;
                    }
                }
            }
        }
        if session_dir.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let session_dir = session_dir.expect("Session directory must be created");

    // Write chunk_0000.ts and chunk_0001.ts into session directory
    let c0 = session_dir.join("chunk_0000.ts");
    let c1 = session_dir.join("chunk_0001.ts");
    fs::write(&c0, b"CHUNK_0_DATA").unwrap();
    fs::write(&c1, b"CHUNK_1_DATA").unwrap();

    // Wait for chunk_0000.ts to be sealed, uploaded and deleted
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(5), event_rx.recv()).await {
        if let AppEvent::UploadCompleted { chunk_name, .. } = ev {
            if chunk_name == "chunk_0000.ts" {
                break;
            }
        }
    }

    assert!(!c0.exists(), "chunk_0000.ts must be uploaded and deleted");
    assert!(c1.exists(), "chunk_0001.ts is still active");
    assert!(
        session_dir.join("metadata.jsonl").exists(),
        "metadata.jsonl exists"
    );

    // Now safely exit via 'q' (cancel token)
    cancel_token.cancel();

    // Await orchestrator graceful shutdown
    let shutdown_res = tokio::time::timeout(Duration::from_secs(10), run_handle).await;
    assert!(
        shutdown_res.is_ok(),
        "Engine orchestrator must shut down within timeout"
    );

    // Check if the session directory is cleaned up
    assert!(
        !c1.exists(),
        "The last video chunk (chunk_0001.ts) must be uploaded and deleted locally upon safe exit via 'q'"
    );
    assert!(
        !session_dir.join("metadata.jsonl").exists(),
        "metadata.jsonl must be cleaned up upon safe exit via 'q'"
    );
    assert!(
        !session_dir.exists(),
        "The recording folder '{}' must be cleaned up upon safe exit via 'q'",
        session_dir.display()
    );

    assert_log_emitted(&mut event_rx, "Cleaned up empty session folder");

    let _ = fs::remove_dir_all(&session_dir);
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::remove_dir_all(&remote_dir);
}

#[tokio::test]
async fn test_rclone_copyto_remove_file() {
    if !ensure_rclone_available() {
        return;
    }
    let temp_dir = std::env::temp_dir().join(format!("test_rclone_del_{}", rand::random::<u32>()));
    let remote_dir =
        std::env::temp_dir().join(format!("test_rclone_dest_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    tokio::fs::create_dir_all(&remote_dir).await.unwrap();

    let chunk_path = temp_dir.join("chunk_0000.ts");
    let remote_dest = format!(
        "{}/chunk_0000.ts",
        remote_dir.to_str().unwrap().replace('\\', "/")
    );

    for i in 0..5 {
        tokio::fs::write(&chunk_path, vec![0u8; 1024 * 1024])
            .await
            .unwrap();
        let mut cmd = tokio::process::Command::new("rclone");
        cmd.arg("copyto")
            .arg(&chunk_path)
            .arg(&remote_dest)
            .arg("--use-json-log")
            .arg("--stats")
            .arg("250ms");
        let mut child = cmd.spawn().unwrap();
        let status = child.wait().await.unwrap();
        assert!(status.success());

        tokio::fs::remove_file(&chunk_path)
            .await
            .unwrap_or_else(|e| {
                panic!("Iteration {i}: remove_file failed: {e}");
            });
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    let _ = tokio::fs::remove_dir_all(&remote_dir).await;
}

#[tokio::test]
async fn test_real_ffmpeg_shutdown_cleanup() {
    if !ensure_ffmpeg_available() || !ensure_rclone_available() {
        return;
    }
    let fixture_dir = std::env::temp_dir().join(format!("test_hls_fix_{}", rand::random::<u32>()));
    fs::create_dir_all(&fixture_dir).unwrap();
    let m3u8_path = fixture_dir.join("stream.m3u8");

    // Generate real HLS stream fixture (3 seconds, 1s segments)
    let status = std::process::Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y")
        .arg("-f")
        .arg("lavfi")
        .arg("-i")
        .arg("testsrc=size=320x240:rate=10")
        .arg("-t")
        .arg("30")
        .arg("-c:v")
        .arg("libx264")
        .arg("-f")
        .arg("hls")
        .arg("-hls_time")
        .arg("1")
        .arg("-hls_list_size")
        .arg("50")
        .arg(&m3u8_path)
        .status()
        .expect("Failed to run ffmpeg to create HLS fixture");
    assert!(status.success(), "ffmpeg fixture generation failed");

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let fix_dir_clone = fixture_dir.clone();

    std::thread::spawn(move || {
        while let Ok(request) = server.recv() {
            let url = request.url().to_string();
            if url.contains("/stream.m3u8") {
                let data = fs::read(fix_dir_clone.join("stream.m3u8")).unwrap_or_default();
                let response = Response::from_data(data).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/vnd.apple.mpegurl"[..])
                        .unwrap(),
                );
                let _ = request.respond(response);
            } else if url.ends_with(".ts") {
                let file_name = url.trim_start_matches('/');
                let data = fs::read(fix_dir_clone.join(file_name)).unwrap_or_default();
                let response = Response::from_data(data).with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"video/mp2t"[..]).unwrap(),
                );
                let _ = request.respond(response);
            } else {
                let _ = request.respond(Response::empty(404));
            }
        }
    });

    let temp_dir =
        std::env::temp_dir().join(format!("test_real_recordings_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();
    let remote_dir =
        std::env::temp_dir().join(format!("test_real_remote_{}", rand::random::<u32>()));
    fs::create_dir_all(&remote_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: true,
            poll_interval_seconds: 1,
            stream_cooldown_seconds: 1,
            chunk_duration_seconds: 1,
            ..Default::default()
        },
        channels: vec![ChannelConfig::with_alias("chan_real_clean", "StreamerReal")],
        ..Default::default()
    };

    let live_detail = make_open_detail(
        "chan_real_clean",
        "StreamerReal",
        "Real FFmpeg Cleanup Stream",
        99999,
        &format!("http://127.0.0.1:{port}/stream.m3u8"),
    );
    let chzzk =
        Arc::new(MockLiveStreamSource::new().with_channel_state("chan_real_clean", live_detail));
    let rclone_config = chzzk_load::config::RcloneConfig {
        remote_path: remote_dir.to_string_lossy().replace('\\', "/"),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
        skip_connection_check: true,
    };
    let rclone_backend = Arc::new(chzzk_load::uploader::RcloneBackend::new(rclone_config));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings.clone(),
        chzzk,
        Some(rclone_backend.clone()),
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait until recording starts
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(5), event_rx.recv()).await {
        if let AppEvent::RecordingStarted { channel_id, .. } = ev {
            if channel_id == "chan_real_clean" {
                break;
            }
        }
    }

    // Wait 2.5 seconds for real FFmpeg to record
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // Trigger graceful shutdown via cancel token (equivalent to pressing 'q')
    cancel_token.cancel();

    let shutdown_res = tokio::time::timeout(Duration::from_secs(10), run_handle).await;
    assert!(
        shutdown_res.is_ok(),
        "Engine orchestrator must shut down cleanly"
    );

    while let Ok(ev) = event_rx.try_recv() {
        println!("Event: {:?}", ev);
    }

    // Inspect what's in temp_dir
    let mut session_dir: Option<PathBuf> = None;
    if let Ok(entries) = fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            if entry.file_type().unwrap().is_dir() {
                session_dir = Some(entry.path());
                break;
            }
        }
    }

    if let Some(ref dir) = session_dir {
        let remaining_files: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        println!(
            "Session dir still exists: {} with files: {:?}",
            dir.display(),
            remaining_files
        );
        panic!(
            "Session dir '{}' was NOT cleaned up! Remaining files: {:?}",
            dir.display(),
            remaining_files
        );
    }

    let _ = fs::remove_dir_all(&fixture_dir);
    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::remove_dir_all(&remote_dir);
}

#[tokio::test]
async fn test_upload_and_unlink_with_transient_file_lock() {
    if !ensure_rclone_available() {
        return;
    }
    let temp_dir = std::env::temp_dir().join(format!("test_rclone_lock_{}", rand::random::<u32>()));
    let remote_dir =
        std::env::temp_dir().join(format!("test_rclone_lock_dest_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    tokio::fs::create_dir_all(&remote_dir).await.unwrap();

    let chunk_path = temp_dir.join("chunk_0000.ts");
    tokio::fs::write(&chunk_path, b"TEST_LOCK_DATA")
        .await
        .unwrap();

    let rclone_config = chzzk_load::config::RcloneConfig {
        remote_path: remote_dir.to_string_lossy().replace('\\', "/"),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
        skip_connection_check: true,
    };
    let backend = chzzk_load::uploader::RcloneBackend::new(rclone_config);

    // Open a read handle on chunk_path to simulate an external process (e.g. antivirus/indexer)
    // holding a shared read lock without FILE_SHARE_DELETE.
    let lock_file = std::fs::File::open(&chunk_path).unwrap();

    // Release the lock after 200ms in the background
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(lock_file);
    });

    let res = backend
        .upload_file(&chunk_path, "test_session", Box::new(|_, _, _| {}))
        .await;

    let res_err = res.as_ref().err().map(|e| e.to_string());
    assert!(res.is_ok(), "upload_file should succeed: {:?}", res_err);

    let unlink_res = chzzk_load::uploader::unlink_local_file_with_retry(&chunk_path).await;
    assert!(
        unlink_res.is_ok(),
        "unlink_local_file_with_retry should tolerate transient file lock: {:?}",
        unlink_res.err()
    );
    assert!(
        !chunk_path.exists(),
        "chunk must be deleted even if briefly locked"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    let _ = tokio::fs::remove_dir_all(&remote_dir).await;
}

#[tokio::test]
#[ignore = "requires live Chzzk stream and gdrive:chzzk credentials"]
async fn test_live_chzzk_shutdown_cleanup() {
    use chzzk_load::chzzk::client::ChzzkClient;

    let temp_dir = std::env::temp_dir().join(format!("test_live_rec_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();
    let remote_dir = std::env::temp_dir().join(format!("test_live_rem_{}", rand::random::<u32>()));
    fs::create_dir_all(&remote_dir).unwrap();

    let settings = Settings {
        general: chzzk_load::config::GeneralConfig {
            recordings_dir: temp_dir.to_string_lossy().to_string(),
            record_chat: true,
            poll_interval_seconds: 1,
            stream_cooldown_seconds: 0,
            chunk_duration_seconds: 5,
            ..Default::default()
        },
        channels: vec![
            ChannelConfig::with_alias("dc7fb0d085cfbbe90e11836e3b85b784", "강소연"),
            ChannelConfig::with_alias("c8adce2ff4a3618931e07c327e1fa070", "포키쨩"),
        ],
        ..Default::default()
    };

    let chzzk = Arc::new(ChzzkClient::new(&settings.chzzk));
    let rclone_config = chzzk_load::config::RcloneConfig {
        remote_path: "gdrive:chzzk".to_string(),
        upload_concurrency: 3,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        skip_connection_check: true,
    };
    let rclone_backend = Arc::new(chzzk_load::uploader::RcloneBackend::new(rclone_config));
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(1000);
    let cancel_token = CancellationToken::new();

    let (started_tx, mut started_rx) = mpsc::channel::<()>(10);
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            println!("Ev: {:?}", ev);
            if let AppEvent::RecordingStarted { .. } = ev {
                let _ = started_tx.try_send(());
            }
        }
    });

    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings.clone(),
        chzzk,
        Some(rclone_backend.clone()),
        event_tx,
        cancel_token.clone(),
    ));

    let run_handle = tokio::spawn(orchestrator.clone().run());

    // Wait until recording starts
    let mut started_count = 0;
    let start_timeout = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < start_timeout {
        if let Ok(Some(())) =
            tokio::time::timeout(Duration::from_millis(500), started_rx.recv()).await
        {
            started_count += 1;
            if started_count >= 2 {
                break;
            }
        }
    }

    if started_count == 0 {
        println!("Stream was not live; skipping live reproduction test.");
        cancel_token.cancel();
        let _ = run_handle.await;
        let _ = fs::remove_dir_all(&temp_dir);
        let _ = fs::remove_dir_all(&remote_dir);
        return;
    }

    // Let FFmpeg record for 18 seconds to produce multiple chunks and chat messages
    tokio::time::sleep(Duration::from_secs(18)).await;

    // Trigger graceful shutdown via cancel token (equivalent to pressing 'q')
    cancel_token.cancel();

    let shutdown_res = tokio::time::timeout(Duration::from_secs(45), run_handle).await;
    assert!(
        shutdown_res.is_ok(),
        "Graceful shutdown must complete within timeout"
    );

    // Inspect what is left in temp_dir
    let mut session_dir: Option<PathBuf> = None;
    if let Ok(entries) = fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            if entry.file_type().unwrap().is_dir() {
                session_dir = Some(entry.path());
                break;
            }
        }
    }

    if let Some(ref dir) = session_dir {
        let remaining_files: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                format!(
                    "{} ({} bytes)",
                    e.file_name().to_string_lossy(),
                    e.metadata().unwrap().len()
                )
            })
            .collect();
        println!(
            "Session dir still exists: {} with files: {:?}",
            dir.display(),
            remaining_files
        );
        panic!(
            "Session dir '{}' was NOT cleaned up! Remaining files: {:?}",
            dir.display(),
            remaining_files
        );
    }

    let _ = fs::remove_dir_all(&temp_dir);
    let _ = fs::remove_dir_all(&remote_dir);
}
