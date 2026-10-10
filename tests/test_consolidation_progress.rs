use std::fs;
use std::time::{Duration, Instant};

use chzzk_load::cli::ConsolidateArgs;
use chzzk_load::consolidation::progress::{
    COLOR_BLUE, COLOR_CYAN, COLOR_DIVIDER, COLOR_GREEN, ChatProgressSnapshot, ChatProgressUpdate,
    ConsolidationProgressCoordinator, CoordinatorOutput, MilestoneTracker, PurgeProgressSnapshot,
    PurgeProgressUpdate, STYLE_DIM, STYLE_RESET, VideoProgressSnapshot, VideoProgressUpdate,
    format_bytes, format_interactive_bar, format_number_with_commas, format_progress_bar,
};
use chzzk_load::consolidation::{VideoProgressTelemetry, run_consolidation_with_coordinator};

mod common;
use common::mock_ffmpeg::get_mock_ffmpeg_bin;

#[test]
fn test_format_progress_bar() {
    assert_eq!(format_progress_bar(20, 0), "────────────────────");
    assert_eq!(format_progress_bar(20, 50), "━━━━━━━━━━──────────");
    assert_eq!(format_progress_bar(20, 60), "━━━━━━━━━━━━────────");
    assert_eq!(format_progress_bar(20, 100), "━━━━━━━━━━━━━━━━━━━━");
    assert_eq!(format_progress_bar(20, 120), "━━━━━━━━━━━━━━━━━━━━");
    assert_eq!(format_progress_bar(0, 50), "");
}

#[test]
fn test_format_interactive_bar() {
    assert_eq!(
        format_interactive_bar(20, 0),
        format!("{STYLE_RESET}{COLOR_DIVIDER}────────────────────{STYLE_RESET}")
    );
    assert_eq!(
        format_interactive_bar(20, 50),
        format!("{STYLE_RESET}━━━━━━━━━━{COLOR_DIVIDER}──────────{STYLE_RESET}")
    );
    assert_eq!(
        format_interactive_bar(20, 100),
        format!("{STYLE_RESET}━━━━━━━━━━━━━━━━━━━━{COLOR_DIVIDER}{STYLE_RESET}")
    );
    assert_eq!(format_interactive_bar(0, 50), "");
}

#[test]
fn test_format_bytes() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(1024), "1.0 KB");
    // 25.4 MB = 25.4 * 1024 * 1024 = 26,633,830.4 bytes
    assert_eq!(format_bytes(26_633_830), "25.4 MB");
    // 120.5 MB = 120.5 * 1024 * 1024 = 126,353,408 bytes
    assert_eq!(format_bytes(126_353_408), "120.5 MB");
    assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
}

#[test]
fn test_format_number_with_commas() {
    assert_eq!(format_number_with_commas(0), "0");
    assert_eq!(format_number_with_commas(999), "999");
    assert_eq!(format_number_with_commas(1000), "1,000");
    assert_eq!(format_number_with_commas(2500), "2,500");
    assert_eq!(format_number_with_commas(12345), "12,345");
    assert_eq!(format_number_with_commas(1234567), "1,234,567");
}

#[test]
fn test_video_progress_snapshot_formatting() {
    let snapshot = VideoProgressSnapshot {
        chunks_fed: 6,
        total_chunks: 10,
        bytes_fed: 126_353_408,
        total_bytes: 0,
        speed: Some("14.5x".to_string()),
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        format!(
            "{COLOR_CYAN} VID  {STYLE_RESET}{}{STYLE_DIM} 60% (6/10 chunks, 120.5 MB) 14.5x{STYLE_RESET}",
            format_interactive_bar(20, 60)
        )
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [VID] Consolidation progress: 60% (6/10 chunks, 120.5 MB) speed: 14.5x"
    );

    let snapshot_no_speed = VideoProgressSnapshot {
        chunks_fed: 2,
        total_chunks: 10,
        bytes_fed: 26_633_830,
        total_bytes: 0,
        speed: None,
    };
    assert_eq!(snapshot_no_speed.pct(), 20);
    assert_eq!(
        snapshot_no_speed.format_interactive(20),
        format!(
            "{COLOR_CYAN} VID  {STYLE_RESET}{}{STYLE_DIM} 20% (2/10 chunks, 25.4 MB){STYLE_RESET}",
            format_interactive_bar(20, 20)
        )
    );
    assert_eq!(
        snapshot_no_speed.format_non_interactive(),
        "[INFO] [VID] Consolidation progress: 20% (2/10 chunks, 25.4 MB)"
    );

    // Byte-smooth percentage progression matching Cloud Upload parity
    let snapshot_byte_smooth = VideoProgressSnapshot {
        chunks_fed: 0,
        total_chunks: 2,
        bytes_fed: 25_000_000,
        total_bytes: 100_000_000,
        speed: None,
    };
    assert_eq!(snapshot_byte_smooth.pct(), 25);
}

#[test]
fn test_interactive_progress_bar_cloud_upload_parity() {
    let snapshot = VideoProgressSnapshot {
        chunks_fed: 6,
        total_chunks: 10,
        bytes_fed: 126_353_408,
        total_bytes: 0,
        speed: Some("14.5x".to_string()),
    };
    let interactive = snapshot.format_interactive(20);
    // Unfilled track must be styled with theme::DIVIDER (#53586f -> \x1b[38;2;83;88;111m)
    assert!(
        interactive.contains("\x1b[38;2;83;88;111m────────"),
        "Unfilled track must be styled with theme::DIVIDER to prevent vertical misalignment: {interactive:?}"
    );
    // Badge must be styled with theme::CYAN (#699c9a -> \x1b[38;2;105;156;154m)
    assert!(
        interactive.contains("\x1b[38;2;105;156;154m VID  \x1b[0m"),
        "Badge must match Cloud Upload convention with theme::CYAN: {interactive:?}"
    );
    // Metrics must be styled with Modifier::DIM (\x1b[2m)
    assert!(
        interactive.contains("\x1b[2m 60%"),
        "Metrics must be styled with DIM modifier: {interactive:?}"
    );
}

#[test]
fn test_chat_progress_snapshot_formatting() {
    let snapshot = ChatProgressSnapshot {
        chunks_read: 6,
        total_chunks: 10,
        total_messages: 15_000,
        deduplicated_messages: 2_655,
        emitted_messages: 12_345,
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        format!(
            "{COLOR_BLUE} CHAT {STYLE_RESET}{}{STYLE_DIM} 60% (6/10 chunks, 12,345 msgs){STYLE_RESET}",
            format_interactive_bar(20, 60)
        )
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [CHAT] Consolidation progress: 60% (6/10 chunks, 12,345 msgs)"
    );
}

#[test]
fn test_purge_progress_snapshot_formatting() {
    let snapshot = PurgeProgressSnapshot {
        chunks_deleted: 20,
        total_chunks: 20,
    };
    assert_eq!(snapshot.pct(), 100);
    assert_eq!(
        snapshot.format_interactive(20),
        format!(
            "{COLOR_GREEN} DEL  {STYLE_RESET}{}{STYLE_DIM} 100% (20/20 chunks deleted){STYLE_RESET}",
            format_interactive_bar(20, 100)
        )
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [DEL] Purge progress: 100% (20/20 chunks deleted)"
    );

    let partial = PurgeProgressSnapshot {
        chunks_deleted: 4,
        total_chunks: 20,
    };
    assert_eq!(partial.pct(), 20);
    assert_eq!(
        partial.format_interactive(20),
        format!(
            "{COLOR_GREEN} DEL  {STYLE_RESET}{}{STYLE_DIM} 20% (4/20 chunks deleted){STYLE_RESET}",
            format_interactive_bar(20, 20)
        )
    );
    assert_eq!(
        partial.format_non_interactive(),
        "[INFO] [DEL] Purge progress: 20% (4/20 chunks deleted)"
    );
}

#[test]
fn test_milestone_tracker_20_percent_boundaries() {
    let t0 = Instant::now();
    let mut tracker = MilestoneTracker::new(t0);

    // Initial 0% should trigger
    assert!(tracker.should_log_at(0, 0, t0));

    // Repeated 0% immediately should not trigger
    assert!(!tracker.should_log_at(0, 0, t0 + Duration::from_secs(1)));

    // 10% has not reached next 20% milestone boundary
    assert!(!tracker.should_log_at(10, 1, t0 + Duration::from_secs(2)));

    // 20% reaches milestone boundary -> should trigger
    assert!(tracker.should_log_at(20, 2, t0 + Duration::from_secs(3)));

    // 35% has not reached 40% boundary
    assert!(!tracker.should_log_at(35, 3, t0 + Duration::from_secs(4)));

    // 40% reaches milestone boundary -> should trigger
    assert!(tracker.should_log_at(40, 4, t0 + Duration::from_secs(5)));

    // Jump to 65% triggers 60% milestone
    assert!(tracker.should_log_at(65, 6, t0 + Duration::from_secs(6)));

    // 80% triggers
    assert!(tracker.should_log_at(80, 8, t0 + Duration::from_secs(7)));

    // 100% triggers
    assert!(tracker.should_log_at(100, 10, t0 + Duration::from_secs(8)));

    // 100% again does not retrigger
    assert!(!tracker.should_log_at(100, 10, t0 + Duration::from_secs(9)));
}

#[test]
fn test_milestone_tracker_heartbeat_trigger() {
    let t0 = Instant::now();
    let mut tracker = MilestoneTracker::new(t0);

    // Initial 0%
    assert!(tracker.should_log_at(0, 0, t0));

    // At T=20s with progress: no 20% milestone, < 30s elapsed -> false
    assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(20)));

    // At T=31s (>30s) with progress advanced (50 > 0) -> true (heartbeat)
    assert!(tracker.should_log_at(5, 50, t0 + Duration::from_secs(31)));

    // At T=35s (<30s since last log at 31s) -> false
    assert!(!tracker.should_log_at(5, 55, t0 + Duration::from_secs(35)));

    // At T=65s (>30s since last log at 31s), even when progress has stalled (still 50) -> true (heartbeat during stall)
    assert!(tracker.should_log_at(5, 50, t0 + Duration::from_secs(65)));

    // At T=70s (<30s since last log at 65s) -> false
    assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(70)));

    // At T=96s (>30s since last log at 65s) with progress advanced (60 > 50) -> true (heartbeat)
    assert!(tracker.should_log_at(5, 60, t0 + Duration::from_secs(96)));
}

#[test]
fn test_ansi_suppression_non_interactive() {
    let vid = VideoProgressSnapshot {
        chunks_fed: 6,
        total_chunks: 10,
        bytes_fed: 126_353_408,
        total_bytes: 0,
        speed: Some("14.5x".to_string()),
    };
    let chat = ChatProgressSnapshot {
        chunks_read: 6,
        total_chunks: 10,
        total_messages: 15_000,
        deduplicated_messages: 2_655,
        emitted_messages: 12_345,
    };
    let del = PurgeProgressSnapshot {
        chunks_deleted: 20,
        total_chunks: 20,
    };

    for text in [
        vid.format_non_interactive(),
        chat.format_non_interactive(),
        del.format_non_interactive(),
    ] {
        assert!(!text.contains('\x1b'), "must not contain ANSI escape");
        assert!(!text.contains('\r'), "must not contain carriage return");
    }
}

#[tokio::test]
async fn test_coordinator_interactive_dual_media() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let mut session = coordinator.start_media(10, 10);
    let v_tx = session.video_sender().unwrap();
    let c_tx = session.chat_sender().unwrap();

    let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
        chunks_fed: 6,
        bytes_fed: 126_353_408,
    });
    let _ = v_tx.send(VideoProgressUpdate::Speed("14.5x".to_string()));
    let _ = c_tx.send(ChatProgressUpdate {
        chunks_read: 6,
        total_messages: 15_000,
        deduplicated_messages: 2_655,
        emitted_messages: 12_345,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID in output");
    assert!(text.contains("CHAT"), "must render CHAT in output");
    assert!(text.contains("100%"), "final render must show 100%");
    assert!(text.contains("━"), "must contain filled bar character ━");
    assert!(
        text.contains('\x1b'),
        "must contain ANSI escapes for TTY mode"
    );
    assert!(text.ends_with('\n'), "must terminate with newline");
}

#[tokio::test]
async fn test_coordinator_interactive_single_media_video_only() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let mut session = coordinator.start_media(5, 0);
    assert!(session.video_sender().is_some());
    assert!(session.chat_sender().is_none());

    let v_tx = session.video_sender().unwrap();
    let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
        chunks_fed: 3,
        bytes_fed: 50_000_000,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID");
    assert!(!text.contains("CHAT"), "video-only must NOT render CHAT");
    assert!(
        !text.contains("\x1b[1A"),
        "single-media must not use dual-line cursor up \\x1b[1A"
    );
}

#[tokio::test]
async fn test_coordinator_interactive_single_media_chat_only() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let mut session = coordinator.start_media(0, 5);
    assert!(session.video_sender().is_none());
    assert!(session.chat_sender().is_some());

    let c_tx = session.chat_sender().unwrap();
    let _ = c_tx.send(ChatProgressUpdate {
        chunks_read: 3,
        total_messages: 5_000,
        deduplicated_messages: 500,
        emitted_messages: 4_500,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("CHAT"), "must render CHAT");
    assert!(!text.contains("VID"), "chat-only must NOT render VID");
    assert!(
        !text.contains("\x1b[1A"),
        "single-media must not use dual-line cursor up \\x1b[1A"
    );
}

#[tokio::test]
async fn test_coordinator_non_interactive_milestones() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, false);

    let mut session = coordinator.start_media(10, 10);
    let v_tx = session.video_sender().unwrap();
    let c_tx = session.chat_sender().unwrap();

    // Step through 20% intervals
    for i in 1..=5 {
        let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
            chunks_fed: i * 2,
            bytes_fed: (i as u64) * 20_000_000,
        });
        let _ = c_tx.send(ChatProgressUpdate {
            chunks_read: i * 2,
            total_messages: i * 2000,
            deduplicated_messages: i * 200,
            emitted_messages: i * 1800,
        });
    }

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !text.contains('\x1b'),
        "non-interactive output must NOT contain ANSI escapes"
    );
    assert!(
        !text.contains('\r'),
        "non-interactive output must NOT contain carriage return"
    );
    assert!(text.contains("[INFO] [VID] Consolidation progress: 0%"));
    assert!(text.contains("[INFO] [CHAT] Consolidation progress: 0%"));
    assert!(text.contains("[INFO] [VID] Consolidation progress: 20%"));
    assert!(text.contains("[INFO] [CHAT] Consolidation progress: 20%"));
    assert!(text.contains("[INFO] [VID] Consolidation progress: 100%"));
    assert!(text.contains("[INFO] [CHAT] Consolidation progress: 100%"));
}

#[tokio::test]
async fn test_coordinator_interactive_purge() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let mut session = coordinator.start_purge(20);
    let p_tx = session.purge_sender().unwrap();
    let _ = p_tx.send(PurgeProgressUpdate { chunks_deleted: 10 });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("DEL"), "must render DEL");
    assert!(
        text.contains("(20/20 chunks deleted)"),
        "final render must show 20/20"
    );
    assert!(
        text.contains('\x1b'),
        "interactive purge must contain ANSI escapes"
    );
    assert!(text.ends_with('\n'), "must terminate with newline");
}

#[tokio::test]
async fn test_coordinator_non_interactive_purge() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, false);

    let mut session = coordinator.start_purge(20);
    let p_tx = session.purge_sender().unwrap();
    let _ = p_tx.send(PurgeProgressUpdate { chunks_deleted: 4 });
    let _ = p_tx.send(PurgeProgressUpdate { chunks_deleted: 20 });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !text.contains('\x1b'),
        "non-interactive purge must NOT contain ANSI escapes"
    );
    assert!(
        !text.contains('\r'),
        "non-interactive purge must NOT contain carriage returns"
    );
    assert!(text.contains("[INFO] [DEL] Purge progress: 0%"));
    assert!(text.contains("[INFO] [DEL] Purge progress: 20%"));
    assert!(text.contains("[INFO] [DEL] Purge progress: 100%"));
}

#[tokio::test]
async fn test_run_consolidation_with_coordinator_interactive_e2e() {
    let mock_bin = get_mock_ffmpeg_bin();
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_bin);
    }

    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_coord_tty_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0 = temp_dir.join("chunk_0000.ts");
    let chunk1 = temp_dir.join("chunk_0001.ts");
    fs::write(&chunk0, vec![1u8; 100]).unwrap();
    fs::write(&chunk1, vec![2u8; 100]).unwrap();

    let chat0 = temp_dir.join("chat_0000.jsonl");
    fs::write(
        &chat0,
        b"{\"time_ms\":1000,\"datetime\":\"2026-10-08 20:00:00\",\"msg_type\":\"COMMERCE\",\"nickname\":\"user1\",\"content\":\"hello\",\"raw\":{}}\n",
    )
    .unwrap();

    let meta = temp_dir.join("metadata.jsonl");
    fs::write(&meta, b"{\"event\":\"start\"}\n").unwrap();

    let args = ConsolidateArgs {
        path: temp_dir.to_string_lossy().to_string(),
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 4,
    };

    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let summary = run_consolidation_with_coordinator(args, coordinator)
        .await
        .expect("consolidation with coordinator should succeed");

    assert!(summary.video_result.is_some());
    assert!(summary.chat_stats.is_some());

    // Invariant: metadata.jsonl preserved, original chunks purged
    assert!(meta.exists(), "metadata.jsonl must remain intact");
    assert!(!chunk0.exists(), "chunk_0000.ts must be purged");
    assert!(!chunk1.exists(), "chunk_0001.ts must be purged");
    assert!(!chat0.exists(), "chat_0000.jsonl must be purged");

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID in interactive mode");
    assert!(
        text.contains("CHAT"),
        "must render CHAT in interactive mode"
    );
    assert!(text.contains("DEL"), "must render DEL in interactive mode");
    assert!(
        text.contains('━'),
        "must contain Cloud Upload filled bar character ━"
    );
    assert!(
        text.contains('\x1b'),
        "must contain ANSI escape codes for interactive TTY mode"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_run_consolidation_with_coordinator_interactive_video_only() {
    let mock_bin = get_mock_ffmpeg_bin();
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_bin);
    }

    let temp_dir = std::env::temp_dir().join(format!(
        "test_cons_coord_tty_vidonly_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, vec![1u8; 100]).unwrap();

    let meta = temp_dir.join("metadata.jsonl");
    fs::write(&meta, b"{\"event\":\"start\"}\n").unwrap();

    let args = ConsolidateArgs {
        path: temp_dir.to_string_lossy().to_string(),
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 4,
    };

    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let summary = run_consolidation_with_coordinator(args, coordinator)
        .await
        .expect("video-only consolidation should succeed");

    assert!(summary.video_result.is_some());
    assert!(summary.chat_stats.is_none());

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID in interactive mode");
    assert!(
        !text.contains("CHAT"),
        "must NOT render CHAT in video-only session"
    );
    assert!(text.contains("DEL"), "must render DEL in interactive mode");
    assert!(
        !text.contains("\x1b[1A"),
        "single-media must not use dual-line cursor up"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_run_consolidation_with_coordinator_non_interactive_e2e() {
    let mock_bin = get_mock_ffmpeg_bin();
    unsafe {
        std::env::set_var("CHZZK_LOAD_FFMPEG_BIN", mock_bin);
    }

    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_coord_nontty_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, vec![1u8; 100]).unwrap();

    let chat0 = temp_dir.join("chat_0000.jsonl");
    fs::write(
        &chat0,
        b"{\"time_ms\":1000,\"datetime\":\"2026-10-08 20:00:00\",\"msg_type\":\"COMMERCE\",\"nickname\":\"user1\",\"content\":\"hello\",\"raw\":{}}\n",
    )
    .unwrap();

    let meta = temp_dir.join("metadata.jsonl");
    fs::write(&meta, b"{\"event\":\"start\"}\n").unwrap();

    let args = ConsolidateArgs {
        path: temp_dir.to_string_lossy().to_string(),
        keep_original: false,
        overwrite: false,
        strict: false,
        delete_concurrency: 4,
    };

    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, false);

    let summary = run_consolidation_with_coordinator(args, coordinator)
        .await
        .expect("consolidation with coordinator should succeed");

    assert!(summary.video_result.is_some());
    assert!(summary.chat_stats.is_some());

    // Invariant: metadata.jsonl preserved, original chunks purged
    assert!(meta.exists(), "metadata.jsonl must remain intact");
    assert!(!chunk0.exists(), "chunk_0000.ts must be purged");
    assert!(!chat0.exists(), "chat_0000.jsonl must be purged");

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !text.contains('\x1b'),
        "must NOT contain ANSI escape codes in non-interactive mode"
    );
    assert!(
        !text.contains('\r'),
        "must NOT contain carriage returns in non-interactive mode"
    );
    assert!(
        text.contains("[INFO] [VID] Consolidation progress:"),
        "must log [VID] progress"
    );
    assert!(
        text.contains("[INFO] [CHAT] Consolidation progress:"),
        "must log [CHAT] progress"
    );
    assert!(
        text.contains("[INFO] [DEL] Purge progress:"),
        "must log [DEL] progress"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_video_progress_telemetry_progress_pipe2_key_values() {
    let mut telemetry = VideoProgressTelemetry::default();

    let pipe2_lines = [
        "frame=150",
        "fps=30.00",
        "stream_0_0_q=-1.0",
        "bitrate=20615.3kbits/s",
        "total_size=12582912",
        "out_time_us=5000000",
        "out_time_ms=5000000",
        "out_time=00:00:05.000000",
        "dup_frames=0",
        "drop_frames=0",
        "speed=12.4x",
        "progress=continue",
    ];

    for line in pipe2_lines {
        assert!(
            telemetry.update_from_line(line),
            "Line should be recognized as progress telemetry: {line}"
        );
    }

    assert_eq!(telemetry.frame, Some(150));
    assert_eq!(telemetry.fps.as_deref(), Some("30.00"));
    assert_eq!(telemetry.total_size, Some(12582912));
    assert_eq!(telemetry.out_time.as_deref(), Some("00:00:05.000000"));
    assert_eq!(telemetry.speed.as_deref(), Some("12.4x"));

    // Ending progress marker
    assert!(telemetry.update_from_line("progress=end"));
}

#[test]
fn test_video_progress_telemetry_traditional_status_lines() {
    let mut telemetry = VideoProgressTelemetry::default();

    // Partial traditional status line
    let line1 = "frame=  100 fps=30 q=-1.0 size=    1024kB";
    assert!(telemetry.update_from_line(line1));
    assert_eq!(telemetry.frame, Some(100));
    assert_eq!(telemetry.fps.as_deref(), Some("30"));
    assert_eq!(telemetry.total_size, Some(1024 * 1024));

    // Full traditional status line with speed and time
    let line2 = "frame=  150 fps= 30.0 q=-1.0 size=   12582kB time=00:00:05.00 bitrate=20615.3kbits/s speed=12.4x";
    assert!(telemetry.update_from_line(line2));
    assert_eq!(telemetry.frame, Some(150));
    assert_eq!(telemetry.fps.as_deref(), Some("30.0"));
    assert_eq!(telemetry.total_size, Some(12582 * 1024));
    assert_eq!(telemetry.out_time.as_deref(), Some("00:00:05.00"));
    assert_eq!(telemetry.speed.as_deref(), Some("12.4x"));
}

#[test]
fn test_video_progress_telemetry_size_units_and_variations() {
    let mut telemetry = VideoProgressTelemetry::default();

    assert!(telemetry.update_from_line("size= 512B"));
    assert_eq!(telemetry.total_size, Some(512));

    assert!(telemetry.update_from_line("size= 1024KiB"));
    assert_eq!(telemetry.total_size, Some(1024 * 1024));

    assert!(telemetry.update_from_line("size= 50MB"));
    assert_eq!(telemetry.total_size, Some(50 * 1024 * 1024));

    assert!(telemetry.update_from_line("size= 2GB"));
    assert_eq!(telemetry.total_size, Some(2 * 1024 * 1024 * 1024));

    // N/A size should be recognized as progress token without overriding valid total_size
    assert!(telemetry.update_from_line("size=N/A"));
    assert_eq!(telemetry.total_size, Some(2 * 1024 * 1024 * 1024));
}

#[test]
fn test_video_progress_telemetry_ignores_non_progress_logs() {
    let mut telemetry = VideoProgressTelemetry::default();

    let non_progress_lines = [
        "",
        "   ",
        "[hls @ 0x123] Opening 'http://example.com/live.m3u8' for reading",
        "[in#0 @ 0xaaaaebdbabe0] Unable to open key file, Server returned 403 Forbidden",
        "segment 0001 skipping due to encryption error",
        "Conversion failed!",
        "[mp4 @ 0x7ffd] Option movflags=+faststart applied",
    ];

    for line in non_progress_lines {
        assert!(
            !telemetry.update_from_line(line),
            "Non-progress line must return false: '{line}'"
        );
    }
}

#[tokio::test]
async fn test_remux_progress_telemetry_proportional_chunk_mapping() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let total_chunks = 5;
    let total_manifest_bytes = 100_000_000u64; // 100 MB

    let mut session = coordinator.start_media(total_chunks, 0);
    let v_tx = session.video_sender().unwrap();

    // Simulate lines arriving from FFmpeg -progress pipe:2
    let progress_events = [
        (20_000_000u64, "8.5x"),   // 20% -> 1 chunk
        (40_000_000u64, "11.2x"),  // 40% -> 2 chunks
        (80_000_000u64, "13.8x"),  // 80% -> 4 chunks
        (100_000_000u64, "15.0x"), // 100% -> 5 chunks
    ];

    for (bytes, speed) in progress_events {
        let chunks_fed = ((bytes as f64 / total_manifest_bytes as f64) * total_chunks as f64)
            .round()
            .min(total_chunks as f64) as usize;
        let _ = v_tx.send(VideoProgressUpdate::Speed(speed.to_string()));
        let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
            chunks_fed,
            bytes_fed: bytes,
        });
    }

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID");
    assert!(text.contains("100%"), "must reach 100%");
    assert!(text.contains("5/5 chunks"), "must show 5/5 chunks");
    assert!(
        text.contains("95.4 MB") || text.contains("100.0 MB") || text.contains("MB"),
        "must show formatted bytes"
    );
    assert!(text.contains("15.0x"), "must render latest speed");
}

#[tokio::test]
async fn test_coordinator_interactive_remux_speed_and_progress_rendering() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, true);

    let mut session = coordinator.start_media(10, 0);
    let v_tx = session.video_sender().unwrap();

    let _ = v_tx.send(VideoProgressUpdate::Speed("12.4x".to_string()));
    let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
        chunks_fed: 4,
        bytes_fed: 40_000_000,
    });
    let _ = v_tx.send(VideoProgressUpdate::Speed("16.8x".to_string()));
    let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
        chunks_fed: 8,
        bytes_fed: 80_000_000,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("VID"), "must render VID in output");
    assert!(text.contains("16.8x"), "must display remux speed 16.8x");
    assert!(text.contains("100%"), "final finish must display 100%");
    assert!(
        text.contains('━'),
        "must contain filled progress bar character"
    );
    assert!(
        text.contains('\x1b'),
        "must contain ANSI codes in interactive mode"
    );
}

#[tokio::test]
async fn test_coordinator_non_interactive_remux_speed_and_milestones() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = ConsolidationProgressCoordinator::with_output(output, false);

    let mut session = coordinator.start_media(5, 0);
    let v_tx = session.video_sender().unwrap();

    let _ = v_tx.send(VideoProgressUpdate::Speed("14.5x".to_string()));

    // Step through 20% intervals
    for i in 1..=5 {
        let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
            chunks_fed: i,
            bytes_fed: (i as u64) * 20_000_000,
        });
    }

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(!text.contains('\x1b'), "must NOT contain ANSI escape codes");
    assert!(!text.contains('\r'), "must NOT contain carriage returns");
    assert!(
        text.contains("[INFO] [VID] Consolidation progress: 0%"),
        "must log initial milestone"
    );
    assert!(
        text.contains("[INFO] [VID] Consolidation progress: 20%"),
        "must log 20% milestone"
    );
    assert!(
        text.contains("speed: 14.5x"),
        "must display speed in non-interactive log line"
    );
    assert!(
        text.contains("[INFO] [VID] Consolidation progress: 100%"),
        "must log 100% milestone"
    );
}
