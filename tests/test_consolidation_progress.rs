use std::fs;
use std::time::{Duration, Instant};

use chzzk_load::cli::ConsolidateArgs;
use chzzk_load::consolidation::progress::{
    ChatProgressSnapshot, ChatProgressUpdate, ConsolidationProgressCoordinator, CoordinatorOutput,
    MilestoneTracker, PurgeProgressSnapshot, PurgeProgressUpdate, VideoProgressSnapshot,
    VideoProgressUpdate, format_bytes, format_number_with_commas, format_progress_bar,
};
use chzzk_load::consolidation::run_consolidation_with_coordinator;

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
        speed: Some("14.5x".to_string()),
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        "[VID]  ━━━━━━━━━━━━──────── 60% (6/10 chunks, 120.5 MB) 14.5x"
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [VID] Consolidation progress: 60% (6/10 chunks, 120.5 MB) speed: 14.5x"
    );

    let snapshot_no_speed = VideoProgressSnapshot {
        chunks_fed: 2,
        total_chunks: 10,
        bytes_fed: 26_633_830,
        speed: None,
    };
    assert_eq!(snapshot_no_speed.pct(), 20);
    assert_eq!(
        snapshot_no_speed.format_interactive(20),
        "[VID]  ━━━━──────────────── 20% (2/10 chunks, 25.4 MB)"
    );
    assert_eq!(
        snapshot_no_speed.format_non_interactive(),
        "[INFO] [VID] Consolidation progress: 20% (2/10 chunks, 25.4 MB)"
    );
}

#[test]
fn test_chat_progress_snapshot_formatting() {
    let snapshot = ChatProgressSnapshot {
        chunks_read: 6,
        total_chunks: 10,
        total_messages: 15_000,
        emitted_messages: 12_345,
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        "[CHAT] ━━━━━━━━━━━━──────── 60% (6/10 chunks, 12,345 msgs)"
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
        "[DEL]  ━━━━━━━━━━━━━━━━━━━━ 100% (20/20 chunks deleted)"
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
        "[DEL]  ━━━━──────────────── 20% (4/20 chunks deleted)"
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

    // At T=65s (>30s since last log at 31s), but progress has NOT advanced (still 50) -> false
    assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(65)));

    // At T=65s with progress advanced (60 > 50) -> true (heartbeat)
    assert!(tracker.should_log_at(5, 60, t0 + Duration::from_secs(65)));
}

#[test]
fn test_ansi_suppression_non_interactive() {
    let vid = VideoProgressSnapshot {
        chunks_fed: 6,
        total_chunks: 10,
        bytes_fed: 126_353_408,
        speed: Some("14.5x".to_string()),
    };
    let chat = ChatProgressSnapshot {
        chunks_read: 6,
        total_chunks: 10,
        total_messages: 15_000,
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
        emitted_messages: 12_345,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("[VID]"), "must render [VID] in output");
    assert!(text.contains("[CHAT]"), "must render [CHAT] in output");
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
    assert!(text.contains("[VID]"), "must render [VID]");
    assert!(
        !text.contains("[CHAT]"),
        "video-only must NOT render [CHAT]"
    );
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
        emitted_messages: 4_500,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.contains("[CHAT]"), "must render [CHAT]");
    assert!(!text.contains("[VID]"), "chat-only must NOT render [VID]");
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
    assert!(text.contains("[DEL]"), "must render [DEL]");
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
    assert!(
        text.contains("[VID]"),
        "must render [VID] in interactive mode"
    );
    assert!(
        text.contains("[CHAT]"),
        "must render [CHAT] in interactive mode"
    );
    assert!(
        text.contains("[DEL]"),
        "must render [DEL] in interactive mode"
    );
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
