use std::time::{Duration, Instant};

use chzzk_load::consolidation::progress::{
    COLOR_BLUE, COLOR_CYAN, CoordinatorOutput, STYLE_DIM, STYLE_RESET, format_interactive_bar,
};
use chzzk_load::vod::progress::{
    COLOR_MAGENTA, VodChatProgressSnapshot, VodChatProgressUpdate, VodMilestoneTracker,
    VodMuxProgressSnapshot, VodProgressCoordinator, VodVideoProgressSnapshot,
    VodVideoProgressUpdate, format_time_hms, parse_aria2c_speed,
};

fn visible_width(s: &str) -> usize {
    let mut in_escape = false;
    let mut count = 0;
    for ch in s.chars() {
        if ch == '\x1b' {
            in_escape = true;
        } else if in_escape {
            if ch.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if ch != '\r' && ch != '\n' {
            count += 1;
        }
    }
    count
}

fn terminal_rows(s: &str) -> Vec<&str> {
    s.split('\n')
        .flat_map(|line| line.split("\x1b[1A"))
        .flat_map(|line| line.split('\r'))
        .filter(|row| !row.is_empty())
        .collect()
}

#[test]
fn test_vod_time_formatting() {
    assert_eq!(format_time_hms(0), "00:00:00");
    assert_eq!(format_time_hms(999), "00:00:00");
    assert_eq!(format_time_hms(1_000), "00:00:01");
    assert_eq!(format_time_hms(59_000), "00:00:59");
    assert_eq!(format_time_hms(60_000), "00:01:00");
    assert_eq!(format_time_hms(61_000), "00:01:01");
    assert_eq!(format_time_hms(3_599_000), "00:59:59");
    assert_eq!(format_time_hms(3_600_000), "01:00:00");
    assert_eq!(format_time_hms(3_661_000), "01:01:01");
    assert_eq!(format_time_hms(7_322_000), "02:02:02");
    assert_eq!(format_time_hms(36_000_000), "10:00:00");
}

#[test]
fn test_parse_aria2c_speed() {
    assert_eq!(
        parse_aria2c_speed("[#2089b0 1.2MiB/5.0MiB(24%) CN:4 DL:2.4MiB]"),
        Some("2.4MiB/s".to_string())
    );
    assert_eq!(
        parse_aria2c_speed("[#2089b0 1.2MiB/5.0MiB(24%) CN:4 DL:15.2MiB/s]"),
        Some("15.2MiB/s".to_string())
    );
    assert_eq!(
        parse_aria2c_speed("(DL:500KiB)"),
        Some("500KiB/s".to_string())
    );
    assert_eq!(
        parse_aria2c_speed("DL:1.2MiB ETA:5s"),
        Some("1.2MiB/s".to_string())
    );
    assert_eq!(parse_aria2c_speed("some random line"), None);
}

#[test]
fn test_vod_video_progress_snapshot_formatting() {
    let snapshot = VodVideoProgressSnapshot {
        segments_downloaded: 6,
        total_segments: 10,
        bytes_downloaded: 126_353_408,
        speed: Some("14.5 MB/s".to_string()),
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        format!(
            "{COLOR_CYAN} VID  {STYLE_RESET}{}{STYLE_DIM} 60% (6/10 segments, 120.5 MB) 14.5 MB/s{STYLE_RESET}",
            format_interactive_bar(20, 60)
        )
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [VOD] [VID] Download progress: 60% (6/10 segments, 120.5 MB) speed: 14.5 MB/s"
    );

    let snapshot_no_speed = VodVideoProgressSnapshot {
        segments_downloaded: 2,
        total_segments: 10,
        bytes_downloaded: 26_633_830,
        speed: None,
    };
    assert_eq!(snapshot_no_speed.pct(), 20);
    assert_eq!(
        snapshot_no_speed.format_interactive(20),
        format!(
            "{COLOR_CYAN} VID  {STYLE_RESET}{}{STYLE_DIM} 20% (2/10 segments, 25.4 MB){STYLE_RESET}",
            format_interactive_bar(20, 20)
        )
    );
    assert_eq!(
        snapshot_no_speed.format_non_interactive(),
        "[INFO] [VOD] [VID] Download progress: 20% (2/10 segments, 25.4 MB)"
    );
}

#[test]
fn test_vod_chat_progress_snapshot_formatting() {
    let snapshot = VodChatProgressSnapshot {
        current_time_ms: 4_320_000, // 01:12:00
        duration_ms: 7_200_000,     // 02:00:00
        emitted_messages: 12_345,
    };
    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_interactive(20),
        format!(
            "{COLOR_BLUE} CHAT {STYLE_RESET}{}{STYLE_DIM} 60% (01:12:00 / 02:00:00, 12,345 msgs){STYLE_RESET}",
            format_interactive_bar(20, 60)
        )
    );
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [VOD] [CHAT] Pagination progress: 60% (01:12:00 / 02:00:00, 12,345 msgs)"
    );
}

#[test]
fn test_vod_mux_phase_snapshot_formatting() {
    let active = VodMuxProgressSnapshot {
        is_active: true,
        is_finished: false,
    };
    assert_eq!(
        active.format_interactive(20),
        format!(
            "{COLOR_MAGENTA} MUX  {STYLE_RESET}{}{STYLE_DIM} Assembling MP4 container...{STYLE_RESET}",
            format_interactive_bar(20, 100)
        )
    );
    assert_eq!(
        VodMuxProgressSnapshot::format_non_interactive_start(),
        "[INFO] [VOD] [MUX] Assembling MP4 container with FFmpeg concat demuxer..."
    );

    let finished = VodMuxProgressSnapshot {
        is_active: true,
        is_finished: true,
    };
    assert_eq!(
        finished.format_interactive(20),
        format!(
            "{COLOR_MAGENTA} MUX  {STYLE_RESET}{}{STYLE_DIM} Container assembly complete{STYLE_RESET}",
            format_interactive_bar(20, 100)
        )
    );
    assert_eq!(
        VodMuxProgressSnapshot::format_non_interactive_finish(),
        "[INFO] [VOD] [MUX] Container assembly complete."
    );
}

#[test]
fn test_vod_milestone_tracker_10s_heartbeat_and_20pct() {
    let t0 = Instant::now();
    let mut tracker = VodMilestoneTracker::new(t0);

    // Initial log at 0%
    assert!(tracker.should_log_at(0, 0, t0));

    // Stalled at 5% after 5 seconds: should NOT log yet (under 10s)
    assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(5)));

    // Stalled at 5% after 11 seconds: 10s heartbeat triggers!
    assert!(tracker.should_log_at(5, 50, t0 + Duration::from_secs(11)));

    // Stalled at 5% after 15 seconds (only 4s since 11s): should NOT log
    assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(15)));

    // 20% milestone reached at 16 seconds: boundary triggers immediately
    assert!(tracker.should_log_at(20, 200, t0 + Duration::from_secs(16)));

    // 40% milestone reached at 17 seconds: boundary triggers immediately
    assert!(tracker.should_log_at(40, 400, t0 + Duration::from_secs(17)));

    // 100% completion milestone
    assert!(tracker.should_log_at(100, 1000, t0 + Duration::from_secs(25)));
}

#[tokio::test]
async fn test_interactive_dual_track_vid_and_chat() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    let mut session = coordinator.start_session(10, 7_200_000, true, true);
    let v_tx = session.video_sender().unwrap();
    let c_tx = session.chat_sender().unwrap();

    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 6,
        bytes: 126_353_408,
    });
    let _ = v_tx.send(VodVideoProgressUpdate::Speed("14.5 MB/s".to_string()));

    let _ = c_tx.send(VodChatProgressUpdate {
        current_time_ms: 4_320_000,
        emitted_messages: 12_345,
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    let vid_lines: Vec<&str> = rows.iter().copied().filter(|l| l.contains("VID")).collect();
    let chat_lines: Vec<&str> = rows
        .iter()
        .copied()
        .filter(|l| l.contains("CHAT"))
        .collect();

    assert!(!vid_lines.is_empty(), "must render VID line");
    assert!(!chat_lines.is_empty(), "must render CHAT line");

    let last_vid = vid_lines.last().unwrap();
    let last_chat = chat_lines.last().unwrap();

    let vid_vis = visible_width(last_vid);
    let chat_vis = visible_width(last_chat);
    assert!(
        vid_vis <= 79,
        "VID line visible length ({vid_vis}) must never exceed 79 columns on 80-col terminal"
    );
    assert!(
        chat_vis <= 79,
        "CHAT line visible length ({chat_vis}) must never exceed 79 columns on 80-col terminal"
    );
}

#[tokio::test]
async fn test_interactive_mux_transition() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    let mut session = coordinator.start_session(10, 7_200_000, true, true);
    let v_tx = session.video_sender().unwrap();
    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 10,
        bytes: 200_000_000,
    });

    // Transition to MUX phase
    session.transition_to_mux();
    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    let mux_lines: Vec<&str> = rows.iter().copied().filter(|l| l.contains("MUX")).collect();

    assert!(
        !mux_lines.is_empty(),
        "must render MUX line after transition"
    );
    let last_mux = mux_lines.last().unwrap();
    let mux_vis = visible_width(last_mux);
    assert!(
        mux_vis <= 79,
        "MUX line visible length ({mux_vis}) must never exceed 79 columns on 80-col terminal"
    );
}

#[tokio::test]
async fn test_headless_non_tty_milestone_output() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, false);

    let mut session = coordinator.start_session(10, 7_200_000, true, true);
    let v_tx = session.video_sender().unwrap();
    let c_tx = session.chat_sender().unwrap();

    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 5,
        bytes: 100_000_000,
    });
    let _ = c_tx.send(VodChatProgressUpdate {
        current_time_ms: 3_600_000,
        emitted_messages: 5_000,
    });

    session.transition_to_mux();
    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(
        !text.contains('\x1b'),
        "Headless output must never contain ANSI escape codes"
    );

    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("[INFO] [VOD] [VID] Download progress:")),
        "Must contain non-interactive VID progress line"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("[INFO] [VOD] [CHAT] Pagination progress:")),
        "Must contain non-interactive CHAT progress line"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("[INFO] [VOD] [MUX] Assembling MP4 container")),
        "Must contain non-interactive MUX start line"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("[INFO] [VOD] [MUX] Container assembly complete")),
        "Must contain non-interactive MUX finish line"
    );
}

#[tokio::test]
async fn test_terminal_geometry_responsive_bar_width() {
    // 1. 120-col terminal: full expansion to MAX_BAR_WIDTH (40)
    let (out_120, buf_120) = CoordinatorOutput::buffer();
    let coord_120 = VodProgressCoordinator::with_output(out_120, true).with_terminal_width(120);
    let mut session_120 = coord_120.start_session(10, 7_200_000, true, true);
    session_120.finish(true).await;
    let text_120 = String::from_utf8(buf_120.lock().unwrap().clone()).unwrap();
    for row in terminal_rows(&text_120) {
        let vis = visible_width(row);
        assert!(vis <= 119, "Row visible width must not exceed 119: {row:?}");
    }

    // 2. 60-col terminal: compression
    let (out_60, buf_60) = CoordinatorOutput::buffer();
    let coord_60 = VodProgressCoordinator::with_output(out_60, true).with_terminal_width(60);
    let mut session_60 = coord_60.start_session(10, 7_200_000, true, true);
    session_60.finish(true).await;
    let text_60 = String::from_utf8(buf_60.lock().unwrap().clone()).unwrap();
    for row in terminal_rows(&text_60) {
        let vis = visible_width(row);
        assert!(vis <= 59, "Row visible width must not exceed 59: {row:?}");
    }

    // 3. 45-col severely constrained terminal: clamps to MIN_BAR_WIDTH (15)
    let (out_45, buf_45) = CoordinatorOutput::buffer();
    let coord_45 = VodProgressCoordinator::with_output(out_45, true).with_terminal_width(45);
    let mut session_45 = coord_45.start_session(10, 7_200_000, true, true);
    session_45.finish(true).await;
    let text_45 = String::from_utf8(buf_45.lock().unwrap().clone()).unwrap();
    assert!(!text_45.is_empty());
}

#[tokio::test]
async fn test_interactive_video_only_mode() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    // has_video = true, has_chat = false (--no-chat)
    let mut session = coordinator.start_session(10, 0, true, false);
    let v_tx = session.video_sender().unwrap();
    assert!(
        session.chat_sender().is_none(),
        "Chat sender must be None when has_chat is false"
    );

    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 5,
        bytes: 50_000_000,
    });
    session.transition_to_mux();
    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    assert!(
        rows.iter().any(|r| r.contains("VID")),
        "Must render VID row in video-only mode"
    );
    assert!(
        rows.iter().any(|r| r.contains("MUX")),
        "Must render MUX row in video-only mode"
    );
    assert!(
        !rows.iter().any(|r| r.contains("CHAT")),
        "Must NOT render CHAT row in video-only mode"
    );
}

#[tokio::test]
async fn test_interactive_chat_only_mode() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    // has_video = false, has_chat = true
    let mut session = coordinator.start_session(0, 3_600_000, false, true);
    assert!(
        session.video_sender().is_none(),
        "Video sender must be None when has_video is false"
    );
    let c_tx = session.chat_sender().unwrap();

    let _ = c_tx.send(VodChatProgressUpdate {
        current_time_ms: 1_800_000,
        emitted_messages: 500,
    });
    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    assert!(
        rows.iter().any(|r| r.contains("CHAT")),
        "Must render CHAT row in chat-only mode"
    );
    assert!(
        !rows.iter().any(|r| r.contains("VID")),
        "Must NOT render VID row in chat-only mode"
    );
}

#[test]
fn test_fixed_bar_width_override() {
    let coordinator = VodProgressCoordinator::new(true)
        .with_terminal_width(120)
        .with_bar_width(25);

    assert_eq!(coordinator.resolve_bar_width(10), 25);
    assert_eq!(coordinator.resolve_bar_width(50), 25);
}

#[test]
fn test_terminal_probe_error_and_zero_fallback() {
    // Error fallback
    let err_coord = VodProgressCoordinator::new(true)
        .with_terminal_probe(|| Err(std::io::Error::other("ioctl failed")));
    assert_eq!(err_coord.query_terminal_cols(), None);
    assert_eq!(err_coord.resolve_bar_width(10), 40);

    // Zero columns fallback
    let zero_coord = VodProgressCoordinator::new(true).with_terminal_probe(|| Ok(0));
    assert_eq!(zero_coord.query_terminal_cols(), None);
    assert_eq!(zero_coord.resolve_bar_width(10), 40);

    // Non-TTY fallback
    let non_tty = VodProgressCoordinator::new(false);
    assert_eq!(non_tty.query_terminal_cols(), None);
}

#[tokio::test(start_paused = true)]
async fn test_dynamic_terminal_resize_simulation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};

    let cols = Arc::new(AtomicU16::new(120));
    let cols_clone = cols.clone();

    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true)
        .with_terminal_probe(move || Ok(cols_clone.load(Ordering::SeqCst)));

    let mut session = coordinator.start_session(10, 7_200_000, true, true);
    let v_tx = session.video_sender().unwrap();

    // Render at 120 cols
    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 3,
        bytes: 30_000_000,
    });
    tokio::time::advance(Duration::from_millis(150)).await;

    // Simulate terminal window resized down to 60 cols
    cols.store(60, Ordering::SeqCst);
    let _ = v_tx.send(VodVideoProgressUpdate::SegmentsDownloaded {
        segments: 6,
        bytes: 60_000_000,
    });
    tokio::time::advance(Duration::from_millis(150)).await;

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    assert!(!rows.is_empty());

    // After resize to 60 columns, the final rendered rows must fit within 59 columns
    let last_vid = rows.iter().rfind(|r| r.contains("VID")).unwrap();
    let vis = visible_width(last_vid);
    assert!(
        vis <= 59,
        "After terminal resize to 60 columns, row width ({vis}) must not exceed 59: {last_vid:?}"
    );
}
