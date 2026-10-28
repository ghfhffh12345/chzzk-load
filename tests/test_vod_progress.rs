use std::path::Path;
use std::time::{Duration, Instant};

use chzzk_load::chzzk::DEFAULT_USER_AGENT;
use chzzk_load::consolidation::progress::{
    COLOR_BLUE, COLOR_CYAN, CoordinatorOutput, STYLE_DIM, STYLE_RESET, format_interactive_bar,
};
use chzzk_load::uploader::unlink_local_file_with_retry;
use chzzk_load::vod::aria2c::{
    build_aria2c_progressive_args, build_aria2c_segment_args, scan_completed_segments,
};
use chzzk_load::vod::progress::{
    COLOR_MAGENTA, VodChatProgressSnapshot, VodChatProgressUpdate, VodMilestoneTracker,
    VodMuxProgressSnapshot, VodProgressCoordinator, VodSpeedTracker, VodVideoProgressSnapshot,
    VodVideoProgressUpdate, format_time_hms, parse_aria2c_bytes, parse_aria2c_single_file_progress,
    parse_aria2c_speed,
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
        ..Default::default()
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
        ..Default::default()
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

#[test]
fn test_vod_rolling_speed_tracker_window_and_dynamic_rates() {
    let t0 = Instant::now();
    let mut tracker = VodSpeedTracker::new(t0);

    // Initial state: no speed calculated before window elapsed
    assert_eq!(tracker.current_speed(), None);

    // Sub-window update (500ms): elapsed < 1000ms, no speed computed
    let t_500ms = t0 + Duration::from_millis(500);
    assert_eq!(tracker.update_at(5_000_000, t_500ms), None);

    // 1.0s boundary reached with 10 MiB (10,485,760 bytes)
    let t_1000ms = t0 + Duration::from_millis(1000);
    assert_eq!(tracker.update_at(10_485_760, t_1000ms), Some("10.0 MB/s"));
    assert_eq!(tracker.current_speed(), Some("10.0 MB/s"));

    // Sub-window again (1500ms): retains previous speed
    let t_1500ms = t0 + Duration::from_millis(1500);
    assert_eq!(tracker.update_at(20_000_000, t_1500ms), Some("10.0 MB/s"));

    // 2.0s boundary reached: delta = 47,185,920 bytes (45 MiB in 1.0s) -> 45.0 MB/s
    let t_2000ms = t0 + Duration::from_millis(2000);
    assert_eq!(
        tracker.update_at(10_485_760 + 47_185_920, t_2000ms),
        Some("45.0 MB/s")
    );

    // Fractional delta calculation: 1.25s elapsed (at 3250ms), 15 MiB transferred -> 12.0 MB/s
    let t_3250ms = t0 + Duration::from_millis(3250);
    let bytes_3250 = 10_485_760 + 47_185_920 + 15_728_640;
    assert_eq!(tracker.update_at(bytes_3250, t_3250ms), Some("12.0 MB/s"));

    // Stalling: no new bytes at 4250ms (idle < 3s timeout) -> retains previous speed
    let t_4250ms = t0 + Duration::from_millis(4250);
    assert_eq!(tracker.update_at(bytes_3250, t_4250ms), Some("12.0 MB/s"));

    // Stalling timeout breached: 3s without progress at 6250ms -> decays to 0 B/s
    let t_6250ms = t0 + Duration::from_millis(6250);
    assert_eq!(tracker.update_at(bytes_3250, t_6250ms), Some("0 B/s"));

    // Resumption: 25 MiB downloaded over next 1.0s at 7250ms -> 25.0 MB/s
    let t_7250ms = t0 + Duration::from_millis(7250);
    let bytes_7250 = bytes_3250 + 26_214_400;
    assert_eq!(tracker.update_at(bytes_7250, t_7250ms), Some("25.0 MB/s"));
}

#[test]
fn test_aria2c_command_construction_browser_headers_and_k1m() {
    let staging_dir = Path::new("/test/staging");
    let input_file = Path::new("/test/staging/segments.txt");
    let cookie = Some("NID_AUT=test_aut_token; NID_SES=test_ses_token");

    let args = build_aria2c_segment_args(staging_dir, input_file, cookie);

    // Verify browser headers matching DEFAULT_USER_AGENT, Referer, and Origin
    assert!(
        args.contains(&format!("--header=User-Agent: {DEFAULT_USER_AGENT}")),
        "args must contain browser User-Agent header matching DEFAULT_USER_AGENT"
    );
    assert!(
        args.contains(&"--header=Referer: https://chzzk.naver.com/".to_string()),
        "args must contain Referer: https://chzzk.naver.com/ header"
    );
    assert!(
        args.contains(&"--header=Origin: https://chzzk.naver.com".to_string()),
        "args must contain Origin: https://chzzk.naver.com header"
    );

    // Verify -k1M minimum split size
    assert!(
        args.iter()
            .any(|a| a == "-k1M" || a == "-k 1M" || a == "--min-split-size=1M"),
        "args must contain -k1M to allow multi-connection chunking on smaller files"
    );

    // Verify cookie propagation
    assert!(
        args.contains(
            &"--header=Cookie: NID_AUT=test_aut_token; NID_SES=test_ses_token".to_string()
        ),
        "args must propagate cookie header"
    );

    // Verify standard aria2c operational flags
    assert!(args.contains(&"-j16".to_string()));
    assert!(args.contains(&"-x16".to_string()));
    assert!(args.contains(&"-s16".to_string()));
    assert!(args.contains(&"--file-allocation=none".to_string()));
    assert!(args.contains(&"--summary-interval=1".to_string()));
    assert!(args.contains(&"--dir".to_string()));
    assert!(args.contains(&staging_dir.to_string_lossy().to_string()));
    assert!(args.contains(&"--input-file".to_string()));
    assert!(args.contains(&input_file.to_string_lossy().to_string()));
}

#[tokio::test]
async fn test_aria2c_scan_completed_segments_memoization() {
    let temp_dir = std::env::temp_dir().join(format!("test_aria2_memo_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    // segment_00000.ts: completed (exists, size 1000, no .aria2)
    tokio::fs::write(temp_dir.join("segment_00000.ts"), vec![0x47; 1000])
        .await
        .unwrap();

    // segment_00001.ts: downloading (exists, but .aria2 control file exists)
    tokio::fs::write(temp_dir.join("segment_00001.ts"), vec![0x47; 500])
        .await
        .unwrap();
    tokio::fs::write(temp_dir.join("segment_00001.ts.aria2"), b"control")
        .await
        .unwrap();

    // segment_00002.ts: empty (0 bytes)
    tokio::fs::write(temp_dir.join("segment_00002.ts"), b"")
        .await
        .unwrap();

    let mut cache = std::collections::HashSet::new();
    let mut cached_bytes = 0u64;

    // First scan: should find only segment_00000.ts
    let (completed, bytes) =
        scan_completed_segments(&temp_dir, &mut cache, &mut cached_bytes).await;
    assert_eq!(completed, 1);
    assert_eq!(bytes, 1000);
    assert!(cache.contains("segment_00000.ts"));

    // Now remove segment_00000.ts from disk to prove cache memoization prevents stat calls
    unlink_local_file_with_retry(&temp_dir.join("segment_00000.ts"))
        .await
        .unwrap();

    // Second scan: cache keeps segment_00000.ts without touching disk
    let (completed2, bytes2) =
        scan_completed_segments(&temp_dir, &mut cache, &mut cached_bytes).await;
    assert_eq!(completed2, 1);
    assert_eq!(bytes2, 1000);

    // Now complete segment_00001.ts by removing its .aria2 file and resizing to 1000 bytes
    unlink_local_file_with_retry(&temp_dir.join("segment_00001.ts.aria2"))
        .await
        .unwrap();
    tokio::fs::write(temp_dir.join("segment_00001.ts"), vec![0x47; 1000])
        .await
        .unwrap();

    let (completed3, bytes3) =
        scan_completed_segments(&temp_dir, &mut cache, &mut cached_bytes).await;
    assert_eq!(completed3, 2);
    assert_eq!(bytes3, 2000);
    assert!(cache.contains("segment_00001.ts"));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[test]
fn test_aria2c_progressive_command_construction() {
    let staging_dir = Path::new("/var/staging");
    let url = "https://chzzk.naver.com/video/progressive_1080p.mp4?token=xyz";
    let out_filename = "vod.mp4.part";
    let cookie = Some("NID_AUT=aut_token; NID_SES=ses_token");

    let args = build_aria2c_progressive_args(staging_dir, url, out_filename, cookie);

    // Concurrency and chunking flags
    assert!(args.contains(&"-x16".to_string()) || args.contains(&"-x 16".to_string()));
    assert!(args.contains(&"-s16".to_string()) || args.contains(&"-s 16".to_string()));
    assert!(args.contains(&"-k1M".to_string()) || args.contains(&"-k 1M".to_string()));
    assert!(args.contains(&"-c".to_string()));
    assert!(args.contains(&"--allow-overwrite=true".to_string()));
    assert!(args.contains(&"--auto-file-renaming=false".to_string()));
    assert!(args.contains(&"--file-allocation=none".to_string()));
    assert!(args.contains(&"--summary-interval=1".to_string()));

    // Target directory and output file
    assert!(
        args.contains(&format!("--dir={}", staging_dir.display()))
            || args.contains(&"--dir".to_string())
    );
    assert!(args.contains(&format!("--out={out_filename}")) || args.contains(&"--out".to_string()));

    // Browser headers
    assert!(args.contains(&format!("--header=User-Agent: {DEFAULT_USER_AGENT}")));
    assert!(args.contains(&"--header=Referer: https://chzzk.naver.com/".to_string()));
    assert!(args.contains(&"--header=Origin: https://chzzk.naver.com".to_string()));

    // Auth cookies
    assert!(args.contains(&"--header=Cookie: NID_AUT=aut_token; NID_SES=ses_token".to_string()));

    // Final URL
    assert_eq!(args.last().unwrap(), url);
}

#[test]
fn test_parse_aria2c_bytes() {
    assert_eq!(parse_aria2c_bytes("0B"), Some(0));
    assert_eq!(parse_aria2c_bytes("56B"), Some(56));
    assert_eq!(parse_aria2c_bytes("1024B"), Some(1024));
    assert_eq!(parse_aria2c_bytes("1KiB"), Some(1024));
    assert_eq!(parse_aria2c_bytes("1.2MiB"), Some(1_258_291));
    assert_eq!(parse_aria2c_bytes("5.0MiB"), Some(5_242_880));
    assert_eq!(parse_aria2c_bytes("1.5GiB"), Some(1_610_612_736));
    assert_eq!(parse_aria2c_bytes("2GiB"), Some(2_147_483_648));
    assert_eq!(parse_aria2c_bytes(""), None);
    assert_eq!(parse_aria2c_bytes("invalid"), None);
}

#[test]
fn test_parse_aria2c_single_file_progress() {
    // Standard progress line with DL and ETA
    let line1 = "[#2089b0 1.2MiB/5.0MiB(24%) CN:16 DL:24.5MiB ETA:1m30s]";
    let prog1 = parse_aria2c_single_file_progress(line1).expect("must parse line 1");
    match prog1 {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 1_258_291);
            assert_eq!(total_bytes, Some(5_242_880));
            assert_eq!(speed, Some("24.5MiB/s".to_string()));
            assert_eq!(eta, Some("1m30s".to_string()));
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // High throughput gigabyte progress line
    let line2 = "[#2089b0 1.2GiB/1.5GiB(80%) CN:16 DL:45.2MiB/s ETA:32s]";
    let prog2 = parse_aria2c_single_file_progress(line2).expect("must parse line 2");
    match prog2 {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 1_288_490_189);
            assert_eq!(total_bytes, Some(1_610_612_736));
            assert_eq!(speed, Some("45.2MiB/s".to_string()));
            assert_eq!(eta, Some("32s".to_string()));
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // Finished progress line without DL/ETA
    let line3 = "[#2089b0 56B/56B(100%)]";
    let prog3 = parse_aria2c_single_file_progress(line3).expect("must parse line 3");
    match prog3 {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 56);
            assert_eq!(total_bytes, Some(56));
            assert_eq!(speed, None);
            assert_eq!(eta, None);
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // Starting line with 0 bytes downloaded
    let line4 = "[#1 0B/1.0GiB(0%) CN:1]";
    let prog4 = parse_aria2c_single_file_progress(line4).expect("must parse line 4");
    match prog4 {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 0);
            assert_eq!(total_bytes, Some(1_073_741_824));
            assert_eq!(speed, None);
            assert_eq!(eta, None);
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // Multi-download HLS lines must be rejected (single-file progress only)
    let multi_line = "[#1 1.0MiB/5.0MiB(20%)] [#2 2.0MiB/5.0MiB(40%)]";
    assert_eq!(parse_aria2c_single_file_progress(multi_line), None);

    // Carriage-return delimited status updates from terminal stream: take latest update
    let cr_line =
        "[#2089b0 1.0MiB/5.0MiB(20%)]\r[#2089b0 2.5MiB/5.0MiB(50%) CN:16 DL:12.3MiB ETA:10s]";
    let prog_cr = parse_aria2c_single_file_progress(cr_line).expect("must parse CR-delimited line");
    match prog_cr {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 2_621_440);
            assert_eq!(total_bytes, Some(5_242_880));
            assert_eq!(speed, Some("12.3MiB/s".to_string()));
            assert_eq!(eta, Some("10s".to_string()));
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // Line containing [FileAlloc:#gid ...] block alongside progress
    let alloc_line = "[#f24103 0B/9.1GiB(0%) CN:1 DL:0B] [FileAlloc:#f24103 4.8GiB/9.1GiB(53%)]";
    let prog_alloc =
        parse_aria2c_single_file_progress(alloc_line).expect("must parse FileAlloc line");
    match prog_alloc {
        VodVideoProgressUpdate::ProgressiveProgress {
            bytes,
            total_bytes,
            speed,
            eta,
        } => {
            assert_eq!(bytes, 0);
            assert_eq!(total_bytes, Some(9_771_050_598));
            assert_eq!(speed, Some("0B/s".to_string()));
            assert_eq!(eta, None);
        }
        other => panic!("Unexpected update type: {other:?}"),
    }

    // Arbitrary logs must be rejected
    assert_eq!(
        parse_aria2c_single_file_progress("Downloading something..."),
        None
    );
}

#[test]
fn test_vod_progressive_progress_snapshot_formatting() {
    let snapshot = VodVideoProgressSnapshot {
        segments_downloaded: 0,
        total_segments: 0,
        bytes_downloaded: 3_145_728,  // 3.0 MB
        total_bytes: Some(5_242_880), // 5.0 MB
        speed: Some("15.0 MB/s".to_string()),
        eta: Some("20s".to_string()),
        is_progressive: true,
    };

    assert_eq!(snapshot.pct(), 60);
    assert_eq!(
        snapshot.format_non_interactive(),
        "[INFO] [VOD] [VID] Download progress: 60% (3.0 MB / 5.0 MB) speed: 15.0 MB/s"
    );
    assert!(
        snapshot
            .metric_suffix()
            .contains("60% (3.0 MB / 5.0 MB) 15.0 MB/s ETA: 20s")
    );
    assert!(snapshot.format_interactive(20).contains("VID"));

    // Progressive snapshot with unknown total bytes
    let snapshot_unknown = VodVideoProgressSnapshot {
        segments_downloaded: 0,
        total_segments: 0,
        bytes_downloaded: 1_048_576,
        total_bytes: None,
        speed: Some("5.0 MB/s".to_string()),
        eta: None,
        is_progressive: true,
    };
    assert_eq!(snapshot_unknown.pct(), 0);
    assert_eq!(
        snapshot_unknown.format_non_interactive(),
        "[INFO] [VOD] [VID] Download progress: 0% (1.0 MB / unknown) speed: 5.0 MB/s"
    );
}

#[tokio::test]
async fn test_vod_progress_coordinator_with_progressive_updates() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    let mut session =
        coordinator.start_progressive_session(Some(10_485_760), 7_200_000, true, false);
    let v_tx = session.video_sender().unwrap();

    let _ = v_tx.send(VodVideoProgressUpdate::ProgressiveProgress {
        bytes: 5_242_880,
        total_bytes: Some(10_485_760),
        speed: Some("25.0 MB/s".to_string()),
        eta: Some("10s".to_string()),
    });

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    let vid_lines: Vec<&str> = rows.iter().copied().filter(|l| l.contains("VID")).collect();
    assert!(!vid_lines.is_empty(), "must render progressive VID line");
    let last_vid = vid_lines.last().unwrap();
    assert!(last_vid.contains("50%"), "must display 50% progress");
    assert!(
        last_vid.contains("5.0 MB / 10.0 MB"),
        "must display byte progress"
    );
}

#[tokio::test]
async fn test_vod_progressive_speed_stabilization_preserves_aria2c_speed_across_ticks() {
    let (output, buf) = CoordinatorOutput::buffer();
    let coordinator = VodProgressCoordinator::with_output(output, true).with_terminal_width(80);

    let mut session =
        coordinator.start_progressive_session(Some(10_485_760), 7_200_000, true, false);
    let v_tx = session.video_sender().unwrap();

    let _ = v_tx.send(VodVideoProgressUpdate::ProgressiveProgress {
        bytes: 5_242_880,
        total_bytes: Some(10_485_760),
        speed: Some("25.0MiB/s".to_string()),
        eta: Some("10s".to_string()),
    });

    // Allow multiple 100ms coordinator ticks to fire
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    session.finish(true).await;

    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    let rows = terminal_rows(&text);
    let vid_lines: Vec<&str> = rows.iter().copied().filter(|l| l.contains("VID")).collect();
    assert!(!vid_lines.is_empty(), "must render progressive VID line");
    let last_vid = vid_lines.last().unwrap();
    assert!(
        last_vid.contains("25.0MiB/s"),
        "Aria2c reported speed must be preserved across ticks without flicker: {last_vid}"
    );
}
