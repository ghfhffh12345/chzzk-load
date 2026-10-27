use std::io::IsTerminal;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub use crate::consolidation::progress::{
    BADGE_LEN, COLOR_BLUE, COLOR_CYAN, COLOR_DIVIDER, COLOR_GREEN, CoordinatorOutput,
    DEFAULT_FALLBACK_BAR_WIDTH, MAX_BAR_WIDTH, MIN_BAR_WIDTH, ProgressBarWidthMode, SAFETY_MARGIN,
    STYLE_DIM, STYLE_RESET, TerminalProbe, format_bytes, format_interactive_bar,
    format_number_with_commas, format_progress_bar,
};

pub const COLOR_MAGENTA: &str = "\x1b[38;2;185;144;181m"; // #b990b5 (theme::MAGENTA)

/// Formats millisecond duration into standard `HH:MM:SS` notation.
pub fn format_time_hms(ms: u64) -> String {
    let total_secs = ms / 1000;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    format!("{hours:02}:{mins:02}:{secs:02}")
}

/// Extracts transfer speed from an aria2c progress line if present.
///
/// Example inputs:
/// - `"[#2089b0 1.2MiB/5.0MiB(24%) CN:4 DL:2.4MiB]"` -> `Some("2.4MiB/s".to_string())`
/// - `"[#2089b0 1.2MiB/5.0MiB(24%) CN:4 DL:15.2MiB/s]"` -> `Some("15.2MiB/s".to_string())`
/// - `"(DL:500KiB)"` -> `Some("500KiB/s".to_string())`
pub fn parse_aria2c_speed(line: &str) -> Option<String> {
    if let Some(idx) = line.find("DL:") {
        let rest = &line[idx + 3..];
        let token = rest
            .split(|c: char| c.is_whitespace() || c == ']' || c == ')')
            .next()?
            .trim();
        if !token.is_empty() {
            let formatted = if token.ends_with("/s") {
                token.to_string()
            } else {
                format!("{token}/s")
            };
            return Some(formatted);
        }
    }
    None
}

/// Tracks rolling byte throughput over a configurable time window (default: 1 second).
///
/// Computes aggregate download rate (`delta_bytes / delta_seconds`) across all concurrent
/// connections, avoiding deceptive single-chunk token extraction. Automatically decays
/// speed to "0 B/s" when progress is stalled longer than `idle_timeout` (default: 3 seconds).
#[derive(Debug, Clone)]
pub struct VodSpeedTracker {
    last_sample_instant: Instant,
    last_sample_bytes: u64,
    last_change_instant: Instant,
    current_speed: Option<String>,
    window_duration: Duration,
    idle_timeout: Duration,
}

impl VodSpeedTracker {
    pub fn new(start_time: Instant) -> Self {
        Self::with_config(start_time, Duration::from_secs(1), Duration::from_secs(3))
    }

    pub fn with_config(start_time: Instant, window: Duration, idle_timeout: Duration) -> Self {
        Self {
            last_sample_instant: start_time,
            last_sample_bytes: 0,
            last_change_instant: start_time,
            current_speed: None,
            window_duration: window,
            idle_timeout,
        }
    }

    pub fn update_at(&mut self, current_bytes: u64, now: Instant) -> Option<&str> {
        let elapsed = now.saturating_duration_since(self.last_sample_instant);
        if elapsed >= self.window_duration {
            let delta_bytes = current_bytes.saturating_sub(self.last_sample_bytes);
            let elapsed_secs = elapsed.as_secs_f64();
            if elapsed_secs > 0.0 {
                let bps = (delta_bytes as f64 / elapsed_secs).round() as u64;
                if delta_bytes > 0 {
                    self.current_speed = Some(format!("{}/s", format_bytes(bps)));
                    self.last_change_instant = now;
                } else if now.saturating_duration_since(self.last_change_instant)
                    >= self.idle_timeout
                {
                    self.current_speed = Some("0 B/s".to_string());
                }
            }
            self.last_sample_instant = now;
            self.last_sample_bytes = current_bytes;
        } else if now.saturating_duration_since(self.last_change_instant) >= self.idle_timeout
            && self.current_speed.is_some()
        {
            self.current_speed = Some("0 B/s".to_string());
        }
        self.current_speed.as_deref()
    }

    pub fn update(&mut self, current_bytes: u64) -> Option<&str> {
        self.update_at(current_bytes, Instant::now())
    }

    pub fn current_speed(&self) -> Option<&str> {
        self.current_speed.as_deref()
    }
}

/// Progress metrics for VOD segment downloads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VodVideoProgressSnapshot {
    pub segments_downloaded: usize,
    pub total_segments: usize,
    pub bytes_downloaded: u64,
    pub speed: Option<String>,
}

impl VodVideoProgressSnapshot {
    pub fn pct(&self) -> u16 {
        if self.total_segments == 0 {
            100
        } else {
            ((self.segments_downloaded as f64 / self.total_segments as f64) * 100.0)
                .round()
                .min(100.0) as u16
        }
    }

    pub fn metric_suffix(&self) -> String {
        let pct = self.pct();
        let bytes_str = format_bytes(self.bytes_downloaded);
        let speed_suffix = match &self.speed {
            Some(s) if !s.trim().is_empty() => format!(" {s}"),
            _ => String::new(),
        };
        format!(
            " {pct}% ({}/{total} segments, {bytes_str}){speed_suffix}",
            self.segments_downloaded,
            total = self.total_segments
        )
    }

    pub fn metric_suffix_len(&self) -> usize {
        self.metric_suffix().chars().count()
    }

    pub fn format_interactive(&self, bar_width: usize) -> String {
        let pct = self.pct();
        let bar = format_interactive_bar(bar_width, pct);
        let suffix = self.metric_suffix();
        format!("{COLOR_CYAN} VID  {STYLE_RESET}{bar}{STYLE_DIM}{suffix}{STYLE_RESET}")
    }

    pub fn format_non_interactive(&self) -> String {
        let pct = self.pct();
        let bytes_str = format_bytes(self.bytes_downloaded);
        let speed_suffix = match &self.speed {
            Some(s) if !s.trim().is_empty() => format!(" speed: {s}"),
            _ => String::new(),
        };
        format!(
            "[INFO] [VOD] [VID] Download progress: {pct}% ({}/{total} segments, {bytes_str}){speed_suffix}",
            self.segments_downloaded,
            total = self.total_segments
        )
    }
}

/// Progress metrics for timeline-synchronized VOD chat pagination.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VodChatProgressSnapshot {
    pub current_time_ms: u64,
    pub duration_ms: u64,
    pub emitted_messages: usize,
}

impl VodChatProgressSnapshot {
    pub fn pct(&self) -> u16 {
        if self.duration_ms == 0 {
            100
        } else {
            ((self.current_time_ms.min(self.duration_ms) as f64 / self.duration_ms as f64) * 100.0)
                .round()
                .min(100.0) as u16
        }
    }

    pub fn metric_suffix(&self) -> String {
        let pct = self.pct();
        let current_hms = format_time_hms(self.current_time_ms);
        let total_hms = format_time_hms(self.duration_ms);
        let msgs_str = format_number_with_commas(self.emitted_messages);
        format!(" {pct}% ({current_hms} / {total_hms}, {msgs_str} msgs)")
    }

    pub fn metric_suffix_len(&self) -> usize {
        self.metric_suffix().chars().count()
    }

    pub fn format_interactive(&self, bar_width: usize) -> String {
        let pct = self.pct();
        let bar = format_interactive_bar(bar_width, pct);
        let suffix = self.metric_suffix();
        format!("{COLOR_BLUE} CHAT {STYLE_RESET}{bar}{STYLE_DIM}{suffix}{STYLE_RESET}")
    }

    pub fn format_non_interactive(&self) -> String {
        let pct = self.pct();
        let current_hms = format_time_hms(self.current_time_ms);
        let total_hms = format_time_hms(self.duration_ms);
        let msgs_str = format_number_with_commas(self.emitted_messages);
        format!(
            "[INFO] [VOD] [CHAT] Pagination progress: {pct}% ({current_hms} / {total_hms}, {msgs_str} msgs)"
        )
    }
}

/// Status phase for FFmpeg container assembly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VodMuxProgressSnapshot {
    pub is_active: bool,
    pub is_finished: bool,
}

impl VodMuxProgressSnapshot {
    pub fn metric_suffix(&self) -> &'static str {
        if self.is_finished {
            " Container assembly complete"
        } else {
            " Assembling MP4 container..."
        }
    }

    pub fn metric_suffix_len(&self) -> usize {
        self.metric_suffix().chars().count()
    }

    pub fn format_interactive(&self, bar_width: usize) -> String {
        let bar = format_interactive_bar(bar_width, 100);
        let suffix = self.metric_suffix();
        format!("{COLOR_MAGENTA} MUX  {STYLE_RESET}{bar}{STYLE_DIM}{suffix}{STYLE_RESET}")
    }

    pub fn format_non_interactive_start() -> &'static str {
        "[INFO] [VOD] [MUX] Assembling MP4 container with FFmpeg concat demuxer..."
    }

    pub fn format_non_interactive_finish() -> &'static str {
        "[INFO] [VOD] [MUX] Container assembly complete."
    }
}

/// Tracks milestone triggers for non-interactive / headless VOD mode.
///
/// Triggers on:
/// - Initial 0%
/// - 20% milestone boundaries: 20%, 40%, 60%, 80%, 100%
/// - 10-second heartbeat since last log to prove liveness (as specified in Issue #83)
#[derive(Debug, Clone)]
pub struct VodMilestoneTracker {
    last_logged_milestone: Option<u16>,
    last_logged_at: Instant,
    last_progress_value: u64,
}

impl VodMilestoneTracker {
    pub fn new(start_time: Instant) -> Self {
        Self {
            last_logged_milestone: None,
            last_logged_at: start_time,
            last_progress_value: 0,
        }
    }

    pub fn should_log_at(&mut self, current_pct: u16, progress_val: u64, now: Instant) -> bool {
        let clamped_pct = current_pct.min(100);
        let current_milestone = (clamped_pct / 20) * 20;

        let is_new_milestone = match self.last_logged_milestone {
            None => true,
            Some(last) => current_milestone > last || (clamped_pct == 100 && last < 100),
        };

        if is_new_milestone {
            self.last_logged_milestone = Some(if clamped_pct == 100 {
                100
            } else {
                current_milestone
            });
            self.last_logged_at = now;
            self.last_progress_value = progress_val;
            return true;
        }

        // 10-second heartbeat since last log
        let elapsed = now.saturating_duration_since(self.last_logged_at);
        if elapsed >= Duration::from_secs(10) {
            self.last_logged_at = now;
            self.last_progress_value = progress_val;
            return true;
        }

        false
    }

    pub fn should_log(&mut self, current_pct: u16, progress_val: u64) -> bool {
        self.should_log_at(current_pct, progress_val, Instant::now())
    }
}

/// Video progress update event sent to the telemetry coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VodVideoProgressUpdate {
    SegmentsDownloaded { segments: usize, bytes: u64 },
    Speed(String),
}

/// Chat progress update event sent to the telemetry coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodChatProgressUpdate {
    pub current_time_ms: u64,
    pub emitted_messages: usize,
}

struct VodRenderState<'a> {
    v_snap: &'a VodVideoProgressSnapshot,
    c_snap: &'a VodChatProgressSnapshot,
    m_snap: &'a VodMuxProgressSnapshot,
    has_video: bool,
    has_chat: bool,
    bar_width: usize,
}

impl<'a> VodRenderState<'a> {
    fn render(&self, output: &CoordinatorOutput, first_render: &mut bool) {
        let top_line = if self.m_snap.is_active {
            Some(self.m_snap.format_interactive(self.bar_width))
        } else if self.has_video {
            Some(self.v_snap.format_interactive(self.bar_width))
        } else {
            None
        };

        let bottom_line = if self.has_chat {
            Some(self.c_snap.format_interactive(self.bar_width))
        } else {
            None
        };

        match (top_line, bottom_line) {
            (Some(top), Some(bottom)) => {
                if *first_render {
                    output.write_str(&format!("{top}\n{bottom}"));
                    *first_render = false;
                } else {
                    output.write_str(&format!("\x1b[1A\r\x1b[2K{top}\n\r\x1b[2K{bottom}"));
                }
            }
            (Some(single), None) | (None, Some(single)) => {
                if *first_render {
                    output.write_str(&single);
                    *first_render = false;
                } else {
                    output.write_str(&format!("\r\x1b[2K{single}"));
                }
            }
            (None, None) => {}
        }
    }
}

fn apply_video_update(
    snapshot: &mut VodVideoProgressSnapshot,
    milestones: &mut VodMilestoneTracker,
    speed_tracker: &mut VodSpeedTracker,
    upd: VodVideoProgressUpdate,
    is_tty: bool,
    output: &CoordinatorOutput,
) {
    match upd {
        VodVideoProgressUpdate::SegmentsDownloaded { segments, bytes } => {
            snapshot.segments_downloaded = segments;
            snapshot.bytes_downloaded = bytes;
            if let Some(spd) = speed_tracker.update_at(bytes, Instant::now()) {
                snapshot.speed = Some(spd.to_string());
            }
        }
        VodVideoProgressUpdate::Speed(speed) => {
            snapshot.speed = Some(speed);
        }
    }
    if !is_tty && milestones.should_log(snapshot.pct(), snapshot.segments_downloaded as u64) {
        output.write_str(&format!("{}\n", snapshot.format_non_interactive()));
    }
}

fn apply_chat_update(
    snapshot: &mut VodChatProgressSnapshot,
    milestones: &mut VodMilestoneTracker,
    upd: VodChatProgressUpdate,
    is_tty: bool,
    output: &CoordinatorOutput,
) {
    snapshot.current_time_ms = upd.current_time_ms;
    snapshot.emitted_messages = upd.emitted_messages;
    if !is_tty && milestones.should_log(snapshot.pct(), snapshot.current_time_ms) {
        output.write_str(&format!("{}\n", snapshot.format_non_interactive()));
    }
}

/// Live progress session for concurrent video and chat VOD archival.
pub struct VodProgressSession {
    video_tx: Option<mpsc::UnboundedSender<VodVideoProgressUpdate>>,
    chat_tx: Option<mpsc::UnboundedSender<VodChatProgressUpdate>>,
    mux_tx: Option<mpsc::UnboundedSender<()>>,
    finish_tx: Option<oneshot::Sender<bool>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl VodProgressSession {
    pub fn video_sender(&self) -> Option<mpsc::UnboundedSender<VodVideoProgressUpdate>> {
        self.video_tx.clone()
    }

    pub fn chat_sender(&self) -> Option<mpsc::UnboundedSender<VodChatProgressUpdate>> {
        self.chat_tx.clone()
    }

    pub fn transition_to_mux(&mut self) {
        if let Some(ref tx) = self.mux_tx {
            let _ = tx.send(());
        }
    }

    pub async fn finish(&mut self, success: bool) {
        if let Some(tx) = self.finish_tx.take() {
            let _ = tx.send(success);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

/// Central progress telemetry coordinator for VOD archival sessions.
#[derive(Clone)]
pub struct VodProgressCoordinator {
    output: CoordinatorOutput,
    is_tty: bool,
    width_mode: ProgressBarWidthMode,
    terminal_probe: Option<TerminalProbe>,
}

impl std::fmt::Debug for VodProgressCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VodProgressCoordinator")
            .field("is_tty", &self.is_tty)
            .field("width_mode", &self.width_mode)
            .finish()
    }
}

impl Default for VodProgressCoordinator {
    fn default() -> Self {
        Self::new(std::io::stdout().is_terminal())
    }
}

impl VodProgressCoordinator {
    pub fn new(is_tty: bool) -> Self {
        Self {
            output: CoordinatorOutput::stdout(),
            is_tty,
            width_mode: ProgressBarWidthMode::Dynamic,
            terminal_probe: None,
        }
    }

    pub fn with_output(output: CoordinatorOutput, is_tty: bool) -> Self {
        Self {
            output,
            is_tty,
            width_mode: ProgressBarWidthMode::Dynamic,
            terminal_probe: None,
        }
    }

    pub fn with_bar_width(mut self, width: usize) -> Self {
        self.width_mode = ProgressBarWidthMode::Fixed(width);
        self
    }

    pub fn with_terminal_width(mut self, width: u16) -> Self {
        self.terminal_probe = Some(Arc::new(move || Ok(width)));
        self
    }

    pub fn with_terminal_probe<F>(mut self, probe: F) -> Self
    where
        F: Fn() -> std::io::Result<u16> + Send + Sync + 'static,
    {
        self.terminal_probe = Some(Arc::new(probe));
        self
    }

    pub fn query_terminal_cols(&self) -> Option<usize> {
        let res = match &self.terminal_probe {
            Some(probe) => probe(),
            None => {
                if !self.is_tty {
                    return None;
                }
                crossterm::terminal::size().map(|(w, _)| w)
            }
        };
        match res {
            Ok(cols) if cols > 0 => Some(cols as usize),
            _ => None,
        }
    }

    pub fn resolve_bar_width(&self, max_suffix_len: usize) -> usize {
        match self.width_mode {
            ProgressBarWidthMode::Fixed(w) => w,
            ProgressBarWidthMode::Dynamic => match self.query_terminal_cols() {
                Some(cols) => {
                    let available = cols.saturating_sub(BADGE_LEN + max_suffix_len + SAFETY_MARGIN);
                    available.clamp(MIN_BAR_WIDTH, MAX_BAR_WIDTH)
                }
                None => DEFAULT_FALLBACK_BAR_WIDTH,
            },
        }
    }

    pub fn resolve_vod_bar_width(
        &self,
        video_suffix_len: usize,
        chat_suffix_len: usize,
        mux_suffix_len: usize,
        has_video: bool,
        has_chat: bool,
        is_mux: bool,
    ) -> usize {
        let max_suffix_len = match (has_video || is_mux, has_chat) {
            (true, true) => {
                let top_len = if is_mux {
                    mux_suffix_len
                } else {
                    video_suffix_len.max(mux_suffix_len)
                };
                top_len.max(chat_suffix_len)
            }
            (true, false) => {
                if is_mux {
                    mux_suffix_len
                } else {
                    video_suffix_len.max(mux_suffix_len)
                }
            }
            (false, true) => chat_suffix_len,
            (false, false) => 0,
        };
        self.resolve_bar_width(max_suffix_len)
    }

    pub fn start_session(
        &self,
        total_segments: usize,
        total_chat_duration_ms: u64,
        has_video: bool,
        has_chat: bool,
    ) -> VodProgressSession {
        if !has_video && !has_chat {
            return VodProgressSession {
                video_tx: None,
                chat_tx: None,
                mux_tx: None,
                finish_tx: None,
                handle: None,
            };
        }

        let (v_tx, mut v_rx) = mpsc::unbounded_channel::<VodVideoProgressUpdate>();
        let (c_tx, mut c_rx) = mpsc::unbounded_channel::<VodChatProgressUpdate>();
        let (mux_tx, mut mux_rx) = mpsc::unbounded_channel::<()>();
        let (finish_tx, mut finish_rx) = oneshot::channel::<bool>();

        let is_tty = self.is_tty;
        let coordinator = self.clone();
        let output = self.output.clone();

        let handle = tokio::spawn(async move {
            let mut video_snapshot = VodVideoProgressSnapshot {
                segments_downloaded: 0,
                total_segments,
                bytes_downloaded: 0,
                speed: None,
            };
            let mut chat_snapshot = VodChatProgressSnapshot {
                current_time_ms: 0,
                duration_ms: total_chat_duration_ms,
                emitted_messages: 0,
            };
            let mut mux_snapshot = VodMuxProgressSnapshot {
                is_active: false,
                is_finished: false,
            };

            let start_time = Instant::now();
            let mut video_milestones = VodMilestoneTracker::new(start_time);
            let mut chat_milestones = VodMilestoneTracker::new(start_time);
            let mut speed_tracker = VodSpeedTracker::new(start_time);

            let mut last_cols = coordinator.query_terminal_cols();
            let mut first_render = true;
            let mut dirty = true;
            let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
            tick_interval.tick().await;

            if !is_tty {
                if has_video && video_milestones.should_log(0, 0) {
                    output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                }
                if has_chat && chat_milestones.should_log(0, 0) {
                    output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                }
            } else {
                let bar_width = coordinator.resolve_vod_bar_width(
                    video_snapshot.metric_suffix_len(),
                    chat_snapshot.metric_suffix_len(),
                    mux_snapshot.metric_suffix_len(),
                    has_video,
                    has_chat,
                    mux_snapshot.is_active,
                );
                let render_state = VodRenderState {
                    v_snap: &video_snapshot,
                    c_snap: &chat_snapshot,
                    m_snap: &mux_snapshot,
                    has_video,
                    has_chat,
                    bar_width,
                };
                render_state.render(&output, &mut first_render);
                dirty = false;
            }

            let finished_success = loop {
                tokio::select! {
                    res = &mut finish_rx => {
                        break res.unwrap_or(false);
                    }
                    Some(()) = mux_rx.recv() => {
                        while let Ok(v_upd) = v_rx.try_recv() {
                            apply_video_update(&mut video_snapshot, &mut video_milestones, &mut speed_tracker, v_upd, is_tty, &output);
                        }
                        if !is_tty && has_video && video_snapshot.pct() < 100 {
                            video_snapshot.segments_downloaded = total_segments;
                            output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                        }
                        mux_snapshot.is_active = true;
                        if !is_tty {
                            output.write_str(&format!("{}\n", VodMuxProgressSnapshot::format_non_interactive_start()));
                        } else {
                            dirty = true;
                        }
                    }
                    Some(v_upd) = v_rx.recv() => {
                        apply_video_update(&mut video_snapshot, &mut video_milestones, &mut speed_tracker, v_upd, is_tty, &output);
                        if is_tty {
                            dirty = true;
                        }
                    }
                    Some(c_upd) = c_rx.recv() => {
                        apply_chat_update(&mut chat_snapshot, &mut chat_milestones, c_upd, is_tty, &output);
                        if is_tty {
                            dirty = true;
                        }
                    }
                    _ = tick_interval.tick() => {
                        let now = Instant::now();
                        let current_cols = coordinator.query_terminal_cols();
                        if current_cols != last_cols {
                            last_cols = current_cols;
                            dirty = true;
                        }
                        if has_video && !mux_snapshot.is_active {
                            if let Some(spd) = speed_tracker.update_at(video_snapshot.bytes_downloaded, now) {
                                let spd_str = spd.to_string();
                                if video_snapshot.speed.as_deref() != Some(&spd_str) {
                                    video_snapshot.speed = Some(spd_str);
                                    dirty = true;
                                }
                            }
                        }
                        if is_tty && dirty {
                            let bar_width = coordinator.resolve_vod_bar_width(
                                video_snapshot.metric_suffix_len(),
                                chat_snapshot.metric_suffix_len(),
                                mux_snapshot.metric_suffix_len(),
                                has_video,
                                has_chat,
                                mux_snapshot.is_active,
                            );
                            let render_state = VodRenderState {
                                v_snap: &video_snapshot,
                                c_snap: &chat_snapshot,
                                m_snap: &mux_snapshot,
                                has_video,
                                has_chat,
                                bar_width,
                            };
                            render_state.render(&output, &mut first_render);
                            dirty = false;
                        } else if !is_tty {
                            if has_video && !mux_snapshot.is_active && video_milestones.should_log(video_snapshot.pct(), video_snapshot.segments_downloaded as u64) {
                                output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                            }
                            if has_chat && chat_milestones.should_log(chat_snapshot.pct(), chat_snapshot.current_time_ms) {
                                output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                            }
                        }
                    }
                }
            };

            // Drain remaining updates
            while let Ok(v_upd) = v_rx.try_recv() {
                apply_video_update(
                    &mut video_snapshot,
                    &mut video_milestones,
                    &mut speed_tracker,
                    v_upd,
                    is_tty,
                    &output,
                );
            }
            while let Ok(c_upd) = c_rx.try_recv() {
                apply_chat_update(
                    &mut chat_snapshot,
                    &mut chat_milestones,
                    c_upd,
                    is_tty,
                    &output,
                );
            }
            while let Ok(()) = mux_rx.try_recv() {
                if !mux_snapshot.is_active {
                    mux_snapshot.is_active = true;
                    if !is_tty {
                        output.write_str(&format!(
                            "{}\n",
                            VodMuxProgressSnapshot::format_non_interactive_start()
                        ));
                    }
                }
            }

            if finished_success {
                if has_video {
                    video_snapshot.segments_downloaded = total_segments;
                }
                if has_chat {
                    chat_snapshot.current_time_ms = total_chat_duration_ms;
                }
                if mux_snapshot.is_active {
                    mux_snapshot.is_finished = true;
                }
            }

            if is_tty {
                let bar_width = coordinator.resolve_vod_bar_width(
                    video_snapshot.metric_suffix_len(),
                    chat_snapshot.metric_suffix_len(),
                    mux_snapshot.metric_suffix_len(),
                    has_video,
                    has_chat,
                    mux_snapshot.is_active,
                );
                let render_state = VodRenderState {
                    v_snap: &video_snapshot,
                    c_snap: &chat_snapshot,
                    m_snap: &mux_snapshot,
                    has_video,
                    has_chat,
                    bar_width,
                };
                render_state.render(&output, &mut first_render);
                output.write_str("\n");
            } else if finished_success {
                if mux_snapshot.is_active {
                    output.write_str(&format!(
                        "{}\n",
                        VodMuxProgressSnapshot::format_non_interactive_finish()
                    ));
                } else if has_video && video_milestones.should_log(100, total_segments as u64) {
                    output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                }
                if has_chat && chat_milestones.should_log(100, total_chat_duration_ms) {
                    output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                }
            }
        });

        VodProgressSession {
            video_tx: if has_video { Some(v_tx) } else { None },
            chat_tx: if has_chat { Some(c_tx) } else { None },
            mux_tx: Some(mux_tx),
            finish_tx: Some(finish_tx),
            handle: Some(handle),
        }
    }
}
