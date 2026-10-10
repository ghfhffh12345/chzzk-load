use std::io::IsTerminal;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub const MIN_BAR_WIDTH: usize = 15;
pub const MAX_BAR_WIDTH: usize = 40;
pub const DEFAULT_FALLBACK_BAR_WIDTH: usize = 40;
pub const DEFAULT_BAR_WIDTH: usize = DEFAULT_FALLBACK_BAR_WIDTH;
pub const BADGE_LEN: usize = 6;
pub const SAFETY_MARGIN: usize = 1;

/// Determines whether the progress bar width dynamically adapts to the terminal columns
/// or uses a deterministic fixed width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressBarWidthMode {
    Dynamic,
    Fixed(usize),
}

/// Type alias for probing terminal column width in production or tests.
pub type TerminalProbe = Arc<dyn Fn() -> std::io::Result<u16> + Send + Sync>;

pub const COLOR_DIVIDER: &str = "\x1b[38;2;83;88;111m"; // #53586f (theme::DIVIDER)
pub const COLOR_GREEN: &str = "\x1b[38;2;131;162;117m"; // #83a275 (theme::GREEN)
pub const COLOR_CYAN: &str = "\x1b[38;2;105;156;154m"; // #699c9a (theme::CYAN)
pub const COLOR_BLUE: &str = "\x1b[38;2;112;135;188m"; // #7087bc (theme::BLUE)
pub const STYLE_DIM: &str = "\x1b[2m";
pub const STYLE_RESET: &str = "\x1b[0m";

/// Formats a plain, unstyled progress bar matching the Cloud Upload glyph language:
/// `━` (Unicode \u{2501}, filled) and `─` (Unicode \u{2500}, unfilled).
#[doc(hidden)]
pub fn format_progress_bar(width: usize, pct: u16) -> String {
    if width == 0 {
        return String::new();
    }
    let pct = pct.min(100);
    let filled = ((pct as usize * width) / 100).min(width);
    let unfilled = width.saturating_sub(filled);
    format!("{}{}", "━".repeat(filled), "─".repeat(unfilled))
}

/// Formats an interactive progress bar styled with TrueColor ANSI escapes matching
/// the TUI Cloud Upload visual language:
/// `━` (Unicode \u{2501}, filled) in standard foreground and `─` (Unicode \u{2500}, unfilled)
/// styled with `theme::DIVIDER` (`#53586f`).
#[doc(hidden)]
pub fn format_interactive_bar(width: usize, pct: u16) -> String {
    if width == 0 {
        return String::new();
    }
    let pct = pct.min(100);
    let filled = ((pct as usize * width) / 100).min(width);
    let unfilled = width.saturating_sub(filled);
    format!(
        "{STYLE_RESET}{}{COLOR_DIVIDER}{}{STYLE_RESET}",
        "━".repeat(filled),
        "─".repeat(unfilled)
    )
}

/// Formats byte quantities into human-readable strings with binary (1024-based) units.
#[doc(hidden)]
pub fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Formats integers with comma thousands separators (e.g. 12,345).
#[doc(hidden)]
pub fn format_number_with_commas(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    let rem = s.len() % 3;
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (i % 3 == rem || (rem == 0 && i % 3 == 0)) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

/// Progress metrics for video remuxing consolidation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VideoProgressSnapshot {
    pub chunks_fed: usize,
    pub total_chunks: usize,
    pub bytes_fed: u64,
    pub total_bytes: u64,
    pub speed: Option<String>,
}

impl VideoProgressSnapshot {
    pub fn pct(&self) -> u16 {
        if self.total_bytes > 0 {
            ((self.bytes_fed as f64 / self.total_bytes as f64) * 100.0)
                .round()
                .min(100.0) as u16
        } else if self.total_chunks > 0 {
            ((self.chunks_fed as f64 / self.total_chunks as f64) * 100.0)
                .round()
                .min(100.0) as u16
        } else {
            100
        }
    }

    pub fn metric_suffix(&self) -> String {
        let pct = self.pct();
        let bytes_str = format_bytes(self.bytes_fed);
        let speed_suffix = match &self.speed {
            Some(s) if !s.trim().is_empty() => format!(" {s}"),
            _ => String::new(),
        };
        format!(
            " {pct}% ({}/{total} chunks, {bytes_str}){speed_suffix}",
            self.chunks_fed,
            total = self.total_chunks
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
        let bytes_str = format_bytes(self.bytes_fed);
        let speed_suffix = match &self.speed {
            Some(s) if !s.trim().is_empty() => format!(" speed: {s}"),
            _ => String::new(),
        };
        format!(
            "[INFO] [VID] Consolidation progress: {pct}% ({}/{total} chunks, {bytes_str}){speed_suffix}",
            self.chunks_fed,
            total = self.total_chunks
        )
    }
}

/// Progress metrics for chat deduplication consolidation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChatProgressSnapshot {
    pub chunks_read: usize,
    pub total_chunks: usize,
    pub total_messages: usize,
    pub deduplicated_messages: usize,
    pub emitted_messages: usize,
}

impl ChatProgressSnapshot {
    pub fn pct(&self) -> u16 {
        if self.total_chunks == 0 {
            100
        } else {
            ((self.chunks_read as f64 / self.total_chunks as f64) * 100.0)
                .round()
                .min(100.0) as u16
        }
    }

    pub fn metric_suffix(&self) -> String {
        let pct = self.pct();
        let msgs_str = format_number_with_commas(self.emitted_messages);
        format!(
            " {pct}% ({}/{total} chunks, {msgs_str} msgs)",
            self.chunks_read,
            total = self.total_chunks
        )
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
        let msgs_str = format_number_with_commas(self.emitted_messages);
        format!(
            "[INFO] [CHAT] Consolidation progress: {pct}% ({}/{total} chunks, {msgs_str} msgs)",
            self.chunks_read,
            total = self.total_chunks
        )
    }
}

/// Progress metrics for original chunk purge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PurgeProgressSnapshot {
    pub chunks_deleted: usize,
    pub total_chunks: usize,
}

impl PurgeProgressSnapshot {
    pub fn pct(&self) -> u16 {
        if self.total_chunks == 0 {
            100
        } else {
            ((self.chunks_deleted as f64 / self.total_chunks as f64) * 100.0)
                .round()
                .min(100.0) as u16
        }
    }

    pub fn metric_suffix(&self) -> String {
        let pct = self.pct();
        format!(
            " {pct}% ({}/{total} chunks deleted)",
            self.chunks_deleted,
            total = self.total_chunks
        )
    }

    pub fn metric_suffix_len(&self) -> usize {
        self.metric_suffix().chars().count()
    }

    pub fn format_interactive(&self, bar_width: usize) -> String {
        let pct = self.pct();
        let bar = format_interactive_bar(bar_width, pct);
        let suffix = self.metric_suffix();
        format!("{COLOR_GREEN} DEL  {STYLE_RESET}{bar}{STYLE_DIM}{suffix}{STYLE_RESET}")
    }

    pub fn format_non_interactive(&self) -> String {
        let pct = self.pct();
        format!(
            "[INFO] [DEL] Purge progress: {pct}% ({}/{total} chunks deleted)",
            self.chunks_deleted,
            total = self.total_chunks
        )
    }
}

/// Tracks milestone triggers for non-interactive mode.
///
/// Triggers on:
/// - 20% milestone boundaries: 0%, 20%, 40%, 60%, 80%, 100%.
/// - 30-second heartbeat since the last log line to prove liveness (even during slow chunks or stalls).
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct MilestoneTracker {
    last_logged_milestone: Option<u16>,
    last_logged_at: Instant,
    last_progress_value: u64,
}

impl MilestoneTracker {
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

        // Heartbeat check: 30 seconds since last log to prove liveness during slow chunks or stalls
        let elapsed = now.saturating_duration_since(self.last_logged_at);
        if elapsed >= Duration::from_secs(30) {
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

/// Video pipeline progress event sent to the telemetry coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VideoProgressUpdate {
    ChunkFed { chunks_fed: usize, bytes_fed: u64 },
    Speed(String),
}

/// Chat pipeline progress event sent to the telemetry coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatProgressUpdate {
    pub chunks_read: usize,
    pub total_messages: usize,
    pub deduplicated_messages: usize,
    pub emitted_messages: usize,
}

/// Purge pipeline progress event sent to the telemetry coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeProgressUpdate {
    pub chunks_deleted: usize,
}

/// Thread-safe output sink wrapper for telemetry rendering.
#[doc(hidden)]
#[derive(Clone)]
pub struct CoordinatorOutput {
    inner: Arc<std::sync::Mutex<Box<dyn Write + Send>>>,
}

impl CoordinatorOutput {
    pub fn stdout() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(Box::new(std::io::stdout()))),
        }
    }

    pub fn buffer() -> (Self, Arc<std::sync::Mutex<Vec<u8>>>) {
        let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Self {
            inner: Arc::new(std::sync::Mutex::new(Box::new(SharedBuffer(Arc::clone(
                &buf,
            ))))),
        };
        (writer, buf)
    }

    pub fn write_str(&self, s: &str) {
        if let Ok(mut w) = self.inner.lock() {
            let _ = w.write_all(s.as_bytes());
            let _ = w.flush();
        }
    }
}

struct SharedBuffer(Arc<std::sync::Mutex<Vec<u8>>>);
impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0.lock().map_err(|_| std::io::ErrorKind::Other)?;
        inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.0.lock().map_err(|_| std::io::ErrorKind::Other)?;
        inner.flush()
    }
}

async fn teardown_session(
    finish_tx: &mut Option<oneshot::Sender<bool>>,
    handle: &mut Option<tokio::task::JoinHandle<()>>,
    success: bool,
) {
    if let Some(tx) = finish_tx.take() {
        let _ = tx.send(success);
    }
    if let Some(handle) = handle.take() {
        let _ = handle.await;
    }
}

/// Live progress session for concurrent video and chat consolidation.
pub struct MediaProgressSession {
    video_tx: Option<mpsc::UnboundedSender<VideoProgressUpdate>>,
    chat_tx: Option<mpsc::UnboundedSender<ChatProgressUpdate>>,
    finish_tx: Option<oneshot::Sender<bool>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl MediaProgressSession {
    pub fn video_sender(&self) -> Option<mpsc::UnboundedSender<VideoProgressUpdate>> {
        self.video_tx.clone()
    }

    pub fn chat_sender(&self) -> Option<mpsc::UnboundedSender<ChatProgressUpdate>> {
        self.chat_tx.clone()
    }

    pub async fn finish(&mut self, success: bool) {
        teardown_session(&mut self.finish_tx, &mut self.handle, success).await;
    }
}

/// Live progress session for chunk purging.
pub struct PurgeProgressSession {
    purge_tx: Option<mpsc::UnboundedSender<PurgeProgressUpdate>>,
    finish_tx: Option<oneshot::Sender<bool>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl PurgeProgressSession {
    pub fn purge_sender(&self) -> Option<mpsc::UnboundedSender<PurgeProgressUpdate>> {
        self.purge_tx.clone()
    }

    pub async fn finish(&mut self, success: bool) {
        teardown_session(&mut self.finish_tx, &mut self.handle, success).await;
    }
}

/// Central progress telemetry coordinator for post-recording consolidation.
#[derive(Clone)]
pub struct ConsolidationProgressCoordinator {
    output: CoordinatorOutput,
    is_tty: bool,
    width_mode: ProgressBarWidthMode,
    terminal_probe: Option<TerminalProbe>,
}

impl Default for ConsolidationProgressCoordinator {
    fn default() -> Self {
        Self::new(std::io::stdout().is_terminal())
    }
}

impl ConsolidationProgressCoordinator {
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

    /// Configures an explicit fixed progress bar width, bypassing dynamic terminal queries.
    pub fn with_bar_width(mut self, width: usize) -> Self {
        self.width_mode = ProgressBarWidthMode::Fixed(width);
        self
    }

    /// Sets a simulated fixed terminal width for testing responsive geometry scenarios.
    pub fn with_terminal_width(mut self, width: u16) -> Self {
        self.terminal_probe = Some(Arc::new(move || Ok(width)));
        self
    }

    /// Sets a custom terminal probe function for testing error fallbacks and dynamic queries.
    pub fn with_terminal_probe<F>(mut self, probe: F) -> Self
    where
        F: Fn() -> std::io::Result<u16> + Send + Sync + 'static,
    {
        self.terminal_probe = Some(Arc::new(probe));
        self
    }

    pub fn width_mode(&self) -> ProgressBarWidthMode {
        self.width_mode
    }

    pub fn is_tty(&self) -> bool {
        self.is_tty
    }

    /// Queries the active terminal columns, returning None on probe failure, 0 columns, or non-TTY.
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

    /// Resolves the progress bar width based on active width mode, terminal width, and metric suffix length.
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

    /// Resolves synchronized progress bar width for concurrent video and chat tracks.
    pub(crate) fn resolve_media_bar_width(
        &self,
        video: &VideoProgressSnapshot,
        chat: &ChatProgressSnapshot,
        has_video: bool,
        has_chat: bool,
    ) -> usize {
        let max_suffix_len = match (has_video, has_chat) {
            (true, true) => video.metric_suffix_len().max(chat.metric_suffix_len()),
            (true, false) => video.metric_suffix_len(),
            (false, true) => chat.metric_suffix_len(),
            (false, false) => 0,
        };
        self.resolve_bar_width(max_suffix_len)
    }

    pub fn start_media(&self, total_video: usize, total_chat: usize) -> MediaProgressSession {
        self.start_media_with_bytes(total_video, total_chat, 0)
    }

    pub fn start_media_with_bytes(
        &self,
        total_video: usize,
        total_chat: usize,
        total_video_bytes: u64,
    ) -> MediaProgressSession {
        let has_video = total_video > 0;
        let has_chat = total_chat > 0;

        if !has_video && !has_chat {
            return MediaProgressSession {
                video_tx: None,
                chat_tx: None,
                finish_tx: None,
                handle: None,
            };
        }

        let (v_tx, mut v_rx) = mpsc::unbounded_channel::<VideoProgressUpdate>();
        let (c_tx, mut c_rx) = mpsc::unbounded_channel::<ChatProgressUpdate>();
        let (finish_tx, mut finish_rx) = oneshot::channel::<bool>();

        let is_tty = self.is_tty;
        let coordinator = self.clone();
        let output = self.output.clone();

        let handle = tokio::spawn(async move {
            let mut video_snapshot = VideoProgressSnapshot {
                chunks_fed: 0,
                total_chunks: total_video,
                bytes_fed: 0,
                total_bytes: total_video_bytes,
                speed: None,
            };
            let mut chat_snapshot = ChatProgressSnapshot {
                chunks_read: 0,
                total_chunks: total_chat,
                total_messages: 0,
                deduplicated_messages: 0,
                emitted_messages: 0,
            };
            let start_time = Instant::now();
            let mut video_milestones = MilestoneTracker::new(start_time);
            let mut chat_milestones = MilestoneTracker::new(start_time);

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
                let bar_width = coordinator.resolve_media_bar_width(
                    &video_snapshot,
                    &chat_snapshot,
                    has_video,
                    has_chat,
                );
                render_interactive_media(
                    &output,
                    &video_snapshot,
                    &chat_snapshot,
                    has_video,
                    has_chat,
                    bar_width,
                    &mut first_render,
                );
                dirty = false;
            }

            let finished_success = loop {
                tokio::select! {
                    res = &mut finish_rx => {
                        break res.unwrap_or(false);
                    }
                    Some(v_upd) = v_rx.recv() => {
                        match v_upd {
                            VideoProgressUpdate::ChunkFed { chunks_fed, bytes_fed } => {
                                video_snapshot.chunks_fed = chunks_fed;
                                video_snapshot.bytes_fed = bytes_fed;
                            }
                            VideoProgressUpdate::Speed(speed) => {
                                video_snapshot.speed = Some(speed);
                            }
                        }
                        if !is_tty {
                            if video_milestones.should_log(video_snapshot.pct(), video_snapshot.chunks_fed as u64) {
                                output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                            }
                        } else {
                            dirty = true;
                        }
                    }
                    Some(c_upd) = c_rx.recv() => {
                        chat_snapshot.chunks_read = c_upd.chunks_read;
                        chat_snapshot.total_messages = c_upd.total_messages;
                        chat_snapshot.deduplicated_messages = c_upd.deduplicated_messages;
                        chat_snapshot.emitted_messages = c_upd.emitted_messages;
                        if !is_tty {
                            if chat_milestones.should_log(chat_snapshot.pct(), chat_snapshot.chunks_read as u64) {
                                output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                            }
                        } else {
                            dirty = true;
                        }
                    }
                    _ = tick_interval.tick() => {
                        if is_tty && dirty {
                            let bar_width = coordinator.resolve_media_bar_width(
                                &video_snapshot,
                                &chat_snapshot,
                                has_video,
                                has_chat,
                            );
                            render_interactive_media(
                                &output,
                                &video_snapshot,
                                &chat_snapshot,
                                has_video,
                                has_chat,
                                bar_width,
                                &mut first_render,
                            );
                            dirty = false;
                        } else if !is_tty {
                            if has_video && video_milestones.should_log(video_snapshot.pct(), video_snapshot.chunks_fed as u64) {
                                output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                            }
                            if has_chat && chat_milestones.should_log(chat_snapshot.pct(), chat_snapshot.chunks_read as u64) {
                                output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                            }
                        }
                    }
                }
            };

            while let Ok(v_upd) = v_rx.try_recv() {
                match v_upd {
                    VideoProgressUpdate::ChunkFed {
                        chunks_fed,
                        bytes_fed,
                    } => {
                        video_snapshot.chunks_fed = chunks_fed;
                        video_snapshot.bytes_fed = bytes_fed;
                    }
                    VideoProgressUpdate::Speed(speed) => {
                        video_snapshot.speed = Some(speed);
                    }
                }
                if !is_tty
                    && video_milestones
                        .should_log(video_snapshot.pct(), video_snapshot.chunks_fed as u64)
                {
                    output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                }
            }
            while let Ok(c_upd) = c_rx.try_recv() {
                chat_snapshot.chunks_read = c_upd.chunks_read;
                chat_snapshot.total_messages = c_upd.total_messages;
                chat_snapshot.deduplicated_messages = c_upd.deduplicated_messages;
                chat_snapshot.emitted_messages = c_upd.emitted_messages;
                if !is_tty
                    && chat_milestones
                        .should_log(chat_snapshot.pct(), chat_snapshot.chunks_read as u64)
                {
                    output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                }
            }

            if finished_success {
                if has_video {
                    video_snapshot.chunks_fed = total_video;
                    if total_video_bytes > 0 {
                        video_snapshot.bytes_fed = total_video_bytes;
                    }
                }
                if has_chat {
                    chat_snapshot.chunks_read = total_chat;
                }
            }

            if is_tty {
                let bar_width = coordinator.resolve_media_bar_width(
                    &video_snapshot,
                    &chat_snapshot,
                    has_video,
                    has_chat,
                );
                render_interactive_media(
                    &output,
                    &video_snapshot,
                    &chat_snapshot,
                    has_video,
                    has_chat,
                    bar_width,
                    &mut first_render,
                );
                output.write_str("\n");
            } else if finished_success {
                if has_video && video_milestones.should_log(100, total_video as u64) {
                    output.write_str(&format!("{}\n", video_snapshot.format_non_interactive()));
                }
                if has_chat && chat_milestones.should_log(100, total_chat as u64) {
                    output.write_str(&format!("{}\n", chat_snapshot.format_non_interactive()));
                }
            }
        });

        MediaProgressSession {
            video_tx: if has_video { Some(v_tx) } else { None },
            chat_tx: if has_chat { Some(c_tx) } else { None },
            finish_tx: Some(finish_tx),
            handle: Some(handle),
        }
    }

    /// Starts a live progress telemetry session for chunk purging (`DEL`).
    ///
    /// In interactive TTY mode, dynamically computes bar width responsive to terminal geometry
    /// clamped within `[MIN_BAR_WIDTH, MAX_BAR_WIDTH]` on initial render, 10 Hz dirty ticks,
    /// and final completion, or respects fixed `with_bar_width` overrides.
    pub fn start_purge(&self, total_purge: usize) -> PurgeProgressSession {
        if total_purge == 0 {
            return PurgeProgressSession {
                purge_tx: None,
                finish_tx: None,
                handle: None,
            };
        }

        let (p_tx, mut p_rx) = mpsc::unbounded_channel::<PurgeProgressUpdate>();
        let (finish_tx, mut finish_rx) = oneshot::channel::<bool>();

        let is_tty = self.is_tty;
        let coordinator = self.clone();
        let output = self.output.clone();

        let handle = tokio::spawn(async move {
            let mut purge_snapshot = PurgeProgressSnapshot {
                chunks_deleted: 0,
                total_chunks: total_purge,
            };
            let start_time = Instant::now();
            let mut purge_milestones = MilestoneTracker::new(start_time);
            let mut dirty = true;
            let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
            tick_interval.tick().await;

            if !is_tty {
                if purge_milestones.should_log(0, 0) {
                    output.write_str(&format!("{}\n", purge_snapshot.format_non_interactive()));
                }
            } else {
                render_interactive_purge(&output, &coordinator, &purge_snapshot);
                dirty = false;
            }

            let finished_success = loop {
                tokio::select! {
                    res = &mut finish_rx => {
                        break res.unwrap_or(false);
                    }
                    Some(upd) = p_rx.recv() => {
                        purge_snapshot.chunks_deleted = upd.chunks_deleted;
                        if !is_tty {
                            if purge_milestones.should_log(purge_snapshot.pct(), purge_snapshot.chunks_deleted as u64) {
                                output.write_str(&format!("{}\n", purge_snapshot.format_non_interactive()));
                            }
                        } else {
                            dirty = true;
                        }
                    }
                    _ = tick_interval.tick() => {
                        if is_tty && dirty {
                            render_interactive_purge(&output, &coordinator, &purge_snapshot);
                            dirty = false;
                        } else if !is_tty && purge_milestones.should_log(purge_snapshot.pct(), purge_snapshot.chunks_deleted as u64) {
                            output.write_str(&format!("{}\n", purge_snapshot.format_non_interactive()));
                        }
                    }
                }
            };

            while let Ok(upd) = p_rx.try_recv() {
                purge_snapshot.chunks_deleted = upd.chunks_deleted;
                if !is_tty
                    && purge_milestones
                        .should_log(purge_snapshot.pct(), purge_snapshot.chunks_deleted as u64)
                {
                    output.write_str(&format!("{}\n", purge_snapshot.format_non_interactive()));
                }
            }

            if finished_success {
                purge_snapshot.chunks_deleted = total_purge;
            }

            if is_tty {
                render_interactive_purge(&output, &coordinator, &purge_snapshot);
                output.write_str("\n");
            } else if finished_success && purge_milestones.should_log(100, total_purge as u64) {
                output.write_str(&format!("{}\n", purge_snapshot.format_non_interactive()));
            }
        });

        PurgeProgressSession {
            purge_tx: Some(p_tx),
            finish_tx: Some(finish_tx),
            handle: Some(handle),
        }
    }
}

fn render_interactive_media(
    output: &CoordinatorOutput,
    v_snap: &VideoProgressSnapshot,
    c_snap: &ChatProgressSnapshot,
    has_video: bool,
    has_chat: bool,
    bar_width: usize,
    first_render: &mut bool,
) {
    if has_video && has_chat {
        let v_line = v_snap.format_interactive(bar_width);
        let c_line = c_snap.format_interactive(bar_width);
        if *first_render {
            output.write_str(&format!("{v_line}\n{c_line}"));
            *first_render = false;
        } else {
            output.write_str(&format!("\x1b[1A\r\x1b[2K{v_line}\n\r\x1b[2K{c_line}"));
        }
    } else if has_video {
        let v_line = v_snap.format_interactive(bar_width);
        output.write_str(&format!("\r\x1b[2K{v_line}"));
        *first_render = false;
    } else if has_chat {
        let c_line = c_snap.format_interactive(bar_width);
        output.write_str(&format!("\r\x1b[2K{c_line}"));
        *first_render = false;
    }
}

fn render_interactive_purge(
    output: &CoordinatorOutput,
    coordinator: &ConsolidationProgressCoordinator,
    snapshot: &PurgeProgressSnapshot,
) {
    let bar_width = coordinator.resolve_bar_width(snapshot.metric_suffix_len());
    let line = snapshot.format_interactive(bar_width);
    output.write_str(&format!("\r\x1b[2K{line}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_media_bar_width() {
        let coordinator = ConsolidationProgressCoordinator::new(true).with_terminal_width(80);
        let video = VideoProgressSnapshot {
            chunks_fed: 5,
            total_chunks: 10,
            bytes_fed: 1000,
            total_bytes: 2000,
            speed: Some("10x".to_string()),
        };
        let chat = ChatProgressSnapshot {
            chunks_read: 2,
            total_chunks: 10,
            total_messages: 50,
            deduplicated_messages: 5,
            emitted_messages: 45,
        };

        // When both video and chat are active, it selects the max suffix length
        let both_width = coordinator.resolve_media_bar_width(&video, &chat, true, true);
        let max_len = video.metric_suffix_len().max(chat.metric_suffix_len());
        assert_eq!(both_width, coordinator.resolve_bar_width(max_len));

        // When only video is active
        let video_only_width = coordinator.resolve_media_bar_width(&video, &chat, true, false);
        assert_eq!(
            video_only_width,
            coordinator.resolve_bar_width(video.metric_suffix_len())
        );

        // When only chat is active
        let chat_only_width = coordinator.resolve_media_bar_width(&video, &chat, false, true);
        assert_eq!(
            chat_only_width,
            coordinator.resolve_bar_width(chat.metric_suffix_len())
        );

        // When neither is active
        let neither_width = coordinator.resolve_media_bar_width(&video, &chat, false, false);
        assert_eq!(neither_width, coordinator.resolve_bar_width(0));
    }

    #[test]
    fn test_format_progress_bar_internal() {
        assert_eq!(format_progress_bar(20, 0), "────────────────────");
        assert_eq!(format_progress_bar(20, 50), "━━━━━━━━━━──────────");
        assert_eq!(format_progress_bar(20, 100), "━━━━━━━━━━━━━━━━━━━━");
        assert_eq!(format_progress_bar(0, 50), "");
    }

    #[test]
    fn test_format_bytes_internal() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn test_format_number_with_commas_internal() {
        assert_eq!(format_number_with_commas(0), "0");
        assert_eq!(format_number_with_commas(999), "999");
        assert_eq!(format_number_with_commas(1000), "1,000");
        assert_eq!(format_number_with_commas(1234567), "1,234,567");
    }

    #[test]
    fn test_milestone_tracker_heartbeat_stall() {
        let t0 = Instant::now();
        let mut tracker = MilestoneTracker::new(t0);

        assert!(tracker.should_log_at(0, 0, t0));
        assert!(!tracker.should_log_at(5, 50, t0 + Duration::from_secs(20)));
        // 31s elapsed: heartbeat triggers
        assert!(tracker.should_log_at(5, 50, t0 + Duration::from_secs(31)));
        // 65s elapsed: stalled progress (still 50), >30s passed since 31s -> heartbeat triggers during stall
        assert!(tracker.should_log_at(5, 50, t0 + Duration::from_secs(65)));
    }
}
