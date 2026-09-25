use crossterm::event::KeyCode;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::tui::event::{AppEvent, LogEntry};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChannelItem {
    pub id: String,
    pub name: String,
    pub is_live: bool,
    pub is_active: bool,
    pub title: String,
    pub chat_count: u64,
}

pub type ChannelItemState = ChannelItem;

impl ChannelItem {
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        is_live: bool,
        is_active: bool,
        title: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            is_live,
            is_active,
            title: title.into(),
            chat_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActiveUpload {
    pub channel_id: String,
    pub chunk_name: String,
    pub streamer_name: String,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    pub speed_mb_s: f64,
}

#[derive(Debug, Clone)]
pub struct App {
    pub channels: Vec<ChannelItem>,
    pub selected_channel_idx: usize,
    pub channel_scroll: usize,
    pub active_upload_name: Option<String>,
    pub upload_progress_pct: u16,
    pub upload_speed: f64,
    pub uploaded_count: usize,
    pub reclaimed_mb: f64,
    pub active_uploads: HashMap<String, ActiveUpload>,
    pub active_recording_starts: HashMap<String, Instant>,
    pub total_recorded_duration: Duration,
    pub logs: VecDeque<LogEntry>,
    pub log_scroll: usize,
    pub show_logs: bool,
    pub is_shutting_down: bool,
    pub should_quit: bool,
    pub refresh_requested: bool,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl From<crate::config::Settings> for App {
    fn from(settings: crate::config::Settings) -> Self {
        Self::from_settings(&settings)
    }
}

impl From<&crate::config::Settings> for App {
    fn from(settings: &crate::config::Settings) -> Self {
        Self::from_settings(settings)
    }
}

impl App {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
            selected_channel_idx: 0,
            channel_scroll: 0,
            active_upload_name: None,
            upload_progress_pct: 0,
            upload_speed: 0.0,
            uploaded_count: 0,
            reclaimed_mb: 0.0,
            active_uploads: HashMap::new(),
            active_recording_starts: HashMap::new(),
            total_recorded_duration: Duration::ZERO,
            logs: VecDeque::with_capacity(200),
            log_scroll: 0,
            show_logs: true,
            is_shutting_down: false,
            should_quit: false,
            refresh_requested: false,
        }
    }

    pub fn from_settings(settings: &crate::config::Settings) -> Self {
        let mut app = Self::new();
        for ch in &settings.channels {
            app.channels.push(ChannelItem {
                id: ch.id.clone(),
                name: ch.name.clone(),
                is_live: false,
                is_active: false,
                title: "Checking...".to_string(),
                chat_count: 0,
            });
        }
        app
    }

    pub fn update(&mut self, event: AppEvent) {
        self.handle_event(event);
    }

    pub fn scroll_channels_down(&mut self) {
        if !self.channels.is_empty() {
            let max = self.channels.len().saturating_sub(1);
            if self.channel_scroll < max {
                self.channel_scroll += 1;
                self.selected_channel_idx = self.channel_scroll;
            }
        }
    }

    pub fn scroll_channels_up(&mut self) {
        self.channel_scroll = self.channel_scroll.saturating_sub(1);
        self.selected_channel_idx = self.channel_scroll;
    }

    pub fn next_channel(&mut self) {
        self.scroll_channels_down();
    }

    pub fn prev_channel(&mut self) {
        self.scroll_channels_up();
    }

    pub fn adjust_channel_scroll(&mut self) {
        self.channel_scroll = self
            .channel_scroll
            .min(self.channels.len().saturating_sub(1));
        self.selected_channel_idx = self.channel_scroll;
    }

    fn update_primary_upload(&mut self, completed_or_failed_chunk: &str) {
        if self.active_upload_name.as_deref() == Some(completed_or_failed_chunk) {
            if let Some(first) = self.active_uploads.values().next() {
                self.active_upload_name = Some(first.chunk_name.clone());
                if first.total_bytes > 0 {
                    self.upload_progress_pct =
                        ((first.uploaded_bytes as f64 / first.total_bytes as f64) * 100.0)
                            .round()
                            .min(100.0) as u16;
                }
                self.upload_speed = first.speed_mb_s;
            } else {
                self.active_upload_name = None;
                self.upload_progress_pct = 0;
                self.upload_speed = 0.0;
            }
        }
    }

    pub fn total_recorded_duration(&self) -> Duration {
        let active_duration: Duration = self
            .active_recording_starts
            .values()
            .map(|start| start.elapsed())
            .sum();
        self.total_recorded_duration + active_duration
    }

    pub fn format_duration(dur: Duration) -> String {
        let total_secs = dur.as_secs();
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;

        if hours > 0 {
            format!("{}h {:02}m", hours, mins)
        } else if mins > 0 {
            format!("{}m {:02}s", mins, secs)
        } else {
            format!("{}s", secs)
        }
    }

    pub fn format_total_recorded(&self) -> String {
        Self::format_duration(self.total_recorded_duration())
    }

    pub fn format_archived_size(&self) -> String {
        if self.reclaimed_mb >= 1024.0 {
            format!("{:.2} GB", self.reclaimed_mb / 1024.0)
        } else {
            format!("{:.1} MB", self.reclaimed_mb)
        }
    }

    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::ChannelUpdate {
                channel_id,
                channel_name,
                is_live,
                title,
            } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.name = channel_name;
                    ch.is_live = is_live;
                    ch.title = title;
                    if !is_live {
                        ch.is_active = false;
                        if let Some(start) = self.active_recording_starts.remove(&channel_id) {
                            self.total_recorded_duration += start.elapsed();
                        }
                    }
                } else {
                    self.channels.push(ChannelItem {
                        id: channel_id,
                        name: channel_name,
                        is_live,
                        is_active: false,
                        title,
                        chat_count: 0,
                    });
                }
            }
            AppEvent::RecordingStarted {
                channel_id,
                session_title: _,
            } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.is_active = true;
                }
                self.active_recording_starts
                    .entry(channel_id)
                    .or_insert_with(Instant::now);
            }
            AppEvent::RecordingEnded { channel_id } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.is_active = false;
                    ch.chat_count = 0;
                }
                if let Some(start) = self.active_recording_starts.remove(&channel_id) {
                    self.total_recorded_duration += start.elapsed();
                }
            }
            AppEvent::ChatStats {
                channel_id,
                message_count,
            } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.chat_count = message_count;
                }
            }
            AppEvent::UploadProgress {
                channel_id,
                chunk_name,
                streamer_name,
                uploaded_bytes,
                total_bytes,
                speed_mb_s,
            } => {
                self.active_upload_name = Some(chunk_name.clone());
                if total_bytes > 0 {
                    self.upload_progress_pct = ((uploaded_bytes as f64 / total_bytes as f64)
                        * 100.0)
                        .round()
                        .min(100.0) as u16;
                }
                self.upload_speed = speed_mb_s;

                self.active_uploads.insert(
                    channel_id.clone(),
                    ActiveUpload {
                        channel_id,
                        chunk_name,
                        streamer_name,
                        uploaded_bytes,
                        total_bytes,
                        speed_mb_s,
                    },
                );
            }
            AppEvent::UploadCompleted {
                channel_id,
                chunk_name,
                reclaimed_bytes,
            } => {
                self.uploaded_count += 1;
                self.reclaimed_mb += reclaimed_bytes as f64 / 1_048_576.0;

                if let Some(upload) = self.active_uploads.get(&channel_id)
                    && upload.chunk_name == chunk_name
                {
                    self.active_uploads.remove(&channel_id);
                }

                self.update_primary_upload(&chunk_name);
            }
            AppEvent::UploadFailed {
                channel_id,
                chunk_name,
            } => {
                if let Some(upload) = self.active_uploads.get(&channel_id)
                    && upload.chunk_name == chunk_name
                {
                    self.active_uploads.remove(&channel_id);
                }

                self.update_primary_upload(&chunk_name);
            }
            AppEvent::Log(entry) => {
                if self.logs.len() >= 200 {
                    self.logs.pop_front();
                }
                self.logs.push_back(entry);
            }
            AppEvent::Key(key) => {
                if key.kind == crossterm::event::KeyEventKind::Release {
                    return;
                }
                match key.code {
                    KeyCode::Char('q') => {
                        if key.kind == crossterm::event::KeyEventKind::Press {
                            if self.is_shutting_down {
                                self.should_quit = true;
                            } else {
                                self.is_shutting_down = true;
                            }
                        }
                    }
                    KeyCode::Char('r') => self.refresh_requested = true,
                    KeyCode::Char('l') => {
                        if key.kind == crossterm::event::KeyEventKind::Press {
                            self.show_logs = !self.show_logs;
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => self.scroll_channels_up(),
                    KeyCode::Down | KeyCode::Char('j') => self.scroll_channels_down(),
                    KeyCode::PageUp => {
                        self.log_scroll = self.log_scroll.saturating_add(5);
                    }
                    KeyCode::PageDown => {
                        self.log_scroll = self.log_scroll.saturating_sub(5);
                    }
                    KeyCode::Home => {
                        self.log_scroll = usize::MAX / 2;
                    }
                    KeyCode::End => {
                        self.log_scroll = 0;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}
