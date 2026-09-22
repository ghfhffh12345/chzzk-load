use crossterm::event::KeyCode;
use std::collections::HashMap;

use crate::tui::event::AppEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelItem {
    pub id: String,
    pub name: String,
    pub is_live: bool,
    pub is_active: bool,
    pub title: String,
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
    pub logs: Vec<String>,
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
            logs: Vec::new(),
            log_scroll: 0,
            show_logs: true,
            is_shutting_down: false,
            should_quit: false,
            refresh_requested: false,
        }
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
                    }
                } else {
                    self.channels.push(ChannelItem {
                        id: channel_id,
                        name: channel_name,
                        is_live,
                        is_active: false,
                        title,
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
            }
            AppEvent::RecordingEnded { channel_id } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.is_active = false;
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

                if self.active_upload_name.as_deref() == Some(&chunk_name) {
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
            AppEvent::UploadFailed {
                channel_id,
                chunk_name,
            } => {
                if let Some(upload) = self.active_uploads.get(&channel_id)
                    && upload.chunk_name == chunk_name
                {
                    self.active_uploads.remove(&channel_id);
                }
                if self.active_upload_name.as_deref() == Some(&chunk_name) {
                    self.active_upload_name = self
                        .active_uploads
                        .values()
                        .next()
                        .map(|u| u.chunk_name.clone());
                    if self.active_upload_name.is_none() {
                        self.upload_progress_pct = 0;
                        self.upload_speed = 0.0;
                    }
                }
            }
            AppEvent::Log(msg) => {
                self.logs.push(msg);
                if self.logs.len() > 200 {
                    self.logs.remove(0);
                }
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
