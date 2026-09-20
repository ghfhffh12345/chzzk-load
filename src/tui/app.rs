use crossterm::event::KeyCode;

use crate::tui::event::AppEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelItem {
    pub id: String,
    pub name: String,
    pub is_live: bool,
    pub title: String,
}

#[derive(Debug, Clone)]
pub struct App {
    pub channels: Vec<ChannelItem>,
    pub selected_channel_idx: usize,
    pub active_stream: Option<String>,
    pub active_upload_name: Option<String>,
    pub upload_progress_pct: u16,
    pub upload_speed: f64,
    pub uploaded_count: usize,
    pub reclaimed_mb: f64,
    pub logs: Vec<String>,
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
            active_stream: None,
            active_upload_name: None,
            upload_progress_pct: 0,
            upload_speed: 0.0,
            uploaded_count: 0,
            reclaimed_mb: 0.0,
            logs: Vec::new(),
            should_quit: false,
            refresh_requested: false,
        }
    }

    pub fn next_channel(&mut self) {
        if !self.channels.is_empty() && self.selected_channel_idx < self.channels.len() - 1 {
            self.selected_channel_idx += 1;
        }
    }

    pub fn prev_channel(&mut self) {
        if self.selected_channel_idx > 0 {
            self.selected_channel_idx -= 1;
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
                } else {
                    self.channels.push(ChannelItem {
                        id: channel_id,
                        name: channel_name,
                        is_live,
                        title,
                    });
                }
            }
            AppEvent::RecordingStarted {
                channel_id,
                session_title,
            } => {
                self.active_stream = Some(format!("{}: {}", channel_id, session_title));
            }
            AppEvent::UploadProgress {
                chunk_name,
                uploaded_bytes,
                total_bytes,
                speed_mb_s,
            } => {
                self.active_upload_name = Some(chunk_name);
                if total_bytes > 0 {
                    self.upload_progress_pct =
                        ((uploaded_bytes as f64 / total_bytes as f64) * 100.0).round().min(100.0) as u16;
                }
                self.upload_speed = speed_mb_s;
            }
            AppEvent::UploadCompleted {
                chunk_name,
                reclaimed_bytes,
            } => {
                self.uploaded_count += 1;
                self.reclaimed_mb += reclaimed_bytes as f64 / 1_048_576.0;
                if self.active_upload_name.as_deref() == Some(&chunk_name) {
                    self.active_upload_name = None;
                    self.upload_progress_pct = 0;
                }
            }
            AppEvent::Log(msg) => {
                self.logs.push(msg);
                if self.logs.len() > 200 {
                    self.logs.remove(0);
                }
            }
            AppEvent::Key(key) => match key.code {
                KeyCode::Char('q') => self.should_quit = true,
                KeyCode::Char('r') => self.refresh_requested = true,
                KeyCode::Up | KeyCode::Char('k') => self.prev_channel(),
                KeyCode::Down | KeyCode::Char('j') => self.next_channel(),
                _ => {}
            },
            _ => {}
        }
    }
}
