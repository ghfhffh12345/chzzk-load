use chrono::{Local, Utc};
use std::fmt::Write;

use crate::chzzk::models_metadata::{
    MetadataDelta, MetadataEvent, MetadataEventType, StreamMetadataState,
};
use crate::recorder::ffmpeg::sanitize_filename;

#[derive(Debug, Clone)]
pub struct FinishedSession {
    pub live_id: Option<u64>,
    pub finished_at: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct ActiveSessionState {
    pub start_timestamp: String,
    pub session_start_instant: std::time::Instant,
    pub streamer_name: String,
    pub alias: Option<String>,
    pub initial_title: String,
    pub current_title: String,
    pub current_metadata: StreamMetadataState,
    pub metadata_history: Vec<MetadataEvent>,
}

impl Default for ActiveSessionState {
    fn default() -> Self {
        Self {
            start_timestamp: String::new(),
            session_start_instant: std::time::Instant::now(),
            streamer_name: String::new(),
            alias: None,
            initial_title: String::new(),
            current_title: String::new(),
            current_metadata: StreamMetadataState::default(),
            metadata_history: Vec::new(),
        }
    }
}

impl ActiveSessionState {
    pub fn new(
        start_timestamp: String,
        streamer_name: String,
        alias: Option<String>,
        initial_metadata: StreamMetadataState,
    ) -> Self {
        let now = Local::now();
        let utc_now = Utc::now();
        let initial_title = initial_metadata.live_title.clone();

        let initial_event = MetadataEvent {
            version: 1,
            event: MetadataEventType::InitialState,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms: 0,
            changes: None,
            state: initial_metadata.clone(),
        };

        Self {
            start_timestamp,
            session_start_instant: std::time::Instant::now(),
            streamer_name,
            alias,
            initial_title: initial_title.clone(),
            current_title: initial_title,
            current_metadata: initial_metadata,
            metadata_history: vec![initial_event],
        }
    }

    pub fn folder_name(&self) -> String {
        let streamer = sanitize_filename(&self.streamer_name);
        let streamer = streamer.trim_end_matches([' ', '.']);
        let streamer = if streamer.is_empty() {
            "Unknown"
        } else {
            streamer
        };

        let title = sanitize_filename(&self.initial_title);
        let title = title.trim_end_matches([' ', '.']);
        let timestamp = &self.start_timestamp;

        let sanitized_alias = self
            .alias
            .as_deref()
            .map(|a| sanitize_filename(a).trim_matches([' ', '.']).to_string())
            .filter(|a| !a.is_empty());

        let folder = if let Some(alias) = sanitized_alias {
            if title.is_empty() {
                format!("[{timestamp}] [{alias}] {streamer}")
            } else {
                format!("[{timestamp}] [{alias}] {streamer} - {title}")
            }
        } else if title.is_empty() {
            format!("[{timestamp}] {streamer}")
        } else {
            format!("[{timestamp}] {streamer} - {title}")
        };
        folder.trim_end_matches([' ', '.']).to_string()
    }

    pub fn record_metadata_change(
        &mut self,
        new_metadata: StreamMetadataState,
    ) -> Option<(MetadataDelta, MetadataEvent)> {
        let delta = self.current_metadata.compute_delta(&new_metadata)?;
        let now = Local::now();
        let utc_now = Utc::now();
        let stream_offset_ms = self.session_start_instant.elapsed().as_millis() as u64;

        let event = MetadataEvent {
            version: 1,
            event: MetadataEventType::MetadataChanged,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms,
            changes: Some(delta.clone()),
            state: new_metadata.clone(),
        };

        self.current_title = new_metadata.live_title.clone();
        self.current_metadata = new_metadata;
        self.metadata_history.push(event.clone());
        Some((delta, event))
    }

    pub fn format_metadata_jsonl(&self) -> String {
        let mut out = String::new();
        for event in &self.metadata_history {
            if let Ok(line) = serde_json::to_string(event) {
                let _ = writeln!(out, "{line}");
            }
        }
        out
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn test_session(streamer: &str, alias: Option<&str>, title: &str) -> ActiveSessionState {
        ActiveSessionState::new(
            "2026-03-30_1200".to_string(),
            streamer.to_string(),
            alias.map(|s| s.to_string()),
            StreamMetadataState {
                live_title: title.to_string(),
                live_category: Some("Game".to_string()),
                live_category_value: Some("Gaming".to_string()),
                tags: vec!["tag1".to_string()],
                ..Default::default()
            },
        )
    }

    #[test]
    pub fn test_active_session_state_stable_folder_name() {
        let mut session = test_session("Streamer", Some("Alias"), "Original Title");
        let initial_folder = session.folder_name();
        assert_eq!(
            initial_folder,
            "[2026-03-30_1200] [Alias] Streamer - Original Title"
        );

        // Simulate title change
        let mut new_meta = session.current_metadata.clone();
        new_meta.live_title = "Brand New Title".to_string();
        let changed = session.record_metadata_change(new_meta);
        assert!(changed.is_some());
        assert_eq!(session.current_title, "Brand New Title");

        // Crucial invariant: folder_name remains anchored to initial title
        assert_eq!(session.folder_name(), initial_folder);
    }

    #[test]
    pub fn test_active_session_state_folder_name_with_alias() {
        let session = test_session("Streamer", Some("MyAlias"), "My Title");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] [MyAlias] Streamer - My Title"
        );
    }

    #[test]
    pub fn test_active_session_state_folder_name_without_alias() {
        let session = test_session("Streamer", None, "My Title");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] Streamer - My Title"
        );
    }

    #[test]
    pub fn test_active_session_state_folder_name_empty_alias_fallback() {
        let session = test_session("Streamer", Some("   "), "My Title");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] Streamer - My Title"
        );
    }

    #[test]
    pub fn test_active_session_state_folder_name_sanitization() {
        let session = test_session("Streamer/Name:Bad*", Some("Alias?Bad|"), "Title<Bad>End.");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] [Alias_Bad_] Streamer_Name_Bad_ - Title_Bad_End"
        );
    }

    #[test]
    pub fn test_active_session_state_folder_name_dots_alias_fallback() {
        let session = test_session("Streamer", Some("..."), "My Title");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] Streamer - My Title"
        );
    }

    #[test]
    pub fn test_active_session_state_folder_name_empty_title() {
        let session = test_session("Streamer", Some("Alias"), "");
        assert_eq!(session.folder_name(), "[2026-03-30_1200] [Alias] Streamer");

        let session_no_alias = test_session("Streamer", None, "");
        assert_eq!(session_no_alias.folder_name(), "[2026-03-30_1200] Streamer");
    }

    #[test]
    pub fn test_active_session_state_folder_name_empty_streamer() {
        let session = test_session("", None, "My Title");
        assert_eq!(
            session.folder_name(),
            "[2026-03-30_1200] Unknown - My Title"
        );
    }

    #[test]
    pub fn test_active_session_state_metadata_jsonl_formatting() {
        let mut session = test_session("Streamer", None, "Initial");
        let initial_jsonl = session.format_metadata_jsonl();
        assert_eq!(initial_jsonl.lines().count(), 1);

        let parsed_initial: serde_json::Value =
            serde_json::from_str(initial_jsonl.lines().next().unwrap()).unwrap();
        assert_eq!(parsed_initial["version"], 1);
        assert_eq!(parsed_initial["event"], "INITIAL_STATE");
        assert_eq!(parsed_initial["stream_offset_ms"], 0);
        assert_eq!(parsed_initial["state"]["live_title"], "Initial");
        assert!(parsed_initial["changes"].is_null());

        let mut updated_meta = session.current_metadata.clone();
        updated_meta.live_title = "Second Title".to_string();
        session.record_metadata_change(updated_meta);

        let multi_jsonl = session.format_metadata_jsonl();
        let lines: Vec<&str> = multi_jsonl.lines().collect();
        assert_eq!(lines.len(), 2);

        let parsed_second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed_second["version"], 1);
        assert_eq!(parsed_second["event"], "METADATA_CHANGED");
        assert_eq!(parsed_second["changes"]["live_title"]["old"], "Initial");
        assert_eq!(
            parsed_second["changes"]["live_title"]["new"],
            "Second Title"
        );
        assert_eq!(parsed_second["state"]["live_title"], "Second Title");
    }
}
