use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::chzzk::models_vod::VodDetail;
use crate::vod::naming::parse_broadcast_anchor_timestamp_ms;

const fn default_metadata_version() -> u8 {
    2
}

/// Event discriminator representing boundary markers in the Dual-Event Boundary Model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VodMetadataEventType {
    #[serde(alias = "START")]
    Start,
    #[serde(alias = "FINISH")]
    Finish,
}

/// Metadata snapshot containing video classifications, identity, and access tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VodMetadataState {
    pub video_no: u64,
    pub video_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_open_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_date: Option<String>,
    #[serde(alias = "video_title")]
    pub title: String,
    #[serde(alias = "streamer_name")]
    pub channel_name: String,
    pub channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_type: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, alias = "paid_promotion")]
    pub paid: bool,
    #[serde(default)]
    pub adult: bool,
}

impl From<&VodDetail> for VodMetadataState {
    fn from(detail: &VodDetail) -> Self {
        Self {
            video_no: detail.video_no,
            video_id: detail.video_id.clone(),
            live_open_date: detail.live_open_date.clone(),
            publish_date: detail.publish_date.clone(),
            title: detail.video_title.clone(),
            channel_name: detail.channel.channel_name.clone(),
            channel_id: detail.channel.channel_id.clone(),
            category_type: detail.category_type.clone(),
            tags: detail.tags.clone(),
            paid: detail.paid_promotion.unwrap_or(false),
            adult: detail.adult.unwrap_or(false),
        }
    }
}

/// Single JSON Lines event record in `vod_metadata.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VodMetadataEvent {
    #[serde(default = "default_metadata_version")]
    pub version: u8,
    pub event: VodMetadataEventType,
    pub timestamp: String,
    pub stream_offset_ms: u64,
    pub state: VodMetadataState,
}

impl VodMetadataEvent {
    pub fn new(
        event: VodMetadataEventType,
        timestamp: String,
        stream_offset_ms: u64,
        state: VodMetadataState,
    ) -> Self {
        Self {
            version: 2,
            event,
            timestamp,
            stream_offset_ms,
            state,
        }
    }

    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        let mut line = serde_json::to_string(self)?;
        line.push('\n');
        Ok(line)
    }
}

/// Synthesizes the initial `start` and terminal `finish` events for the Dual-Event Boundary Model.
pub fn synthesize_vod_metadata_events(
    detail: &VodDetail,
    duration_ms: u64,
) -> (VodMetadataEvent, VodMetadataEvent) {
    let state = VodMetadataState::from(detail);

    // Resolve start timestamp in RFC 3339 format
    let anchor_ms = parse_broadcast_anchor_timestamp_ms(
        detail.live_open_date.as_deref(),
        detail.publish_date.as_deref(),
    );

    let start_dt = anchor_ms
        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
        .unwrap_or_else(Utc::now);

    let finish_dt = anchor_ms
        .and_then(|ms| DateTime::from_timestamp_millis((ms + duration_ms) as i64))
        .unwrap_or_else(|| start_dt + chrono::Duration::milliseconds(duration_ms as i64));

    let start_timestamp = start_dt.to_rfc3339_opts(SecondsFormat::Secs, true);
    let finish_timestamp = finish_dt.to_rfc3339_opts(SecondsFormat::Secs, true);

    let start_event = VodMetadataEvent::new(
        VodMetadataEventType::Start,
        start_timestamp,
        0,
        state.clone(),
    );

    let finish_event = VodMetadataEvent::new(
        VodMetadataEventType::Finish,
        finish_timestamp,
        duration_ms,
        state,
    );

    (start_event, finish_event)
}

/// Formats the complete dual-event `vod_metadata.jsonl` payload.
pub fn format_vod_metadata_jsonl(detail: &VodDetail, duration_ms: u64) -> Result<String> {
    let (start, finish) = synthesize_vod_metadata_events(detail, duration_ms);
    let mut out = String::new();
    out.push_str(
        &start
            .to_json_line()
            .context("Failed to serialize start event")?,
    );
    out.push_str(
        &finish
            .to_json_line()
            .context("Failed to serialize finish event")?,
    );
    Ok(out)
}

/// Staged metadata file target paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodMetadataTarget {
    pub part_path: PathBuf,
    pub final_path: PathBuf,
}

/// Synthesizes and writes `vod_metadata.jsonl.part` into the destination directory.
pub async fn synthesize_vod_metadata_to_staged(
    output_dir: &Path,
    detail: &VodDetail,
    duration_ms: u64,
) -> Result<VodMetadataTarget> {
    let part_path = output_dir.join("vod_metadata.jsonl.part");
    let final_path = output_dir.join("vod_metadata.jsonl");

    let content = format_vod_metadata_jsonl(detail, duration_ms)?;
    tokio::fs::write(&part_path, content.as_bytes())
        .await
        .with_context(|| {
            format!(
                "Failed to write staged VOD metadata to '{}'",
                part_path.display()
            )
        })?;

    Ok(VodMetadataTarget {
        part_path,
        final_path,
    })
}

/// Atomically promotes `vod_metadata.jsonl.part` to `vod_metadata.jsonl`.
pub async fn promote_vod_metadata(part_path: &Path, final_path: &Path) -> Result<()> {
    crate::uploader::rename_local_file_with_retry(part_path, final_path)
        .await
        .context("Failed to promote staged VOD metadata to final destination")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chzzk::models::ChannelInfo;

    fn sample_detail() -> VodDetail {
        VodDetail {
            video_no: 4531238,
            video_id: "neon_xyz_123".to_string(),
            video_title: "Chzzk Live Replay Title".to_string(),
            in_key: None,
            live_rewind_playback_json: None,
            live_open_date: Some("2026-09-30 11:00:00".to_string()),
            publish_date: Some("2026-09-30 15:00:00".to_string()),
            duration: Some(7200),
            channel: ChannelInfo {
                channel_id: "chan_progamer".to_string(),
                channel_name: "ProGamer".to_string(),
                channel_image_url: None,
                verified_mark: None,
            },
            category_type: Some("GAME".to_string()),
            video_category: None,
            video_category_value: None,
            tags: vec!["game".to_string(), "rpg".to_string()],
            paid_promotion: Some(true),
            adult: Some(false),
        }
    }

    #[test]
    fn test_synthesize_vod_metadata_events_dual_boundary() {
        let detail = sample_detail();
        let (start, finish) = synthesize_vod_metadata_events(&detail, 7_200_000);

        // Verify start event attributes
        assert_eq!(start.version, 2);
        assert_eq!(start.event, VodMetadataEventType::Start);
        assert_eq!(start.stream_offset_ms, 0);
        assert_eq!(start.state.video_no, 4531238);
        assert_eq!(start.state.video_id, "neon_xyz_123");
        assert_eq!(start.state.title, "Chzzk Live Replay Title");
        assert_eq!(start.state.channel_name, "ProGamer");
        assert_eq!(start.state.channel_id, "chan_progamer");
        assert_eq!(start.state.category_type.as_deref(), Some("GAME"));
        assert_eq!(start.state.tags, vec!["game", "rpg"]);
        assert!(start.state.paid);
        assert!(!start.state.adult);
        assert_eq!(
            start.state.live_open_date.as_deref(),
            Some("2026-09-30 11:00:00")
        );
        assert_eq!(
            start.state.publish_date.as_deref(),
            Some("2026-09-30 15:00:00")
        );

        // Verify finish event attributes
        assert_eq!(finish.version, 2);
        assert_eq!(finish.event, VodMetadataEventType::Finish);
        assert_eq!(finish.stream_offset_ms, 7_200_000);
        assert_eq!(finish.state, start.state);
    }

    #[test]
    fn test_synthesize_vod_metadata_events_without_anchor_dates_preserves_duration() {
        let mut detail = sample_detail();
        detail.live_open_date = None;
        detail.publish_date = None;

        let (start, finish) = synthesize_vod_metadata_events(&detail, 3_600_000);
        assert_eq!(start.stream_offset_ms, 0);
        assert_eq!(finish.stream_offset_ms, 3_600_000);

        // Verify timestamps reflect 1 hour (3600s) difference despite missing anchor dates
        let start_dt = DateTime::parse_from_rfc3339(&start.timestamp).unwrap();
        let finish_dt = DateTime::parse_from_rfc3339(&finish.timestamp).unwrap();
        let diff_secs = (finish_dt - start_dt).num_seconds();
        assert_eq!(diff_secs, 3600);
    }

    #[test]
    fn test_vod_metadata_jsonl_serialization_and_roundtrip() {
        let detail = sample_detail();
        let jsonl = format_vod_metadata_jsonl(&detail, 7_200_000).unwrap();
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 2);

        // Validate line 1 (start event)
        let val_start: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(val_start["version"], 2);
        assert_eq!(val_start["event"], "start");
        assert_eq!(val_start["stream_offset_ms"], 0);
        assert_eq!(val_start["state"]["video_no"], 4531238);
        assert_eq!(val_start["state"]["video_id"], "neon_xyz_123");
        assert_eq!(val_start["state"]["title"], "Chzzk Live Replay Title");
        assert_eq!(val_start["state"]["channel_name"], "ProGamer");
        assert_eq!(val_start["state"]["channel_id"], "chan_progamer");
        assert_eq!(val_start["state"]["category_type"], "GAME");
        assert_eq!(
            val_start["state"]["tags"],
            serde_json::json!(["game", "rpg"])
        );
        assert_eq!(val_start["state"]["paid"], true);
        assert_eq!(val_start["state"]["adult"], false);

        // Validate line 2 (finish event)
        let val_finish: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(val_finish["version"], 2);
        assert_eq!(val_finish["event"], "finish");
        assert_eq!(val_finish["stream_offset_ms"], 7_200_000);
        assert_eq!(val_finish["state"]["video_no"], 4531238);

        // Verify deserialization back to typed VodMetadataEvent
        let parsed_start: VodMetadataEvent = serde_json::from_str(lines[0]).unwrap();
        let parsed_finish: VodMetadataEvent = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed_start.event, VodMetadataEventType::Start);
        assert_eq!(parsed_finish.event, VodMetadataEventType::Finish);
        assert_eq!(parsed_finish.stream_offset_ms, 7_200_000);
    }

    #[test]
    fn test_vod_metadata_event_case_insensitive_and_alias_deserialization() {
        let upper_json = r#"{
            "version": 2,
            "event": "START",
            "timestamp": "2026-09-30T11:00:00Z",
            "stream_offset_ms": 0,
            "state": {
                "video_no": 9999,
                "video_id": "v999",
                "video_title": "Alias Title",
                "streamer_name": "Alias Streamer",
                "channel_id": "c999",
                "paid_promotion": true,
                "adult": true
            }
        }"#;

        let parsed: VodMetadataEvent = serde_json::from_str(upper_json).unwrap();
        assert_eq!(parsed.event, VodMetadataEventType::Start);
        assert_eq!(parsed.state.title, "Alias Title");
        assert_eq!(parsed.state.channel_name, "Alias Streamer");
        assert!(parsed.state.paid);
        assert!(parsed.state.adult);
    }

    #[tokio::test]
    async fn test_synthesize_and_promote_vod_metadata_staged() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_meta_staged_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let detail = sample_detail();
        let target = synthesize_vod_metadata_to_staged(&temp_dir, &detail, 3_600_000)
            .await
            .expect("synthesizing staged metadata should succeed");

        assert!(target.part_path.exists());
        assert!(!target.final_path.exists());

        promote_vod_metadata(&target.part_path, &target.final_path)
            .await
            .expect("promoting metadata should succeed");

        assert!(!target.part_path.exists());
        assert!(target.final_path.exists());

        let lines: Vec<String> = tokio::fs::read_to_string(&target.final_path)
            .await
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        assert_eq!(lines.len(), 2);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
