use chrono::{DateTime, NaiveDateTime};

use crate::chzzk::models_vod::VodDetail;
use crate::recorder::ffmpeg::sanitize_filename;

/// Normalizes and formats the authoritative broadcast start anchor timestamp for directory naming.
///
/// Priority:
/// 1. `live_open_date` (authoritative live broadcast start timestamp)
/// 2. `publish_date` (fallback for non-live or standalone uploaded videos)
/// 3. Current local time (if both are absent)
///
/// Converts timestamps into the canonical Windows-safe format: `YYYY-MM-DD_HHMMSS`.
pub fn format_broadcast_anchor_timestamp(
    live_open_date: Option<&str>,
    publish_date: Option<&str>,
) -> String {
    let raw = live_open_date
        .filter(|s| !s.trim().is_empty())
        .or_else(|| publish_date.filter(|s| !s.trim().is_empty()));

    if let Some(date_str) = raw {
        let trimmed = date_str.trim();

        // 1. Try NaiveDateTime with standard formats
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
            return dt.format("%Y-%m-%d_%H%M%S").to_string();
        }
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M") {
            return dt.format("%Y-%m-%d_%H%M%S").to_string();
        }
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d_%H%M%S") {
            return dt.format("%Y-%m-%d_%H%M%S").to_string();
        }

        // 2. Try RFC 3339 / ISO 8601
        if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
            return dt.format("%Y-%m-%d_%H%M%S").to_string();
        }

        // 3. Fallback: sanitize colons and spaces to ensure Windows path safety
        return sanitize_filename(trimmed)
            .replace(':', "-")
            .replace(' ', "_");
    }

    chrono::Local::now().format("%Y-%m-%d_%H%M%S").to_string()
}

/// Parses the broadcast start anchor date into UTC epoch timestamp milliseconds.
pub fn parse_broadcast_anchor_timestamp_ms(
    live_open_date: Option<&str>,
    publish_date: Option<&str>,
) -> Option<u64> {
    let raw = live_open_date
        .filter(|s| !s.trim().is_empty())
        .or_else(|| publish_date.filter(|s| !s.trim().is_empty()))?;

    let trimmed = raw.trim();
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_millis().max(0) as u64);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M") {
        return Some(dt.and_utc().timestamp_millis().max(0) as u64);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.timestamp_millis().max(0) as u64);
    }
    None
}

/// Generates the canonical VOD archive folder name:
/// `[{timestamp}] [{alias}] {channel} - {title}` (omitting `[{alias}]` if None).
///
/// All components (channel name, title, alias) are sanitized against illegal filesystem characters
/// and trailing dots/spaces to ensure cross-platform safety across Windows, macOS, and Linux.
pub fn resolve_vod_folder_name(detail: &VodDetail, alias: Option<&str>) -> String {
    let timestamp = format_broadcast_anchor_timestamp(
        detail.live_open_date.as_deref(),
        detail.publish_date.as_deref(),
    );

    let streamer_raw = sanitize_filename(&detail.channel.channel_name);
    let streamer = streamer_raw.trim_end_matches([' ', '.']);
    let streamer = if streamer.is_empty() {
        "Unknown"
    } else {
        streamer
    };

    let title_raw = sanitize_filename(&detail.video_title);
    let title = title_raw.trim_end_matches([' ', '.']);

    let sanitized_alias = alias
        .map(|a| sanitize_filename(a).trim_matches([' ', '.']).to_string())
        .filter(|a| !a.is_empty());

    if let Some(a) = sanitized_alias {
        if title.is_empty() {
            format!("[{timestamp}] [{a}] {streamer}")
        } else {
            format!("[{timestamp}] [{a}] {streamer} - {title}")
        }
    } else if title.is_empty() {
        format!("[{timestamp}] {streamer}")
    } else {
        format!("[{timestamp}] {streamer} - {title}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chzzk::models::ChannelInfo;

    fn sample_vod_detail(
        live_open: Option<&str>,
        publish: Option<&str>,
        title: &str,
        streamer: &str,
    ) -> VodDetail {
        VodDetail {
            video_no: 12345,
            video_id: "test_vid".to_string(),
            video_title: title.to_string(),
            in_key: None,
            live_open_date: live_open.map(String::from),
            publish_date: publish.map(String::from),
            duration: Some(3600),
            channel: ChannelInfo {
                channel_id: "chan_id".to_string(),
                channel_name: streamer.to_string(),
                channel_image_url: None,
                verified_mark: None,
            },
            category_type: None,
            video_category: None,
            video_category_value: None,
            tags: vec![],
            paid_promotion: None,
            adult: None,
        }
    }

    #[test]
    fn test_format_broadcast_anchor_timestamp_priority() {
        // 1. liveOpenDate prioritized over publishDate
        assert_eq!(
            format_broadcast_anchor_timestamp(
                Some("2026-09-30 11:00:00"),
                Some("2026-09-30 15:30:00")
            ),
            "2026-09-30_110000"
        );

        // 2. publishDate fallback when liveOpenDate is None
        assert_eq!(
            format_broadcast_anchor_timestamp(None, Some("2026-09-30 15:30:00")),
            "2026-09-30_153000"
        );

        // 3. publishDate fallback when liveOpenDate is empty
        assert_eq!(
            format_broadcast_anchor_timestamp(Some("   "), Some("2026-09-30 15:30:00")),
            "2026-09-30_153000"
        );
    }

    #[test]
    fn test_resolve_vod_folder_name_with_alias() {
        let detail = sample_vod_detail(
            Some("2026-09-30 11:00:00"),
            None,
            "Exciting Gameplay",
            "StreamerPro",
        );
        let folder = resolve_vod_folder_name(&detail, Some("MyAlias"));
        assert_eq!(
            folder,
            "[2026-09-30_110000] [MyAlias] StreamerPro - Exciting Gameplay"
        );
    }

    #[test]
    fn test_resolve_vod_folder_name_without_alias() {
        let detail = sample_vod_detail(
            Some("2026-09-30 11:00:00"),
            None,
            "Exciting Gameplay",
            "StreamerPro",
        );
        let folder = resolve_vod_folder_name(&detail, None);
        assert_eq!(
            folder,
            "[2026-09-30_110000] StreamerPro - Exciting Gameplay"
        );
    }

    #[test]
    fn test_resolve_vod_folder_name_sanitizes_illegal_characters() {
        let detail = sample_vod_detail(
            Some("2026-09-30 11:00:00"),
            None,
            "Is this stream real? Yes! <3 | Part 1...",
            "Streamer / Name...",
        );
        let folder = resolve_vod_folder_name(&detail, Some("Alias:Special?"));
        assert_eq!(
            folder,
            "[2026-09-30_110000] [Alias_Special_] Streamer _ Name - Is this stream real_ Yes! _3 _ Part 1"
        );
    }

    #[test]
    fn test_format_broadcast_anchor_timestamp_none_fallback_to_now() {
        let ts = format_broadcast_anchor_timestamp(None, None);
        assert_eq!(ts.len(), 17); // YYYY-MM-DD_HHMMSS
        assert!(ts.contains('_'));

        let ts_empty = format_broadcast_anchor_timestamp(Some("   "), Some(""));
        assert_eq!(ts_empty.len(), 17);
    }

    #[test]
    fn test_parse_broadcast_anchor_timestamp_ms_priority() {
        // live_open_date takes precedence
        let ms = parse_broadcast_anchor_timestamp_ms(
            Some("2026-09-30 11:00:00"),
            Some("2026-09-30 15:30:00"),
        );
        assert!(ms.is_some());

        // publish_date fallback
        let ms_fallback = parse_broadcast_anchor_timestamp_ms(None, Some("2026-09-30 15:30:00"));
        assert!(ms_fallback.is_some());
        assert_ne!(ms, ms_fallback);

        // both absent returns None
        assert_eq!(parse_broadcast_anchor_timestamp_ms(None, None), None);
        assert_eq!(
            parse_broadcast_anchor_timestamp_ms(Some(" "), Some("")),
            None
        );
    }

    #[test]
    fn test_resolve_vod_folder_name_empty_title_and_streamer_edge_cases() {
        let detail_empty_title =
            sample_vod_detail(Some("2026-09-30 11:00:00"), None, "   ...  ", "Streamer");
        let folder = resolve_vod_folder_name(&detail_empty_title, None);
        assert_eq!(folder, "[2026-09-30_110000] Streamer");

        let detail_empty_streamer =
            sample_vod_detail(Some("2026-09-30 11:00:00"), None, "My Cool VOD", "   ...  ");
        let folder_unknown = resolve_vod_folder_name(&detail_empty_streamer, None);
        assert_eq!(folder_unknown, "[2026-09-30_110000] Unknown - My Cool VOD");
    }
}
