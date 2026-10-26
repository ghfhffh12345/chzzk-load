use serde::{Deserialize, Serialize};

use crate::chzzk::models::ChannelInfo;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VodDetail {
    pub video_no: u64,
    pub video_id: String,
    pub video_title: String,
    #[serde(default)]
    pub in_key: Option<String>,
    #[serde(default)]
    pub live_open_date: Option<String>,
    #[serde(default)]
    pub publish_date: Option<String>,
    #[serde(default)]
    pub duration: Option<u64>,
    pub channel: ChannelInfo,
    #[serde(default)]
    pub category_type: Option<String>,
    #[serde(default)]
    pub video_category: Option<String>,
    #[serde(default)]
    pub video_category_value: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub paid_promotion: Option<bool>,
    #[serde(default)]
    pub adult: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodPlaybackInfo {
    #[serde(default)]
    pub media: Vec<VodPlaybackMedia>,
}

impl VodPlaybackInfo {
    /// Discovers the HLS master playlist URL from the playback media descriptors.
    /// Prioritizes entries with protocol "HLS" or paths containing ".m3u8".
    pub fn find_hls_master_url(&self) -> Option<&str> {
        self.media
            .iter()
            .find(|m| {
                m.protocol
                    .as_deref()
                    .is_some_and(|p| p.eq_ignore_ascii_case("hls"))
                    || m.path.contains(".m3u8")
            })
            .or_else(|| self.media.first())
            .map(|m| m.path.as_str())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodPlaybackMedia {
    #[serde(default)]
    pub media_id: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
    pub path: String,
    #[serde(default)]
    pub encoding_option: Option<VodEncodingOption>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodEncodingOption {
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub bitrate: Option<u64>,
    #[serde(default)]
    pub name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vod_detail_deserialization() {
        let json = r#"{
            "videoNo": 4531238,
            "videoId": "neon_video_xyz",
            "videoTitle": "VOD Title Sample",
            "inKey": "sample_inkey_123",
            "liveOpenDate": "2026-09-30 11:00:00",
            "publishDate": "2026-09-30 15:00:00",
            "duration": 7200,
            "channel": {
                "channelId": "chan_123",
                "channelName": "StreamerName"
            },
            "categoryType": "GAME",
            "tags": ["game", "rpg"],
            "paidPromotion": false,
            "adult": false
        }"#;

        let detail: VodDetail = serde_json::from_str(json).unwrap();
        assert_eq!(detail.video_no, 4531238);
        assert_eq!(detail.video_id, "neon_video_xyz");
        assert_eq!(detail.video_title, "VOD Title Sample");
        assert_eq!(detail.in_key.as_deref(), Some("sample_inkey_123"));
        assert_eq!(detail.duration, Some(7200));
        assert_eq!(detail.channel.channel_name, "StreamerName");
        assert_eq!(detail.tags, vec!["game", "rpg"]);
    }

    #[test]
    fn test_vod_playback_info_find_hls() {
        let info = VodPlaybackInfo {
            media: vec![
                VodPlaybackMedia {
                    media_id: Some("MPD".to_string()),
                    protocol: Some("DASH".to_string()),
                    path: "https://example.com/stream.mpd".to_string(),
                    encoding_option: None,
                },
                VodPlaybackMedia {
                    media_id: Some("HLS".to_string()),
                    protocol: Some("HLS".to_string()),
                    path: "https://example.com/master.m3u8".to_string(),
                    encoding_option: None,
                },
            ],
        };

        assert_eq!(
            info.find_hls_master_url(),
            Some("https://example.com/master.m3u8")
        );
    }
}
