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

/// Paginated VOD chat response payload representing a slice of chat history across the timeline.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodChatPage {
    #[serde(default, alias = "chats", alias = "videoChats")]
    pub video_chats: Vec<serde_json::Value>,
    #[serde(default)]
    pub next_player_message_time: Option<u64>,
}

/// Typed error outcomes encountered during VOD chat pagination.
#[derive(Debug, thiserror::Error)]
pub enum VodChatError {
    #[error("Rate limited (HTTP 429 Too Many Requests)")]
    RateLimited,
    #[error("VOD chat unavailable or disabled: {0}")]
    Unavailable(String),
    #[error("API network or parsing error: {0}")]
    Transport(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vod_chat_page_deserialization_with_video_chats() {
        let json = r#"{
            "videoChats": [
                {
                    "messageTime": 1711234567890,
                    "playerMessageTime": 12000,
                    "content": "hello from VOD"
                }
            ],
            "nextPlayerMessageTime": 25000
        }"#;

        let page: VodChatPage = serde_json::from_str(json).unwrap();
        assert_eq!(page.video_chats.len(), 1);
        assert_eq!(page.next_player_message_time, Some(25000));
        assert_eq!(page.video_chats[0]["playerMessageTime"], 12000);
        assert_eq!(page.video_chats[0]["content"], "hello from VOD");
    }

    #[test]
    fn test_vod_chat_page_deserialization_with_chats_alias() {
        let json = r#"{
            "chats": [
                {
                    "messageTime": 1711234570000,
                    "playerMessageTime": 30000,
                    "msg": "another chat"
                }
            ]
        }"#;

        let page: VodChatPage = serde_json::from_str(json).unwrap();
        assert_eq!(page.video_chats.len(), 1);
        assert_eq!(page.next_player_message_time, None);
        assert_eq!(page.video_chats[0]["msg"], "another chat");
    }

    #[test]
    fn test_vod_chat_page_empty_deserialization() {
        let json = r#"{}"#;
        let page: VodChatPage = serde_json::from_str(json).unwrap();
        assert!(page.video_chats.is_empty());
        assert_eq!(page.next_player_message_time, None);
    }

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
