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
    #[serde(default)]
    pub period: Vec<VodPeriod>,
}

impl VodPlaybackInfo {
    /// Discovers the HLS master playlist URL from either:
    /// 1. `media` array entries (prioritizing protocol "HLS" or paths with ".m3u8")
    /// 2. `period` adaptation sets containing `otherAttributes.m3u` (prioritizing `video/mp2t` or `hls`)
    /// 3. `period` representations containing `otherAttributes.m3u`
    pub fn find_hls_master_url(&self) -> Option<&str> {
        // 1. Direct media array if present
        if let Some(m) = self.media.iter().find(|m| {
            m.protocol
                .as_deref()
                .is_some_and(|p| p.eq_ignore_ascii_case("hls"))
                || m.path.contains(".m3u8")
        }) {
            return Some(&m.path);
        }

        // 2. NeonPlayer MPD structure (period -> adaptationSet -> otherAttributes.m3u)
        for period in &self.period {
            // Check video/mp2t or hls adaptation sets first
            for set in &period.adaptation_set {
                if set
                    .mime_type
                    .as_deref()
                    .is_some_and(|m| m.contains("mp2t") || m.contains("hls"))
                    && let Some(ref m3u) = set.other_attributes.m3u
                {
                    return Some(m3u.as_str());
                }
            }
            // Check any adaptation set's other_attributes.m3u
            for set in &period.adaptation_set {
                if let Some(ref m3u) = set.other_attributes.m3u {
                    return Some(m3u.as_str());
                }
            }
            // Check representation level other_attributes.m3u
            for set in &period.adaptation_set {
                for rep in &set.representation {
                    if let Some(ref m3u) = rep.other_attributes.m3u {
                        return Some(m3u.as_str());
                    }
                }
            }
        }

        // Fallback to first entry in media if present
        self.media.first().map(|m| m.path.as_str())
    }

    /// Discovers progressive MP4 URLs from either MPD period adaptationSets or media descriptors.
    pub fn find_progressive_mp4_url(&self) -> Option<&str> {
        for period in &self.period {
            for set in &period.adaptation_set {
                for rep in &set.representation {
                    if let Some(base) = rep.base_url.first()
                        && (base.value.contains(".mp4")
                            || rep.mime_type.as_deref() == Some("video/mp4")
                            || set.mime_type.as_deref() == Some("video/mp4"))
                    {
                        return Some(base.value.as_str());
                    }
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodPeriod {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub duration: Option<String>,
    #[serde(default)]
    pub adaptation_set: Vec<VodAdaptationSet>,
    #[serde(default)]
    pub other_attributes: VodOtherAttributes,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodAdaptationSet {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub other_attributes: VodOtherAttributes,
    #[serde(default)]
    pub representation: Vec<VodRepresentation>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodRepresentation {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub bandwidth: Option<u64>,
    #[serde(default)]
    pub frame_rate: Option<String>,
    #[serde(default)]
    pub codecs: Option<String>,
    #[serde(default, alias = "baseURL", alias = "baseUrl")]
    pub base_url: Vec<VodBaseUrl>,
    #[serde(default)]
    pub other_attributes: VodOtherAttributes,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodBaseUrl {
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VodOtherAttributes {
    #[serde(default)]
    pub m3u: Option<String>,
    #[serde(default)]
    pub video_id: Option<String>,
    #[serde(default)]
    pub expire_time: Option<String>,
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
            ..Default::default()
        };

        assert_eq!(
            info.find_hls_master_url(),
            Some("https://example.com/master.m3u8")
        );
    }

    #[test]
    fn test_vod_playback_info_neonplayer_mpd_period_structure() {
        let json = r#"{
            "period": [
                {
                    "adaptationSet": [
                        {
                            "mimeType": "video/mp4",
                            "representation": [
                                {
                                    "id": "PD_1080P",
                                    "width": 1920,
                                    "height": 1080,
                                    "bandwidth": 8000000,
                                    "baseURL": [
                                        { "value": "https://cdn.example.com/vod/1080p.mp4?token=123" }
                                    ]
                                }
                            ]
                        },
                        {
                            "mimeType": "video/mp2t",
                            "otherAttributes": {
                                "m3u": "https://cdn.example.com/vod/master.m3u8?token=123"
                            },
                            "representation": [
                                {
                                    "id": "rep_1080",
                                    "width": 1920,
                                    "height": 1080,
                                    "otherAttributes": {
                                        "m3u": "https://cdn.example.com/vod/1080p.m3u8?token=123"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }"#;

        let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
        assert_eq!(
            info.find_hls_master_url(),
            Some("https://cdn.example.com/vod/master.m3u8?token=123")
        );
        assert_eq!(
            info.find_progressive_mp4_url(),
            Some("https://cdn.example.com/vod/1080p.mp4?token=123")
        );
    }

    #[test]
    fn test_vod_playback_info_neonplayer_representation_m3u_fallback() {
        let json = r#"{
            "period": [
                {
                    "adaptationSet": [
                        {
                            "mimeType": "video/mp2t",
                            "representation": [
                                {
                                    "id": "rep_720",
                                    "width": 1280,
                                    "height": 720,
                                    "otherAttributes": {
                                        "m3u": "https://cdn.example.com/vod/720p.m3u8?token=abc"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }"#;

        let info: VodPlaybackInfo = serde_json::from_str(json).unwrap();
        assert_eq!(
            info.find_hls_master_url(),
            Some("https://cdn.example.com/vod/720p.m3u8?token=abc")
        );
    }
}
