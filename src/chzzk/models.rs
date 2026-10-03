use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LiveStatus {
    Open,
    Close,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChzzkResponse<T> {
    pub code: i32,
    pub message: Option<String>,
    pub content: Option<T>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelInfo {
    pub channel_id: String,
    pub channel_name: String,
    #[serde(default)]
    pub channel_image_url: Option<String>,
    #[serde(default)]
    pub verified_mark: Option<bool>,
}

pub fn deserialize_optional_u64_or_string<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }

    match Option::<NumOrStr>::deserialize(deserializer)? {
        Some(NumOrStr::Num(n)) => Ok(Some(n)),
        Some(NumOrStr::Str(s)) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

pub fn deserialize_optional_i64_or_string<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(i64),
        Str(String),
    }

    match Option::<NumOrStr>::deserialize(deserializer)? {
        Some(NumOrStr::Num(n)) => Ok(Some(n)),
        Some(NumOrStr::Str(s)) => s
            .trim()
            .parse::<i64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveDetailContent {
    #[serde(default, deserialize_with = "deserialize_optional_u64_or_string")]
    pub live_id: Option<u64>,
    pub status: String,
    pub live_title: Option<String>,
    pub channel: ChannelInfo,
    pub live_playback_json: Option<String>,
    #[serde(default)]
    pub chat_channel_id: Option<String>,
    #[serde(default)]
    pub adult: Option<bool>,
    #[serde(default)]
    pub open_date: Option<String>,
    #[serde(default)]
    pub close_date: Option<String>,
    #[serde(default)]
    pub category_type: Option<String>,
    #[serde(default)]
    pub live_category: Option<String>,
    #[serde(default)]
    pub live_category_value: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub paid_promotion: Option<bool>,
    #[serde(
        default,
        deserialize_with = "crate::chzzk::models_metadata::deserialize_optional_string_or_number"
    )]
    pub drops_campaign_no: Option<String>,
    #[serde(default)]
    pub kr_only_viewing: Option<bool>,
    #[serde(default)]
    pub clip_active: Option<bool>,
    #[serde(default)]
    pub time_machine_active: Option<bool>,
    #[serde(default)]
    pub chat_active: Option<bool>,
    #[serde(default)]
    pub chat_available_group: Option<String>,
    #[serde(default)]
    pub chat_available_condition: Option<String>,
    #[serde(default)]
    pub min_follower_minute: Option<u32>,
    #[serde(default)]
    pub allow_subscriber_in_follower_mode: Option<bool>,
    #[serde(default)]
    pub chat_slow_mode_sec: Option<u32>,
    #[serde(default)]
    pub chat_emoji_mode: Option<bool>,
    #[serde(default)]
    pub chat_donation_ranking_exposure: Option<bool>,
    #[serde(default)]
    pub live_image_url: Option<String>,
    #[serde(default)]
    pub default_thumbnail_image_url: Option<String>,
    #[serde(default)]
    pub concurrent_user_count: Option<u64>,
    #[serde(default)]
    pub accumulate_count: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_i64_or_string")]
    pub watch_party_no: Option<i64>,
    #[serde(default)]
    pub watch_party_tag: Option<String>,
    #[serde(default)]
    pub watch_party_type: Option<String>,
    #[serde(default)]
    pub watch_party_paid_product_id: Option<serde_json::Value>,
    #[serde(default)]
    pub paid_product: Option<serde_json::Value>,
    #[serde(default)]
    pub live_polling_status_json: Option<String>,
    #[serde(default)]
    pub user_adult_status: Option<String>,
    #[serde(default)]
    pub membership_benefit_type: Option<String>,
    #[serde(default)]
    pub tv_app_viewing_policy_type: Option<String>,
    #[serde(default)]
    pub blind_type: Option<serde_json::Value>,
    #[serde(default)]
    pub log_power_active: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaybackMeta {
    pub video_id: Option<String>,
    pub stream_seq: Option<u64>,
    pub live_id: Option<String>,
    pub paid_live: Option<bool>,
    pub playback_auth_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaybackJson {
    #[serde(default)]
    pub meta: Option<PlaybackMeta>,
    #[serde(default)]
    pub media: Vec<MediaEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LivePollingStatus {
    pub status: Option<String>,
    pub is_publishing: Option<bool>,
    pub playable_status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaEntry {
    pub media_id: String,
    pub path: String,
    #[serde(default)]
    pub p2p_path: Option<String>,
    #[serde(default)]
    pub p2p_path_url_encoding: Option<String>,
    #[serde(default)]
    pub encoding_track: Vec<EncodingTrack>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodingTrack {
    pub encoding_track_id: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub p2p_path: Option<String>,
    #[serde(default)]
    pub p2p_path_url_encoding: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStreamInfo {
    pub channel_id: String,
    pub live_id: Option<u64>,
    pub streamer_name: String,
    pub title: String,
    pub hls_url: String,
    pub chat_channel_id: Option<String>,
    pub metadata: crate::chzzk::models_metadata::StreamMetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum LiveDetail {
    /// Stream is OPEN and media HLS URL is available for recording.
    Open(LiveStreamInfo),
    /// Stream is OPEN, but recording is unavailable (restricted / 19+ / missing credentials).
    Restricted {
        channel_id: String,
        live_id: Option<u64>,
        streamer_name: String,
        title: String,
        chat_channel_id: Option<String>,
        adult: bool,
    },
    /// Channel is CLOSE (offline).
    Close { streamer_name: Option<String> },
}
