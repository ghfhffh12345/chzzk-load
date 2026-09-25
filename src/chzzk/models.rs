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
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaybackJson {
    #[serde(default)]
    pub media: Vec<MediaEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaEntry {
    pub media_id: String,
    pub path: String,
    #[serde(default)]
    pub encoding_track: Vec<EncodingTrack>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodingTrack {
    pub encoding_track_id: String,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStreamInfo {
    pub channel_id: String,
    pub live_id: Option<u64>,
    pub streamer_name: String,
    pub title: String,
    pub hls_url: String,
    pub chat_channel_id: Option<String>,
}
