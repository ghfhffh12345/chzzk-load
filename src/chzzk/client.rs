use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, USER_AGENT};

use crate::chzzk::models::{ChzzkResponse, LiveDetailContent, LiveStreamInfo, PlaybackJson};
use crate::chzzk::models_chat::ChatAccessTokenResponse;
use crate::config::ChzzkConfig;

const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};

use crate::chzzk::models::EncodingTrack;

fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let input_bytes = input.as_bytes();
    let mut i = 0;
    while i < input_bytes.len() {
        if input_bytes[i] == b'%'
            && i + 2 < input_bytes.len()
            && let Ok(val) = u8::from_str_radix(
                std::str::from_utf8(&input_bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            )
        {
            bytes.push(val);
            i += 3;
            continue;
        }
        bytes.push(input_bytes[i]);
        i += 1;
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_string())
}

fn decode_base64_url(encoded: &str) -> Option<String> {
    let trimmed = encoded.trim();
    if trimmed.is_empty() {
        return None;
    }

    let decoded_bytes = STANDARD
        .decode(trimmed)
        .or_else(|_| STANDARD_NO_PAD.decode(trimmed))
        .or_else(|_| URL_SAFE.decode(trimmed))
        .or_else(|_| URL_SAFE_NO_PAD.decode(trimmed))
        .ok()?;

    let decoded_str = String::from_utf8(decoded_bytes).ok()?;
    let url = decoded_str.trim();
    if url.starts_with("http://") || url.starts_with("https://") {
        Some(url.to_string())
    } else {
        None
    }
}

pub fn extract_cdn_url(s: &str) -> Option<String> {
    let lower = s.to_ascii_lowercase();
    let val_start = if let Some(idx) = lower.find("cdn_url=") {
        idx + "cdn_url=".len()
    } else {
        let idx = lower.find("cdn_url%3d")?;
        idx + "cdn_url%3d".len()
    };

    let remainder = &s[val_start..];
    let raw_val = if let Some(end) = remainder.find('&') {
        &remainder[..end]
    } else if let Some(end) = remainder.to_ascii_lowercase().find("%26") {
        &remainder[..end]
    } else {
        remainder
    };

    let unescaped = percent_decode(raw_val);
    decode_base64_url(&unescaped)
}

fn extract_track_url(track: &EncodingTrack) -> Option<String> {
    if let Some(p) = &track.path {
        let trimmed = p.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    if let Some(p2p) = &track.p2p_path
        && let Some(url) = extract_cdn_url(p2p)
    {
        return Some(url);
    }

    if let Some(p2p_enc) = &track.p2p_path_url_encoding
        && let Some(url) = extract_cdn_url(p2p_enc)
    {
        return Some(url);
    }

    None
}

pub fn extract_best_hls_url(playback_json_str: &Option<String>) -> Result<String> {
    let json_str = playback_json_str
        .as_ref()
        .ok_or_else(|| anyhow!("No livePlaybackJson available"))?;
    let playback: PlaybackJson =
        serde_json::from_str(json_str).context("Failed to parse livePlaybackJson")?;

    let hls_media = playback
        .media
        .iter()
        .find(|m| m.media_id.eq_ignore_ascii_case("HLS"))
        .ok_or_else(|| anyhow!("No HLS media entry found"))?;

    let mut best_1080 = None;
    let mut best_720 = None;
    let mut best_other_video = None;

    for track in &hls_media.encoding_track {
        if let Some(track_url) = extract_track_url(track) {
            let id = &track.encoding_track_id;
            if id.contains("1080") && best_1080.is_none() {
                best_1080 = Some(track_url);
            } else if id.contains("720") && best_720.is_none() {
                best_720 = Some(track_url);
            } else if !id.eq_ignore_ascii_case("audioOnly") && best_other_video.is_none() {
                best_other_video = Some(track_url);
            }
        }
    }

    if let Some(path) = best_1080 {
        return Ok(path);
    }
    if let Some(path) = best_720 {
        return Ok(path);
    }
    if let Some(path) = best_other_video {
        return Ok(path);
    }

    if let Some(p2p) = &hls_media.p2p_path
        && let Some(url) = extract_cdn_url(p2p)
    {
        return Ok(url);
    }
    if let Some(p2p_enc) = &hls_media.p2p_path_url_encoding
        && let Some(url) = extract_cdn_url(p2p_enc)
    {
        return Ok(url);
    }

    Ok(hls_media.path.clone())
}

#[derive(Clone)]
pub struct ChzzkClient {
    client: reqwest::Client,
    base_url: String,
    game_base_url: String,
    chat_ws_url: Option<String>,
    cookie_header: Option<String>,
}

impl ChzzkClient {
    pub fn new(config: &ChzzkConfig) -> Self {
        let cookie_str = if !config.nid_aut.is_empty() && !config.nid_ses.is_empty() {
            Some(format!(
                "NID_AUT={}; NID_SES={}",
                config.nid_aut, config.nid_ses
            ))
        } else {
            None
        };

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        if let Some(val) = cookie_str
            .as_deref()
            .and_then(|c| HeaderValue::from_str(c).ok())
        {
            headers.insert(COOKIE, val);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            client,
            base_url: "https://api.chzzk.naver.com".to_string(),
            game_base_url: "https://comm-api.game.naver.com/nng_main".to_string(),
            chat_ws_url: None,
            cookie_header: cookie_str,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_game_base_url(mut self, game_base_url: impl Into<String>) -> Self {
        self.game_base_url = game_base_url.into();
        self
    }

    pub fn with_chat_ws_url(mut self, chat_ws_url: impl Into<String>) -> Self {
        self.chat_ws_url = Some(chat_ws_url.into());
        self
    }

    pub fn chat_ws_url(&self) -> Option<&str> {
        self.chat_ws_url.as_deref()
    }

    pub fn cookie_header(&self) -> Option<&str> {
        self.cookie_header.as_deref()
    }

    pub async fn get_live_detail(&self, channel_id: &str) -> Result<Option<LiveStreamInfo>> {
        let url = format!(
            "{}/service/v2/channels/{}/live-detail",
            self.base_url, channel_id
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let body: ChzzkResponse<LiveDetailContent> = resp.json().await?;

        if body.code != 200 {
            let msg = body
                .message
                .unwrap_or_else(|| "Unknown API error".to_string());
            bail!("Chzzk API returned error code {}: {}", body.code, msg);
        }

        if let Some(content) = body.content.filter(|c| c.status == "OPEN") {
            let hls_url = extract_best_hls_url(&content.live_playback_json)?;
            return Ok(Some(LiveStreamInfo {
                channel_id: channel_id.to_string(),
                live_id: content.live_id,
                streamer_name: content.channel.channel_name,
                title: content
                    .live_title
                    .unwrap_or_else(|| "Untitled Broadcast".to_string()),
                hls_url,
                chat_channel_id: content.chat_channel_id,
            }));
        }

        Ok(None)
    }

    pub async fn get_chat_access_token(&self, chat_channel_id: &str) -> Result<String> {
        let url = format!(
            "{}/v1/chats/access-token?channelId={}&chatType=STREAMING",
            self.game_base_url, chat_channel_id
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let body: ChzzkResponse<ChatAccessTokenResponse> = resp.json().await?;

        if body.code != 200 {
            let msg = body
                .message
                .unwrap_or_else(|| "Unknown API error".to_string());
            bail!("Chzzk API returned error code {}: {}", body.code, msg);
        }

        let content = body
            .content
            .ok_or_else(|| anyhow!("Chat access token response missing content"))?;
        Ok(content.access_token)
    }
}
