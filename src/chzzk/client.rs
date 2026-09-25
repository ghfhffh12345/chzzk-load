use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, USER_AGENT};

use crate::chzzk::models::{ChzzkResponse, LiveDetailContent, LiveStreamInfo, PlaybackJson};
use crate::chzzk::models_chat::ChatAccessTokenResponse;
use crate::config::ChzzkConfig;

const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

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
        if let Some(path) = &track.path {
            if track.encoding_track_id.contains("1080") && best_1080.is_none() {
                best_1080 = Some(path.clone());
            } else if track.encoding_track_id.contains("720") && best_720.is_none() {
                best_720 = Some(path.clone());
            } else if !track.encoding_track_id.eq_ignore_ascii_case("audioOnly")
                && best_other_video.is_none()
            {
                best_other_video = Some(path.clone());
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

    Ok(hls_media.path.clone())
}

#[derive(Clone)]
pub struct ChzzkClient {
    client: reqwest::Client,
    base_url: String,
    game_base_url: String,
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
