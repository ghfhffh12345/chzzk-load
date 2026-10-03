use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, USER_AGENT};

use crate::chzzk::models::{
    ChzzkResponse, LiveDetail, LiveDetailContent, LivePollingStatus, LiveStreamInfo, PlaybackJson,
    PlaybackMeta,
};
use crate::chzzk::models_chat::ChatAccessTokenResponse;
use crate::chzzk::models_metadata::{CategoryType, StreamAccessTier, StreamMetadataState};
use crate::chzzk::source::{BoxFuture, LiveStreamSource};
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

pub fn is_stream_auth_required(content: &LiveDetailContent, hls_url: &str) -> bool {
    // 1. Check PlaybackJson (meta and raw JSON containing aes_key / encryption)
    if let Some(json_str) = &content.live_playback_json {
        if json_str.contains("aes_key") || json_str.contains("/encryption/") {
            return true;
        }
        if let Ok(playback) = serde_json::from_str::<PlaybackJson>(json_str)
            && let Some(meta) = playback.meta
        {
            if meta.paid_live.unwrap_or(false) {
                return true;
            }
            if let Some(auth_type) = &meta.playback_auth_type
                && !auth_type.eq_ignore_ascii_case("NONE")
            {
                return true;
            }
        }
    }

    // 2. Check track URL query parameter playback_auth_type or aes_key
    let lower_url = hls_url.to_ascii_lowercase();
    if lower_url.contains("aes_key")
        || lower_url.contains("/encryption/")
        || (lower_url.contains("playback_auth_type=")
            && !lower_url.contains("playback_auth_type=none"))
    {
        return true;
    }

    // 3. Check content fields (paidProduct, membershipBenefitType, watchPartyPaidProductId)
    if content.paid_product.is_some() || content.watch_party_paid_product_id.is_some() {
        return true;
    }
    if let Some(membership) = &content.membership_benefit_type
        && !membership.eq_ignore_ascii_case("NONE")
    {
        return true;
    }

    false
}

pub fn resolve_access_tier(
    content: &LiveDetailContent,
    playback_meta: Option<&PlaybackMeta>,
) -> StreamAccessTier {
    // 1. One-off Paid Product / Pay-Per-View Ticket
    if content.paid_product.is_some()
        || content.watch_party_paid_product_id.is_some()
        || playback_meta.and_then(|m| m.paid_live).unwrap_or(false)
    {
        return StreamAccessTier::PayPerView;
    }

    // 2. Channel Subscriber-Only (Member-only) or NaverPlus
    if let Some(membership) = &content.membership_benefit_type {
        if membership.eq_ignore_ascii_case("MEMBER_ONLY")
            || membership.eq_ignore_ascii_case("CHANNEL_SUBSCRIPTION")
        {
            return StreamAccessTier::ChannelSubscription;
        }
        if membership.eq_ignore_ascii_case("NAVER_PLUS") {
            return StreamAccessTier::NaverPlus;
        }
    }

    // 3. Platform Cheat Key Pass
    if let Some(meta) = playback_meta
        && let Some(auth_type) = &meta.playback_auth_type
        && auth_type.eq_ignore_ascii_case("CHZZK_CHEAT_KEY")
    {
        return StreamAccessTier::CheatKey;
    }

    // 4. 19+ Age Gating
    if content.adult.unwrap_or(false) {
        return StreamAccessTier::AdultOnly;
    }

    // 5. Default Public Access
    StreamAccessTier::Public
}

pub fn is_polling_status_restricted(content: &LiveDetailContent) -> bool {
    if let Some(polling_str) = &content.live_polling_status_json
        && let Ok(polling) = serde_json::from_str::<LivePollingStatus>(polling_str)
        && let Some(status) = polling.playable_status
        && !status.eq_ignore_ascii_case("PLAYABLE")
    {
        return true;
    }
    false
}

pub fn extract_hls_key_uri(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if (trimmed.starts_with("#EXT-X-KEY:") || trimmed.starts_with("#EXT-X-SESSION-KEY:"))
            && !trimmed.contains("METHOD=NONE")
            && let Some(uri_idx) = trimmed.find("URI=\"")
        {
            let rest = &trimmed[uri_idx + 5..];
            if let Some(end_quote) = rest.find('"') {
                let uri = &rest[..end_quote];
                if !uri.is_empty() {
                    return Some(uri.to_string());
                }
            }
        }
    }
    None
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

    pub async fn check_aes_key_access(&self, live_id: u64) -> Result<bool> {
        let url = format!(
            "{}/service/v1/encryption/lives/{live_id}/aes_key",
            self.base_url
        );
        let resp = self.client.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::FORBIDDEN
            || resp.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            Ok(false)
        } else if resp.status().is_success() {
            Ok(true)
        } else if resp.status().is_client_error() {
            Ok(false)
        } else {
            bail!(
                "Unexpected HTTP status {} from aes_key endpoint",
                resp.status()
            );
        }
    }

    pub async fn get_live_detail(&self, channel_id: &str) -> Result<LiveDetail> {
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

        let streamer_name = body
            .content
            .as_ref()
            .map(|c| c.channel.channel_name.clone());

        if let Some(content) = body.content.filter(|c| c.status == "OPEN") {
            let live_id = content.live_id;
            let streamer_name = content.channel.channel_name.clone();
            let title = content
                .live_title
                .clone()
                .unwrap_or_else(|| "Untitled Broadcast".to_string());
            let chat_channel_id = content.chat_channel_id.clone();
            let adult = content.adult.unwrap_or(false);

            if is_polling_status_restricted(&content) {
                return Ok(LiveDetail::Restricted {
                    channel_id: channel_id.to_string(),
                    live_id,
                    streamer_name,
                    title,
                    chat_channel_id,
                    adult,
                });
            }

            let hls_url = match extract_best_hls_url(&content.live_playback_json) {
                Ok(url) => url,
                Err(_) => {
                    return Ok(LiveDetail::Restricted {
                        channel_id: channel_id.to_string(),
                        live_id,
                        streamer_name,
                        title,
                        chat_channel_id,
                        adult,
                    });
                }
            };

            let auth_required = is_stream_auth_required(&content, &hls_url);
            if auth_required {
                if self.cookie_header.is_none() {
                    return Ok(LiveDetail::Restricted {
                        channel_id: channel_id.to_string(),
                        live_id,
                        streamer_name,
                        title,
                        chat_channel_id,
                        adult,
                    });
                }

                if let Some(id) = live_id {
                    match self.check_aes_key_access(id).await {
                        Ok(true) => {}
                        Ok(false) => {
                            return Ok(LiveDetail::Restricted {
                                channel_id: channel_id.to_string(),
                                live_id,
                                streamer_name,
                                title,
                                chat_channel_id,
                                adult,
                            });
                        }
                        Err(_e) => {}
                    }
                }
            }

            let playback_meta = content
                .live_playback_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<PlaybackJson>(json).ok()?.meta);

            let category_type = content.category_type.as_deref().map(|ct| match ct {
                "GAME" => CategoryType::Game,
                "SPORTS" => CategoryType::Sports,
                "ETC" => CategoryType::Etc,
                "TALK" => CategoryType::Talk,
                _ => CategoryType::Unknown,
            });

            let metadata = StreamMetadataState {
                live_id,
                open_date: content.open_date.clone(),
                close_date: content.close_date.clone(),
                channel_id: channel_id.to_string(),
                channel_name: streamer_name.clone(),
                live_title: title.clone(),
                category_type,
                live_category: content.live_category.clone(),
                live_category_value: content.live_category_value.clone(),
                tags: content.tags.clone(),
                access_tier: resolve_access_tier(&content, playback_meta.as_ref()),
                is_kr_only: content.kr_only_viewing.unwrap_or(false),
                is_chat_active: content.chat_active.unwrap_or(true),
                is_watch_party: content.watch_party_no.is_some()
                    || content.watch_party_tag.is_some(),
                paid_promotion: content.paid_promotion.unwrap_or(false),
                drops_campaign_no: content.drops_campaign_no.clone(),
            };

            Ok(LiveDetail::Open(LiveStreamInfo {
                channel_id: channel_id.to_string(),
                live_id,
                streamer_name,
                title,
                hls_url,
                chat_channel_id,
                metadata,
            }))
        } else {
            Ok(LiveDetail::Close { streamer_name })
        }
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

impl LiveStreamSource for ChzzkClient {
    fn get_live_detail<'a>(
        &'a self,
        channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<LiveDetail>> {
        Box::pin(self.get_live_detail(channel_id))
    }

    fn get_chat_access_token<'a>(
        &'a self,
        chat_channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<String>> {
        Box::pin(self.get_chat_access_token(chat_channel_id))
    }

    fn chat_ws_url(&self) -> Option<&str> {
        self.chat_ws_url()
    }
}
