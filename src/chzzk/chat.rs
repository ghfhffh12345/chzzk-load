use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::chzzk::models_chat::RecordedChatMessage;
use crate::recorder::chat_writer::ChatWriter;

pub const CMD_PING: u32 = 0;
pub const CMD_PONG: u32 = 10000;
pub const CMD_CONNECT: u32 = 100;
pub const CMD_CONNECTED: u32 = 10100;
pub const CMD_CHAT: u32 = 93101;
pub const CMD_DONATION: u32 = 93102;
pub const CMD_SUBSCRIPTION: u32 = 93103;
pub const CMD_BLIND: u32 = 94008;

pub const PING_PAYLOAD: &str = r#"{"cmd":0,"ver":"2"}"#;
pub const PONG_PAYLOAD: &str = r#"{"cmd":10000,"ver":"2"}"#;

/// Computes the Chzzk WebSocket server ID (1..=9) from the chat channel ID.
///
/// Uses the sum of byte values mod 9 + 1.
pub fn compute_server_id(chat_channel_id: &str) -> u32 {
    let sum: u32 = chat_channel_id.as_bytes().iter().map(|&b| b as u32).sum();
    (sum % 9) + 1
}

/// Constructs the secure WebSocket endpoint URL for the assigned chat server ID.
pub fn build_ws_url(server_id: u32) -> String {
    format!("wss://kr-ss{server_id}.chat.naver.com/chat")
}

/// Parses a Chzzk chat packet into a list of `RecordedChatMessage` entries.
///
/// Extracts timestamps, user nicknames, badges, donation amounts, and raw payloads.
pub fn parse_chat_packet(json: &serde_json::Value) -> Vec<RecordedChatMessage> {
    parse_chat_packet_owned(json.clone())
}

/// Parses an owned Chzzk chat packet into a list of `RecordedChatMessage` entries,
/// moving raw message JSON directly into each `RecordedChatMessage` to eliminate heap cloning.
pub fn parse_chat_packet_owned(mut json: serde_json::Value) -> Vec<RecordedChatMessage> {
    let cmd = extract_cmd(&json).unwrap_or(0);
    let items: Vec<serde_json::Value> = match json.get_mut("bdy").map(serde_json::Value::take) {
        Some(serde_json::Value::Array(arr)) => arr,
        _ => {
            if let serde_json::Value::Array(arr) = json {
                arr
            } else if json.is_object()
                && (json.get("msg").is_some() || json.get("msgTime").is_some())
            {
                vec![json]
            } else {
                Vec::new()
            }
        }
    };

    let mut results = Vec::with_capacity(items.len());
    for mut item in items {
        let time_ms = item
            .get("msgTime")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);

        let datetime = chrono::DateTime::from_timestamp_millis(time_ms as i64)
            .map(|dt| {
                chrono::DateTime::<chrono::Local>::from(dt)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            })
            .unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());

        let (mut nickname, mut user_id_hash) = if let Some(profile_val) = item.get("profile") {
            let obj: Option<serde_json::Value> = if let Some(s) = profile_val.as_str() {
                serde_json::from_str(s).ok()
            } else if profile_val.is_object() {
                Some(profile_val.clone())
            } else {
                None
            };
            if let Some(obj) = obj {
                let nick = obj
                    .get("nickname")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let hash = obj
                    .get("userIdHash")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                (nick, hash)
            } else {
                (String::new(), None)
            }
        } else {
            (String::new(), None)
        };

        if nickname.is_empty()
            && let Some(nick) = item
                .get("nickname")
                .or_else(|| item.get("senderNickname"))
                .and_then(|v| v.as_str())
        {
            nickname = nick.to_string();
        }
        if user_id_hash.is_none()
            && let Some(hash) = item.get("userIdHash").and_then(|v| v.as_str())
        {
            user_id_hash = Some(hash.to_string());
        }

        let extras = match item.get_mut("extras").map(serde_json::Value::take) {
            Some(serde_json::Value::String(s)) => {
                serde_json::from_str::<serde_json::Value>(&s).ok()
            }
            Some(val @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => Some(val),
            _ => None,
        };

        let donation_amount = item
            .get("payAmount")
            .or_else(|| item.get("donationAmount"))
            .and_then(|v| v.as_u64())
            .or_else(|| {
                extras.as_ref().and_then(|ext| {
                    ext.get("payAmount")
                        .or_else(|| ext.get("donationAmount"))
                        .and_then(|v| v.as_u64())
                })
            });

        let msg_type_code = item.get("msgTypeCode").and_then(|v| v.as_u64());
        let msg_type = match msg_type_code {
            Some(1) => "TEXT".to_string(),
            Some(10) => "DONATION".to_string(),
            Some(11) => "SUBSCRIPTION".to_string(),
            Some(30) => "SYSTEM_MESSAGE".to_string(),
            Some(other) => format!("TYPE_{other}"),
            None => match cmd {
                CMD_DONATION => "DONATION".to_string(),
                CMD_SUBSCRIPTION => "SUBSCRIPTION".to_string(),
                _ => "TEXT".to_string(),
            },
        };

        let content = item
            .get("msg")
            .or_else(|| item.get("content"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        results.push(RecordedChatMessage {
            time_ms,
            datetime,
            msg_type,
            nickname,
            user_id_hash,
            content,
            donation_amount,
            extras,
            raw: item,
        });
    }

    results
}

fn extract_cmd(val: &serde_json::Value) -> Option<u32> {
    if let Some(cmd) = val.get("cmd") {
        if let Some(n) = cmd.as_u64() {
            return Some(n as u32);
        }
        if let Some(s) = cmd.as_str()
            && let Ok(n) = s.parse::<u32>()
        {
            return Some(n);
        }
    }
    None
}

/// Asynchronous client for recording live chat streams from Chzzk's WebSocket infrastructure.
pub struct ChzzkChatClient {
    chat_channel_id: String,
    access_token: String,
    target_path: PathBuf,
    flush_interval: Duration,
    cancel_token: CancellationToken,
    custom_ws_url: Option<String>,
}

impl ChzzkChatClient {
    /// Creates a new `ChzzkChatClient`.
    pub fn new(
        chat_channel_id: impl Into<String>,
        access_token: impl Into<String>,
        target_path: impl Into<PathBuf>,
        flush_interval: Duration,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            chat_channel_id: chat_channel_id.into(),
            access_token: access_token.into(),
            target_path: target_path.into(),
            flush_interval,
            cancel_token,
            custom_ws_url: None,
        }
    }

    /// Overrides the WebSocket server URL (primarily used in tests).
    #[must_use]
    pub fn with_custom_ws_url(mut self, url: impl Into<String>) -> Self {
        self.custom_ws_url = Some(url.into());
        self
    }

    /// Returns a reference to the chat channel ID.
    pub fn chat_channel_id(&self) -> &str {
        &self.chat_channel_id
    }

    /// Returns a reference to the access token.
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Returns a reference to the target output file path.
    pub fn target_path(&self) -> &Path {
        &self.target_path
    }

    /// Returns the flush interval duration.
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Connects to the chat WebSocket server, handles handshake and heartbeats,
    /// buffers incoming chat messages, and terminates upon cancellation.
    ///
    /// Optionally sends the current captured message count to `on_stats`.
    /// Returns the total number of messages written to disk.
    pub async fn run(&self, on_stats: Option<tokio::sync::mpsc::Sender<u64>>) -> Result<u64> {
        let mut writer = ChatWriter::new(self.target_path.clone(), self.flush_interval, 500);

        let ws_url = match &self.custom_ws_url {
            Some(url) => url.clone(),
            None => {
                let server_id = compute_server_id(&self.chat_channel_id);
                build_ws_url(server_id)
            }
        };

        let mut reconnect_attempt: u32 = 0;

        'outer: while !self.cancel_token.is_cancelled() {
            let connect_result = tokio::select! {
                _ = self.cancel_token.cancelled() => break 'outer,
                res = tokio_tungstenite::connect_async(&ws_url) => res,
            };
            let (ws_stream, _) = match connect_result {
                Ok(stream) => stream,
                Err(_err) => {
                    if self.cancel_token.is_cancelled() {
                        break 'outer;
                    }
                    reconnect_attempt += 1;
                    let backoff =
                        Duration::from_millis(std::cmp::min(1000 * reconnect_attempt as u64, 5000));
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => break 'outer,
                        _ = tokio::time::sleep(backoff) => continue 'outer,
                    }
                }
            };

            let (mut ws_sink, mut ws_reader) = ws_stream.split();

            // Handshake: CONNECT (cmd: 100)
            let connect_packet = serde_json::json!({
                "cmd": CMD_CONNECT,
                "ver": "2",
                "svcid": "game",
                "cid": self.chat_channel_id,
                "tid": 1,
                "bdy": {
                    "accTkn": self.access_token,
                    "auth": "READ",
                    "devType": 2001,
                    "uid": serde_json::Value::Null,
                }
            });

            let send_res = tokio::select! {
                _ = self.cancel_token.cancelled() => break 'outer,
                res = ws_sink.send(Message::Text(connect_packet.to_string().into())) => res,
            };

            if send_res.is_err() {
                if self.cancel_token.is_cancelled() {
                    break 'outer;
                }
                reconnect_attempt += 1;
                let backoff =
                    Duration::from_millis(std::cmp::min(1000 * reconnect_attempt as u64, 5000));
                tokio::select! {
                    _ = self.cancel_token.cancelled() => break 'outer,
                    _ = tokio::time::sleep(backoff) => continue 'outer,
                }
            }

            // Await CONNECTED (cmd: 10100) with 10-second timeout
            let handshake_res = tokio::time::timeout(Duration::from_secs(10), async {
                while !self.cancel_token.is_cancelled() {
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            let _ = ws_sink.send(Message::Close(None)).await;
                            return false;
                        }
                        msg = ws_reader.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                                        && extract_cmd(&val) == Some(CMD_CONNECTED)
                                    {
                                        return true;
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = ws_sink.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                    return false;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                false
            })
            .await;

            let connected = matches!(handshake_res, Ok(true));

            if self.cancel_token.is_cancelled() {
                break 'outer;
            }

            if !connected {
                reconnect_attempt += 1;
                let backoff =
                    Duration::from_millis(std::cmp::min(1000 * reconnect_attempt as u64, 5000));
                tokio::select! {
                    _ = self.cancel_token.cancelled() => break 'outer,
                    _ = tokio::time::sleep(backoff) => continue 'outer,
                }
            }

            // Handshake confirmed: reset reconnect attempt counter
            reconnect_attempt = 0;

            let mut ping_interval = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(20),
                Duration::from_secs(20),
            );
            let mut flush_timer = tokio::time::interval(Duration::from_millis(200));

            'session: loop {
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        let _ = tokio::time::timeout(
                            Duration::from_millis(500),
                            ws_sink.send(Message::Close(None)),
                        )
                        .await;
                        break 'outer;
                    }
                    _ = ping_interval.tick() => {
                        if ws_sink.send(Message::Text(PING_PAYLOAD.into())).await.is_err() {
                            break 'session;
                        }
                    }
                    _ = flush_timer.tick() => {
                        let _ = writer.maybe_flush_timer().await;
                    }
                    msg = ws_reader.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                                    let cmd = extract_cmd(&val).unwrap_or(0);
                                    match cmd {
                                        CMD_PING => {
                                            if ws_sink.send(Message::Text(PONG_PAYLOAD.into())).await.is_err() {
                                                break 'session;
                                            }
                                        }
                                        CMD_CHAT | CMD_DONATION | CMD_SUBSCRIPTION => {
                                            let msgs = parse_chat_packet_owned(val);
                                            if !msgs.is_empty() {
                                                for m in msgs {
                                                    writer.push(m).await.context("Failed to write chat message")?;
                                                }
                                                if let Some(ref tx) = on_stats {
                                                    let count = writer.total_written() + writer.buffered_count() as u64;
                                                    let _ = tx.try_send(count);
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                let _ = ws_sink.send(Message::Pong(data)).await;
                            }
                            Some(Ok(Message::Close(_))) => {
                                break 'session;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(_)) | None => {
                                break 'session;
                            }
                        }
                    }
                }
            }

            if self.cancel_token.is_cancelled() {
                break 'outer;
            }

            reconnect_attempt += 1;
            let backoff =
                Duration::from_millis(std::cmp::min(1000 * reconnect_attempt as u64, 5000));
            tokio::select! {
                _ = self.cancel_token.cancelled() => break 'outer,
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        let total = writer.flush_and_close().await?;
        if let Some(ref tx) = on_stats {
            let _ = tx.try_send(total);
        }
        Ok(total)
    }
}
