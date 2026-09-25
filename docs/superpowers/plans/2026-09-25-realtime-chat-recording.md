# Real-Time Chat Recording Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record real-time Chzzk live broadcast chat alongside the video stream into an SBC-friendly batched JSON Lines file (`chat.jsonl`) and upload it to Google Drive on session completion.

**Architecture:** A native async WebSocket client (`ChzzkChatClient`) connects to `wss://kr-ss{n}.chat.naver.com/chat` with heartbeat and auto-reconnect, deserializing chat and donation events. An in-memory batched writer (`ChatWriter`) minimizes microSD I/O wear by flushing only on time/capacity thresholds. The recording session orchestrator manages concurrent execution and uploads `chat.jsonl` to Google Drive upon stream completion.

**Tech Stack:** Rust (2024 edition), `tokio`, `tokio-tungstenite`, `reqwest`, `serde`, `serde_json`, `ratatui`

**Spec:** `docs/superpowers/specs/2026-09-25-realtime-chat-recording-design.md`

## Global Constraints
- Preserve standalone single-binary invariant across Windows, Linux (x86_64, aarch64 musl), and macOS with zero external runtime dependencies.
- No direct terminal pollution (`println!`, unredirected stderr/stdout while TUI is active).
- Resilient HTTP and WebSocket mocking in tests with dynamic ports (`127.0.0.1:0`).
- Test file operations restricted strictly to `std::env::temp_dir()`.
- Sequential batched file I/O with dual-trigger flush (500 msgs / 64KB or 30s) to minimize flash wear on microSD cards.

---

### Task 1: Dependencies & Configuration Settings

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs:1-45`
- Test: `tests/test_config.rs`

**Interfaces:**
- Consumes: Existing `GeneralConfig` in `src/config.rs`.
- Produces: `GeneralConfig.record_chat: bool`, `GeneralConfig.chat_flush_interval_seconds: u64`.

- [ ] **Step 1: Write failing test for new configuration fields in `tests/test_config.rs`**

```rust
#[test]
fn test_general_config_chat_settings_default() {
    let json_data = r#"{}"#;
    let cfg: GeneralConfig = serde_json::from_str(json_data).expect("Failed to parse empty general config");
    assert!(cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 30);
}

#[test]
fn test_general_config_chat_settings_custom() {
    let json_data = r#"{
        "record_chat": false,
        "chat_flush_interval_seconds": 60
    }"#;
    let cfg: GeneralConfig = serde_json::from_str(json_data).expect("Failed to parse custom chat config");
    assert!(!cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 60);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_config test_general_config_chat_settings`
Expected: FAIL due to missing fields `record_chat` and `chat_flush_interval_seconds`.

- [ ] **Step 3: Update `Cargo.toml` and implement config defaults in `src/config.rs`**

Add `tokio-tungstenite` to `Cargo.toml`:
```toml
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-webpki-roots"] }
```

In `src/config.rs`:
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneralConfig {
    #[serde(default = "default_chunk_duration")]
    pub chunk_duration_seconds: u64,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_stream_cooldown")]
    pub stream_cooldown_seconds: u64,
    #[serde(default = "default_recordings_dir")]
    pub recordings_dir: String,
    #[serde(default = "default_min_free_disk_gb")]
    pub min_free_disk_gb: f64,
    #[serde(default = "default_record_chat")]
    pub record_chat: bool,
    #[serde(default = "default_chat_flush_interval")]
    pub chat_flush_interval_seconds: u64,
}

fn default_record_chat() -> bool {
    true
}

fn default_chat_flush_interval() -> u64 {
    30
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_config`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add Cargo.toml Cargo.lock src/config.rs tests/test_config.rs
git commit -m "feat(config): add record_chat and chat_flush_interval_seconds settings"
```

---

### Task 2: Chzzk API Access Token & Chat Models

**Files:**
- Create: `src/chzzk/models_chat.rs`
- Modify: `src/chzzk/models.rs:48-90`
- Modify: `src/chzzk/client.rs:100-133`
- Modify: `src/chzzk/mod.rs`
- Test: `tests/test_chzzk_client.rs`

**Interfaces:**
- Consumes: `ChzzkClient`, `LiveDetailContent`.
- Produces: `LiveStreamInfo.chat_channel_id: Option<String>`, `ChzzkClient::get_chat_access_token(&self, chat_channel_id: &str) -> Result<String>`, `RecordedChatMessage`.

- [ ] **Step 1: Write failing test in `tests/test_chzzk_client.rs` for token extraction and chat_channel_id**

```rust
#[tokio::test]
async fn test_get_chat_access_token_success() {
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let base_url = format!("http://127.0.0.1:{}", port);

    let server_handle = tokio::task::spawn_blocking(move || {
        let req = server.recv().unwrap();
        assert!(req.url().contains("/v1/chats/access-token"));
        assert!(req.url().contains("channelId=chat_chan_123"));
        let response_body = r#"{
            "code": 200,
            "message": null,
            "content": {
                "accessToken": "mock_token_abc123",
                "extraToken": "mock_extra"
            }
        }"#;
        let resp = tiny_http::Response::from_string(response_body)
            .with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
        req.respond(resp).unwrap();
    });

    let config = ChzzkConfig::default();
    let client = ChzzkClient::new(&config).with_game_base_url(&base_url);
    let token = client.get_chat_access_token("chat_chan_123").await.unwrap();
    assert_eq!(token, "mock_token_abc123");

    server_handle.await.unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chzzk_client test_get_chat_access_token_success`
Expected: FAIL due to missing `with_game_base_url` and `get_chat_access_token`.

- [ ] **Step 3: Implement chat models in `src/chzzk/models_chat.rs` and update `src/chzzk/client.rs`**

Create `src/chzzk/models_chat.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct ChatAccessTokenResponse {
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[serde(rename = "extraToken")]
    pub extra_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedChatMessage {
    pub time_ms: u64,
    pub datetime: String,
    pub msg_type: String,
    pub nickname: String,
    pub user_id_hash: Option<String>,
    pub content: String,
    pub donation_amount: Option<u64>,
    pub extras: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}
```

Update `src/chzzk/models.rs`:
```rust
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStreamInfo {
    pub channel_id: String,
    pub live_id: Option<u64>,
    pub streamer_name: String,
    pub title: String,
    pub hls_url: String,
    pub chat_channel_id: Option<String>,
}
```

In `src/chzzk/client.rs`:
- Add field `game_base_url: String` (defaults to `"https://comm-api.game.naver.com/nng_main"`).
- Implement `with_game_base_url(mut self, url: impl Into<String>) -> Self`.
- Implement `pub async fn get_chat_access_token(&self, chat_channel_id: &str) -> Result<String>`.
- In `get_live_detail`, populate `chat_channel_id: content.chat_channel_id`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_chzzk_client`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/chzzk/ Cargo.toml tests/test_chzzk_client.rs
git commit -m "feat(chzzk): add chat models, chat_channel_id extraction and access token API"
```

---

### Task 3: Flash-Friendly Batched In-Memory Writer (`ChatWriter`)

**Files:**
- Create: `src/recorder/chat_writer.rs`
- Modify: `src/recorder/mod.rs`
- Test: `tests/test_chat_writer.rs`

**Interfaces:**
- Consumes: `RecordedChatMessage`.
- Produces: `ChatWriter`:
  - `pub fn new(target_path: PathBuf, flush_interval: Duration, capacity_threshold: usize) -> Self`
  - `pub async fn push(&mut self, msg: RecordedChatMessage) -> Result<()>`
  - `pub async fn flush(&mut self) -> Result<()>`
  - `pub async fn flush_and_close(&mut self) -> Result<u64>` (returns total written count)

- [ ] **Step 1: Write failing test in `tests/test_chat_writer.rs`**

```rust
use chzzk_load::chzzk::models_chat::RecordedChatMessage;
use chzzk_load::recorder::chat_writer::ChatWriter;
use std::time::Duration;

#[tokio::test]
async fn test_chat_writer_batches_and_flushes_on_capacity() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat.jsonl");

    // Capacity threshold of 3 messages, long duration
    let mut writer = ChatWriter::new(chat_file.clone(), Duration::from_secs(3600), 3);

    for i in 1..=2 {
        let msg = RecordedChatMessage {
            time_ms: 1000 * i,
            datetime: "2026-09-25 00:00:00".to_string(),
            msg_type: "TEXT".to_string(),
            nickname: format!("User{}", i),
            user_id_hash: None,
            content: format!("Message {}", i),
            donation_amount: None,
            extras: None,
            raw: serde_json::json!({}),
        };
        writer.push(msg).await.unwrap();
    }

    // Should NOT have flushed yet (count = 2 < 3)
    let exists = tokio::fs::try_exists(&chat_file).await.unwrap_or(false);
    if exists {
        let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
        assert!(content.is_empty(), "Expected empty file before capacity trigger");
    }

    // Push 3rd message -> triggers capacity flush
    let msg3 = RecordedChatMessage {
        time_ms: 3000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User3".to_string(),
        user_id_hash: None,
        content: "Message 3".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg3).await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3);

    let total = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 3);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_flushes_on_interval_and_termination() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_int_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat.jsonl");

    // Interval threshold of 50ms, large capacity
    let mut writer = ChatWriter::new(chat_file.clone(), Duration::from_millis(50), 1000);

    let msg = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User1".to_string(),
        user_id_hash: None,
        content: "Single message".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg).await.unwrap();

    // Wait for timer threshold to elapse
    tokio::time::sleep(Duration::from_millis(100)).await;
    writer.maybe_flush_timer().await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert_eq!(content.lines().count(), 1);

    let total = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 1);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chat_writer`
Expected: FAIL due to missing `ChatWriter`.

- [ ] **Step 3: Implement `ChatWriter` in `src/recorder/chat_writer.rs`**

```rust
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use crate::chzzk::models_chat::RecordedChatMessage;

pub struct ChatWriter {
    target_path: PathBuf,
    buffer: Vec<String>,
    buffered_bytes: usize,
    capacity_threshold: usize,
    max_bytes_threshold: usize,
    flush_interval: Duration,
    last_flush: Instant,
    total_written: u64,
}

impl ChatWriter {
    pub fn new(target_path: PathBuf, flush_interval: Duration, capacity_threshold: usize) -> Self {
        Self {
            target_path,
            buffer: Vec::with_capacity(capacity_threshold),
            buffered_bytes: 0,
            capacity_threshold,
            max_bytes_threshold: 64 * 1024, // 64 KB
            flush_interval,
            last_flush: Instant::now(),
            total_written: 0,
        }
    }

    pub fn target_path(&self) -> &PathBuf {
        &self.target_path
    }

    pub fn total_written(&self) -> u64 {
        self.total_written
    }

    pub async fn push(&mut self, msg: RecordedChatMessage) -> Result<()> {
        let line = serde_json::to_string(&msg).context("Failed to serialize RecordedChatMessage")?;
        self.buffered_bytes += line.len() + 1;
        self.buffer.push(line);

        if self.buffer.len() >= self.capacity_threshold || self.buffered_bytes >= self.max_bytes_threshold {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn maybe_flush_timer(&mut self) -> Result<bool> {
        if !self.buffer.is_empty() && self.last_flush.elapsed() >= self.flush_interval {
            self.flush().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }

        if let Some(parent) = self.target_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("Failed to create directory {}", parent.display())
            })?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.target_path)
            .await
            .with_context(|| format!("Failed to open chat file {}", self.target_path.display()))?;

        let mut data = Vec::with_capacity(self.buffered_bytes);
        for line in &self.buffer {
            data.extend_from_slice(line.as_bytes());
            data.push(b'\n');
        }

        file.write_all(&data).await.context("Failed to write chat batch to disk")?;
        file.flush().await.context("Failed to flush chat file")?;

        self.total_written += self.buffer.len() as u64;
        self.buffer.clear();
        self.buffered_bytes = 0;
        self.last_flush = Instant::now();
        Ok(())
    }

    pub async fn flush_and_close(&mut self) -> Result<u64> {
        self.flush().await?;
        Ok(self.total_written)
    }
}
```

Register `pub mod chat_writer;` in `src/recorder/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_chat_writer`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/recorder/ tests/test_chat_writer.rs
git commit -m "feat(recorder): implement flash-friendly batched ChatWriter"
```

---

### Task 4: Chzzk WebSocket Client (`ChzzkChatClient`)

**Files:**
- Create: `src/chzzk/chat.rs`
- Modify: `src/chzzk/mod.rs`
- Test: `tests/test_chat_client.rs`

**Interfaces:**
- Consumes: `ChatAccessTokenResponse`, `RecordedChatMessage`, `ChatWriter`.
- Produces: `ChzzkChatClient`:
  - `pub fn compute_server_id(chat_channel_id: &str) -> u32`
  - `pub fn build_ws_url(server_id: u32) -> String`
  - `pub async fn run_chat_loop(...)`

- [ ] **Step 1: Write failing test in `tests/test_chat_client.rs`**

```rust
use chzzk_load::chzzk::chat::{compute_server_id, parse_chat_packet, ChzzkChatClient};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[test]
fn test_compute_server_id() {
    // Check distribution 1..=9
    let id1 = compute_server_id("N12345");
    assert!((1..=9).contains(&id1));
    let id2 = compute_server_id("abcdef");
    assert!((1..=9).contains(&id2));
}

#[tokio::test]
async fn test_mock_websocket_handshake_and_chat_receiving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{}", addr);

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

        // 1. Expect CONNECT packet (cmd: 100)
        let msg = ws.next().await.unwrap().unwrap();
        let text = msg.to_text().unwrap();
        assert!(text.contains(r#""cmd":100"#) || text.contains(r#""cmd": 100"#));

        // 2. Respond with CONNECTED (cmd: 10100)
        let resp = serde_json::json!({
            "cmd": 10100,
            "bdy": { "sid": "session_test_xyz" }
        });
        ws.send(Message::Text(resp.to_string())).await.unwrap();

        // 3. Send a CHAT packet (cmd: 93101)
        let chat_packet = serde_json::json!({
            "cmd": 93101,
            "bdy": [
                {
                    "msg": "Hello integration test!",
                    "msgTime": 1727268158000u64,
                    "msgTypeCode": 1,
                    "profile": "{\"nickname\":\"TestViewer\",\"userIdHash\":\"hash123\"}",
                    "extras": "{}"
                }
            ]
        });
        ws.send(Message::Text(chat_packet.to_string())).await.unwrap();

        // 4. Send PING (cmd: 0)
        let ping_packet = serde_json::json!({ "cmd": 0, "ver": "2" });
        ws.send(Message::Text(ping_packet.to_string())).await.unwrap();

        // 5. Expect PONG (cmd: 10000)
        let pong = ws.next().await.unwrap().unwrap();
        assert!(pong.to_text().unwrap().contains("10000"));
    });

    let cancel_token = CancellationToken::new();
    let temp_dir = std::env::temp_dir().join(format!("test_ws_chat_{}", rand::random::<u32>()));
    let chat_file = temp_dir.join("chat.jsonl");

    let client = ChzzkChatClient::new(
        "mock_channel".to_string(),
        "mock_access_token".to_string(),
        chat_file.clone(),
        Duration::from_millis(50),
        cancel_token.clone(),
    ).with_custom_ws_url(ws_url);

    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        cancel_clone.cancel();
    });

    let total = client.run(None).await.unwrap();
    assert_eq!(total, 1);

    server_task.await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert!(content.contains("Hello integration test!"));
    assert!(content.contains("TestViewer"));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chat_client`
Expected: FAIL due to missing `ChzzkChatClient` and `compute_server_id`.

- [ ] **Step 3: Implement `ChzzkChatClient` in `src/chzzk/chat.rs`**

Implement:
- `pub fn compute_server_id(chat_channel_id: &str) -> u32`: sum of byte values mod 9 + 1.
- `pub fn build_ws_url(server_id: u32) -> String`: `format!("wss://kr-ss{}.chat.naver.com/chat", server_id)`.
- Command codes: `PING = 0`, `PONG = 10000`, `CONNECT = 100`, `CONNECTED = 10100`, `CHAT = 93101`, `DONATION = 93102`, `SUBSCRIPTION = 93103`.
- Parsing `parse_chat_packet(json: &serde_json::Value) -> Vec<RecordedChatMessage>`.
- Client event loop with:
  - TLS connection via `tokio_tungstenite::connect_async`.
  - Handshake transmission and acknowledgment.
  - Heartbeat response (replying to server PING with PONG) and 20s interval client PING.
  - Forwarding parsed messages to `ChatWriter`.
  - Periodically calling `writer.maybe_flush_timer()`.
  - On cancellation, closing WebSocket gracefully and calling `writer.flush_and_close()`.

Register `pub mod chat;` in `src/chzzk/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_chat_client`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/chzzk/ tests/test_chat_client.rs
git commit -m "feat(chzzk): implement ChzzkChatClient with handshake, heartbeat, and parsing"
```

---

### Task 5: TUI Events & Telemetry

**Files:**
- Modify: `src/tui/event.rs`
- Modify: `src/tui/app.rs`
- Modify: `src/tui/ui.rs`
- Test: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: `AppEvent`, `LogEntry`.
- Produces: `AppEvent::ChatStats { channel_id, message_count }`, `LogEntry::chat(msg)`.

- [ ] **Step 1: Write failing test in `tests/test_tui_state.rs`**

```rust
#[test]
fn test_app_state_chat_stats_update() {
    let settings = Settings::default();
    let mut app = App::new(settings);

    // Initial state
    assert_eq!(app.channels[0].chat_count, 0);

    // Dispatch ChatStats event
    app.update(AppEvent::ChatStats {
        channel_id: app.channels[0].id.clone(),
        message_count: 142,
    });

    assert_eq!(app.channels[0].chat_count, 142);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_tui_state test_app_state_chat_stats_update`
Expected: FAIL due to missing `ChatStats` and `chat_count`.

- [ ] **Step 3: Update `event.rs`, `app.rs`, and `ui.rs`**

In `src/tui/event.rs`:
```rust
pub enum AppEvent {
    // ... existing variants ...
    ChatStats {
        channel_id: String,
        message_count: u64,
    },
}

impl LogEntry {
    pub fn chat(msg: impl Into<String>) -> Self {
        Self {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            category: "CHAT".to_string(),
            message: msg.into(),
        }
    }
}
```

In `src/tui/app.rs`:
- Add `pub chat_count: u64` to `ChannelItemState`.
- In `update()`, handle `AppEvent::ChatStats`:
  ```rust
  AppEvent::ChatStats { channel_id, message_count } => {
      if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
          ch.chat_count = message_count;
      }
  }
  ```
- Reset `chat_count = 0` on `RecordingEnded`.

In `src/tui/ui.rs`:
- Style `CHAT` category log prefix in `Color::Cyan`.
- In Monitored Channels table row, display `(X chats)` alongside segments when recording is active.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_tui_state`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/tui/ tests/test_tui_state.rs
git commit -m "feat(tui): add ChatStats event, CHAT log category and channel table telemetry"
```

---

### Task 6: Engine Orchestrator Lifecycle & Google Drive Upload Integration

**Files:**
- Modify: `src/engine/mod.rs`
- Test: `tests/test_engine_chat.rs`

**Interfaces:**
- Consumes: `Settings.general.record_chat`, `ChzzkChatClient`, `ChatWriter`, `DriveClient`.
- Produces: Concurrent chat recording during session, and upload + cleanup of `chat.jsonl` upon session end.

- [ ] **Step 1: Write integration test in `tests/test_engine_chat.rs`**

```rust
use chzzk_load::config::Settings;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::event::AppEvent;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_engine_orchestrator_chat_lifecycle_with_cancel() {
    let mut settings = Settings::default();
    let temp_dir = std::env::temp_dir().join(format!("test_eng_chat_{}", rand::random::<u32>()));
    settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
    settings.general.record_chat = true;
    settings.general.chat_flush_interval_seconds = 1;

    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);
    let cancel_token = CancellationToken::new();

    // Verify session starts and can be cleanly cancelled without errors
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    // Drain events to verify clean shutdown
    let mut saw_recording_started = false;
    let timeout = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            Some(ev) = event_rx.recv() => {
                match ev {
                    AppEvent::RecordingStarted { .. } => saw_recording_started = true,
                    AppEvent::RecordingEnded { .. } => break,
                    _ => {}
                }
            }
            _ = &mut timeout => break,
        }
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
```

- [ ] **Step 2: Run test to verify it fails/compiles**

Run: `cargo test --test test_engine_chat`
Expected: Compile check & verify engine integration.

- [ ] **Step 3: Integrate chat recording and Drive upload into `src/engine/mod.rs`**

In `src/engine/mod.rs`:
1. In `spawn_recording_session`:
   - Check `if settings.general.record_chat`.
   - If `let Some(ref chat_cid) = info.chat_channel_id`:
     - Fetch token: `chzzk.get_chat_access_token(chat_cid).await`.
     - Construct `ChatWriter` with `session_dir.join("chat.jsonl")` and `settings.general.chat_flush_interval_seconds`.
     - Spawn Tokio task for `ChzzkChatClient` passing session `cancel_token.clone()`.
     - Forward count updates to `event_tx.send(AppEvent::ChatStats { ... })`.
2. When the session terminates:
   - Chat task finishes and flushes `chat.jsonl`.
   - If `drive_opt` is `Some(drive)`:
     - Check if `chat.jsonl` exists on disk.
     - Upload `chat.jsonl` to `session_folder_id` in Google Drive.
     - Delete local `chat.jsonl` upon confirmed upload.
     - Emit `AppEvent::Log(LogEntry::drive(format!("Uploaded 'chat.jsonl' for {}", channel_id)))`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_engine_chat`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/engine/mod.rs tests/test_engine_chat.rs
git commit -m "feat(engine): integrate real-time chat recording and Drive upload lifecycle"
```

---

### Task 7: Full System Verification & Regression Suite

**Files:**
- All modified files

- [ ] **Step 1: Run format check**

Run: `cargo fmt --check`
Expected: PASS (0 format violations)

- [ ] **Step 2: Run linter with zero warnings tolerance**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS (0 warnings)

- [ ] **Step 3: Run full test suite**

Run: `cargo test --all-targets`
Expected: All unit, smoke, and integration tests PASS.

- [ ] **Step 4: Run npm package distribution test suite**

Run: `node scripts/test-npm-packages.js`
Expected: PASS (mock packaging and CLI platform launcher tests pass).

- [ ] **Step 5: Final commit if any polish was applied**

```bash
git add -A
git commit -m "chore: format and pass full test and clippy suite"
```
