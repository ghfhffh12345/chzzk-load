# chzzk-load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `chzzk-load`, a standalone Rust application with a modern Ratatui TUI that monitors Chzzk live streams, losslessly segments broadcasts into MPEG-TS chunks on the fly via FFmpeg, stream-uploads completed chunks to Google Drive with resumable retry, and deletes local chunks immediately upon confirmation to maintain a strictly bounded disk footprint.

**Architecture:** A decoupled, asynchronous Tokio application where the recording engine (Chzzk poller, FFmpeg supervisor, N+1 chunk watcher) and the upload engine (OAuth2 token provider, Drive resumable uploader, deletion worker) communicate with the Ratatui TUI via a centralized asynchronous MPSC event channel.

**Tech Stack:** Rust 2024, Tokio (multi-threaded), Ratatui (v0.29), Crossterm (v0.28), Reqwest, Serde/Serde_json, Clap, Google Drive v3 REST API.

**Spec:** [`docs/superpowers/specs/2026-09-20-chzzk-load-design.md`](file:///C:/Users/official/Documents/Code/chzzk-load/docs/superpowers/specs/2026-09-20-chzzk-load-design.md)

## Global Constraints
- Target binary: Standalone executable `chzzk-load.exe` (Windows x86_64), runnable without `cargo`.
- Configuration: Dedicated `settings.json` file loaded from the executable directory or CLI `--config <path>`.
- Container format: MPEG-TS (`.ts`) chunks with FFmpeg stream-copy (`-c copy`) and zero CPU re-encoding.
- Disk usage: Bounded to maximum 1–2 segments on disk simultaneously; local file is immediately deleted upon HTTP 200/201 upload confirmation.
- Safe chunk boundary: Chunk $N$ is only enqueued for upload after chunk $N+1$ exists and has file size $> 0$.

---

### Task 1: Project Scaffolding, Cargo Dependencies, and Portable Path Resolver

**Files:**
- Create: `Cargo.toml`
- Create: `src/app_path.rs`
- Create: `tests/test_app_path.rs`
- Create: `src/lib.rs`

**Interfaces:**
- Produces: `app_path::resolve_path(rel_or_abs: &Path) -> PathBuf`
- Produces: `app_path::get_exe_dir() -> PathBuf`

- [ ] **Step 1: Write Cargo.toml with dependencies**

```toml
[package]
name = "chzzk-load"
version = "0.1.0"
edition = "2024"

[dependencies]
tokio = { version = "1.43", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
ratatui = "0.29"
crossterm = { version = "0.28", features = ["event-stream"] }
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4.5", features = ["derive"] }
anyhow = "1.0"
thiserror = "2.0"
rand = "0.8"
sha2 = "0.10"
base64 = "0.22"
tiny_http = "0.12"
url = "2.5"

[lib]
name = "chzzk_load"
path = "src/lib.rs"

[[bin]]
name = "chzzk-load"
path = "src/main.rs"
```

- [ ] **Step 2: Write failing test for portable path resolution**

Create `tests/test_app_path.rs`:
```rust
use std::path::{Path, PathBuf};
use chzzk_load::app_path::{resolve_path, get_exe_dir};

#[test]
fn test_resolve_relative_path() {
    let exe_dir = get_exe_dir();
    let rel = Path::new("settings.json");
    let resolved = resolve_path(rel);
    assert_eq!(resolved, exe_dir.join("settings.json"));
}

#[test]
fn test_resolve_absolute_path() {
    #[cfg(windows)]
    let abs = PathBuf::from("C:\\custom\\settings.json");
    #[cfg(not(windows))]
    let abs = PathBuf::from("/custom/settings.json");

    let resolved = resolve_path(&abs);
    assert_eq!(resolved, abs);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --test test_app_path`
Expected: FAIL with compilation error (module `app_path` not found).

- [ ] **Step 4: Implement minimal `app_path` and `src/lib.rs`**

Create `src/app_path.rs`:
```rust
use std::path::{Path, PathBuf};

pub fn get_exe_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

pub fn resolve_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        get_exe_dir().join(path)
    }
}
```

Create `src/lib.rs`:
```rust
pub mod app_path;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --test test_app_path`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/lib.rs src/app_path.rs tests/test_app_path.rs
git commit -m "feat: initialize Cargo project and portable path resolver"
```

---

### Task 2: Configuration Loader (`settings.json`) & CLI Arguments

**Files:**
- Create: `src/config.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_config.rs`

**Interfaces:**
- Consumes: `app_path::resolve_path`
- Produces: `config::Settings`, `config::GeneralConfig`, `config::GoogleDriveConfig`, `config::ChzzkConfig`, `config::ChannelConfig`
- Produces: `Settings::load_or_create_default(path: &Path) -> anyhow::Result<Settings>`

- [ ] **Step 1: Write failing test for `Settings` serialization and default creation**

Create `tests/test_config.rs`:
```rust
use std::path::PathBuf;
use chzzk_load::config::Settings;

#[test]
fn test_default_settings_and_serialization() {
    let settings = Settings::default();
    assert_eq!(settings.general.chunk_duration_seconds, 600);
    assert_eq!(settings.general.poll_interval_seconds, 20);
    assert_eq!(settings.google_drive.root_folder_name, "Chzzk_Recordings");
    assert_eq!(settings.channels.len(), 1);

    let json_str = serde_json::to_string_pretty(&settings).expect("Serialize to json");
    let deserialized: Settings = serde_json::from_str(&json_str).expect("Deserialize from json");
    assert_eq!(deserialized.general.chunk_duration_seconds, 600);
}

#[test]
fn test_load_or_create_creates_file_if_missing() {
    let temp_dir = std::env::temp_dir().join(format!("chzzk_test_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("settings.json");

    assert!(!config_path.exists());
    let settings = Settings::load_or_create_default(&config_path).expect("Create default");
    assert!(config_path.exists());
    assert_eq!(settings.general.chunk_duration_seconds, 600);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_config`
Expected: FAIL with compilation error (`config` module missing).

- [ ] **Step 3: Implement `src/config.rs`**

Create `src/config.rs`:
```rust
use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};
use anyhow::Context;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneralConfig {
    #[serde(default = "default_chunk_duration")]
    pub chunk_duration_seconds: u64,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_recordings_dir")]
    pub recordings_dir: String,
    #[serde(default = "default_min_free_disk_gb")]
    pub min_free_disk_gb: f64,
}

fn default_chunk_duration() -> u64 { 600 }
fn default_poll_interval() -> u64 { 20 }
fn default_recordings_dir() -> String { "recordings".to_string() }
fn default_min_free_disk_gb() -> f64 { 2.0 }

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            chunk_duration_seconds: default_chunk_duration(),
            poll_interval_seconds: default_poll_interval(),
            recordings_dir: default_recordings_dir(),
            min_free_disk_gb: default_min_free_disk_gb(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GoogleDriveConfig {
    #[serde(default = "default_credentials_path")]
    pub credentials_path: String,
    #[serde(default = "default_token_path")]
    pub token_path: String,
    #[serde(default = "default_root_folder")]
    pub root_folder_name: String,
}

fn default_credentials_path() -> String { "credentials.json".to_string() }
fn default_token_path() -> String { "token.json".to_string() }
fn default_root_folder() -> String { "Chzzk_Recordings".to_string() }

impl Default for GoogleDriveConfig {
    fn default() -> Self {
        Self {
            credentials_path: default_credentials_path(),
            token_path: default_token_path(),
            root_folder_name: default_root_folder(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ChzzkConfig {
    #[serde(default)]
    pub nid_aut: String,
    #[serde(default)]
    pub nid_ses: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelConfig {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub google_drive: GoogleDriveConfig,
    #[serde(default)]
    pub chzzk: ChzzkConfig,
    #[serde(default = "default_channels")]
    pub channels: Vec<ChannelConfig>,
}

fn default_channels() -> Vec<ChannelConfig> {
    vec![ChannelConfig {
        id: "4c3b44869c9b1399723ec28ec236f736".to_string(),
        name: "SampleStreamer".to_string(),
    }]
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            general: GeneralConfig::default(),
            google_drive: GoogleDriveConfig::default(),
            chzzk: ChzzkConfig::default(),
            channels: default_channels(),
        }
    }
}

impl Settings {
    pub fn load_or_create_default(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read settings from {}", path.display()))?;
            let settings: Settings = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse JSON in {}", path.display()))?;
            Ok(settings)
        } else {
            let settings = Settings::default();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let content = serde_json::to_string_pretty(&settings)?;
            fs::write(path, content)
                .with_context(|| format!("Failed to write default settings to {}", path.display()))?;
            Ok(settings)
        }
    }
}
```

Update `src/lib.rs` to export `pub mod config;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_config`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/config.rs src/lib.rs tests/test_config.rs
git commit -m "feat: implement settings serialization, default generation and loader"
```

---

### Task 3: Chzzk API Client & HLS Stream Resolver

**Files:**
- Create: `src/chzzk/mod.rs`
- Create: `src/chzzk/models.rs`
- Create: `src/chzzk/client.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_chzzk_client.rs`

**Interfaces:**
- Consumes: `config::ChzzkConfig`
- Produces: `chzzk::models::LiveDetailContent`, `chzzk::models::LiveStatus`
- Produces: `chzzk::client::ChzzkClient::new(config: ChzzkConfig) -> ChzzkClient`
- Produces: `chzzk::client::ChzzkClient::get_live_detail(&self, channel_id: &str) -> anyhow::Result<Option<LiveStreamInfo>>`

- [ ] **Step 1: Write failing test for live status parsing and HLS URL selection**

Create `tests/test_chzzk_client.rs`:
```rust
use chzzk_load::chzzk::models::{ChzzkResponse, LiveDetailContent};
use chzzk_load::chzzk::client::extract_best_hls_url;

#[test]
fn test_parse_live_detail_and_extract_hls() {
    let mock_json = r#"{
        "code": 200,
        "message": null,
        "content": {
            "status": "OPEN",
            "liveTitle": "Stream Title",
            "channel": {
                "channelId": "4c3b44869c9b1399723ec28ec236f736",
                "channelName": "TesterStreamer"
            },
            "livePlaybackJson": "{\"media\":[{\"mediaId\":\"HLS\",\"path\":\"https://live.chzzk.naver.com/hls/master.m3u8\",\"encodingTrack\":[{\"encodingTrackId\":\"720p\",\"path\":\"https://live.chzzk.naver.com/hls/720p.m3u8\"},{\"encodingTrackId\":\"1080p\",\"path\":\"https://live.chzzk.naver.com/hls/1080p.m3u8\"}]}]}"
        }
    }"#;

    let response: ChzzkResponse<LiveDetailContent> = serde_json::from_str(mock_json).unwrap();
    let content = response.content.expect("content should be present");
    assert_eq!(content.status, "OPEN");
    assert_eq!(content.channel.channel_name, "TesterStreamer");

    let hls_url = extract_best_hls_url(&content.live_playback_json).expect("valid hls url");
    assert_eq!(hls_url, "https://live.chzzk.naver.com/hls/1080p.m3u8");
}

#[test]
fn test_parse_offline_channel() {
    let mock_json = r#"{
        "code": 200,
        "message": null,
        "content": {
            "status": "CLOSE",
            "liveTitle": null,
            "channel": {
                "channelId": "4c3b44869c9b1399723ec28ec236f736",
                "channelName": "OfflineStreamer"
            },
            "livePlaybackJson": null
        }
    }"#;

    let response: ChzzkResponse<LiveDetailContent> = serde_json::from_str(mock_json).unwrap();
    let content = response.content.unwrap();
    assert_eq!(content.status, "CLOSE");
    assert!(content.live_playback_json.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chzzk_client`
Expected: FAIL with compilation error (`chzzk` module missing).

- [ ] **Step 3: Implement `src/chzzk/models.rs`, `src/chzzk/client.rs`, `src/chzzk/mod.rs`**

Create `src/chzzk/models.rs`:
```rust
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveDetailContent {
    pub status: String,
    pub live_title: Option<String>,
    pub channel: ChannelInfo,
    pub live_playback_json: Option<String>,
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
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct LiveStreamInfo {
    pub channel_id: String,
    pub streamer_name: String,
    pub title: String,
    pub hls_url: String,
}
```

Create `src/chzzk/client.rs`:
```rust
use reqwest::header::{HeaderMap, HeaderValue, COOKIE, USER_AGENT};
use anyhow::{anyhow, Context, Result};
use crate::chzzk::models::{ChzzkResponse, LiveDetailContent, LiveStreamInfo, PlaybackJson};
use crate::config::ChzzkConfig;

const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";

pub fn extract_best_hls_url(playback_json_str: &Option<String>) -> Result<String> {
    let json_str = playback_json_str.as_ref().ok_or_else(|| anyhow!("No livePlaybackJson available"))?;
    let playback: PlaybackJson = serde_json::from_str(json_str)
        .context("Failed to parse livePlaybackJson")?;

    let hls_media = playback.media.into_iter().find(|m| m.media_id.to_uppercase() == "HLS")
        .ok_or_else(|| anyhow!("No HLS media entry found"))?;

    // Prioritize 1080p, then 720p, then 480p, else fallback to root path
    if let Some(track) = hls_media.encoding_track.iter().find(|t| t.encoding_track_id.contains("1080")) {
        return Ok(track.path.clone());
    }
    if let Some(track) = hls_media.encoding_track.iter().find(|t| t.encoding_track_id.contains("720")) {
        return Ok(track.path.clone());
    }
    if let Some(track) = hls_media.encoding_track.first() {
        return Ok(track.path.clone());
    }

    Ok(hls_media.path)
}

#[derive(Clone)]
pub struct ChzzkClient {
    client: reqwest::Client,
    cookie_header: Option<String>,
}

impl ChzzkClient {
    pub fn new(config: &ChzzkConfig) -> Self {
        let cookie_str = if !config.nid_aut.is_empty() && !config.nid_ses.is_empty() {
            Some(format!("NID_AUT={}; NID_SES={}", config.nid_aut, config.nid_ses))
        } else {
            None
        };

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        if let Some(ref c) = cookie_str {
            if let Ok(val) = HeaderValue::from_str(c) {
                headers.insert(COOKIE, val);
            }
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap_or_default();

        Self {
            client,
            cookie_header: cookie_str,
        }
    }

    pub fn cookie_header(&self) -> Option<&str> {
        self.cookie_header.as_deref()
    }

    pub async fn get_live_detail(&self, channel_id: &str) -> Result<Option<LiveStreamInfo>> {
        let url = format!("https://api.chzzk.naver.com/service/v2/channels/{}/live-detail", channel_id);
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let body: ChzzkResponse<LiveDetailContent> = resp.json().await?;

        if let Some(content) = body.content {
            if content.status == "OPEN" {
                let hls_url = extract_best_hls_url(&content.live_playback_json)?;
                return Ok(Some(LiveStreamInfo {
                    channel_id: channel_id.to_string(),
                    streamer_name: content.channel.channel_name,
                    title: content.live_title.unwrap_or_else(|| "Untitled Broadcast".to_string()),
                    hls_url,
                }));
            }
        }

        Ok(None)
    }
}
```

Create `src/chzzk/mod.rs`:
```rust
pub mod models;
pub mod client;
pub use client::ChzzkClient;
```

Update `src/lib.rs` to export `pub mod chzzk;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_chzzk_client`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/chzzk/ tests/test_chzzk_client.rs src/lib.rs
git commit -m "feat: implement Chzzk API client with live status detection and HLS selector"
```

---

### Task 4: FFmpeg Subprocess Manager & N+1 Chunk Watcher

**Files:**
- Create: `src/recorder/mod.rs`
- Create: `src/recorder/ffmpeg.rs`
- Create: `src/recorder/watcher.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_recorder_watcher.rs`

**Interfaces:**
- Consumes: `chzzk::models::LiveStreamInfo`
- Produces: `recorder::watcher::SegmentWatcher`
- Produces: `recorder::ffmpeg::build_ffmpeg_command`
- Produces: `recorder::watcher::detect_sealed_chunks(dir: &Path, already_enqueued: &mut HashSet<String>) -> Vec<PathBuf>`

- [ ] **Step 1: Write failing test for N+1 segment detection**

Create `tests/test_recorder_watcher.rs`:
```rust
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use chzzk_load::recorder::watcher::detect_sealed_chunks;

#[test]
fn test_n_plus_one_detection_logic() {
    let temp_dir = std::env::temp_dir().join(format!("test_watcher_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let mut enqueued = HashSet::new();

    // Initially chunk_0000.ts is being written, chunk_0001 does NOT exist yet
    let chunk0 = temp_dir.join("chunk_0000.ts");
    let mut f0 = File::create(&chunk0).unwrap();
    f0.write_all(b"partial content").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    // Should NOT seal chunk 0 yet because chunk 1 does not exist
    assert_eq!(sealed.len(), 0);

    // Now chunk_0001.ts appears with > 0 bytes
    let chunk1 = temp_dir.join("chunk_0001.ts");
    let mut f1 = File::create(&chunk1).unwrap();
    f1.write_all(b"start of chunk 1").unwrap();

    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    // chunk_0000.ts MUST be detected as sealed
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0], chunk0);
    assert!(enqueued.contains("chunk_0000.ts"));

    // Running again without new chunks should yield 0
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, false);
    assert_eq!(sealed.len(), 0);

    // When stream concludes (is_final = true), chunk_0001.ts should be sealed
    let sealed = detect_sealed_chunks(&temp_dir, &mut enqueued, true);
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0], chunk1);
    assert!(enqueued.contains("chunk_0001.ts"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_recorder_watcher`
Expected: FAIL with compilation error (`recorder` module missing).

- [ ] **Step 3: Implement `src/recorder/ffmpeg.rs`, `src/recorder/watcher.rs`, `src/recorder/mod.rs`**

Create `src/recorder/ffmpeg.rs`:
```rust
use std::path::Path;
use tokio::process::Command;

pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            other => other,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

pub fn build_ffmpeg_command(
    m3u8_url: &str,
    output_pattern: &Path,
    chunk_duration_seconds: u64,
    cookie_header: Option<&str>,
) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y");

    let mut headers = "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n".to_string();
    if let Some(cookie) = cookie_header {
        headers.push_str(&format!("Cookie: {}\r\n", cookie));
    }
    cmd.arg("-headers").arg(headers);

    cmd.arg("-i").arg(m3u8_url)
        .arg("-c").arg("copy")
        .arg("-f").arg("segment")
        .arg("-segment_time").arg(chunk_duration_seconds.to_string())
        .arg("-segment_format").arg("mpegts")
        .arg("-reset_timestamps").arg("1")
        .arg(output_pattern);

    cmd
}
```

Create `src/recorder/watcher.rs`:
```rust
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

pub fn detect_sealed_chunks(
    session_dir: &Path,
    already_enqueued: &mut HashSet<String>,
    is_stream_finished: bool,
) -> Vec<PathBuf> {
    let mut chunks = Vec::new();

    if let Ok(entries) = fs::read_dir(session_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("ts") {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(meta) = entry.metadata() {
                        if meta.len() > 0 {
                            chunks.push((name.to_string(), path, meta.len()));
                        }
                    }
                }
            }
        }
    }

    chunks.sort_by(|a, b| a.0.cmp(&b.0));
    let mut sealed = Vec::new();

    if chunks.is_empty() {
        return sealed;
    }

    // N+1 rule: If chunk N+1 exists, chunk N is sealed
    for i in 0..chunks.len().saturating_sub(1) {
        let (name, path, _) = &chunks[i];
        if !already_enqueued.contains(name) {
            already_enqueued.insert(name.clone());
            sealed.push(path.clone());
        }
    }

    // If stream ended, the final chunk is also sealed
    if is_stream_finished {
        if let Some((name, path, _)) = chunks.last() {
            if !already_enqueued.contains(name) {
                already_enqueued.insert(name.clone());
                sealed.push(path.clone());
            }
        }
    }

    sealed
}
```

Create `src/recorder/mod.rs`:
```rust
pub mod ffmpeg;
pub mod watcher;
```

Update `src/lib.rs` to export `pub mod recorder;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_recorder_watcher`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recorder/ tests/test_recorder_watcher.rs src/lib.rs
git commit -m "feat: implement FFmpeg command builder and N+1 segment completion watcher"
```

---

### Task 5: Google Drive OAuth2 PKCE & Token Provider

**Files:**
- Create: `src/drive/mod.rs`
- Create: `src/drive/auth.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_drive_auth.rs`

**Interfaces:**
- Consumes: `config::GoogleDriveConfig`
- Produces: `drive::auth::DriveAuth::get_valid_access_token(&self) -> Result<String>`
- Produces: `drive::auth::DriveAuth::load_or_authorize(...) -> Result<DriveAuth>`

- [ ] **Step 1: Write failing test for OAuth2 PKCE challenge generator and token storage serialization**

Create `tests/test_drive_auth.rs`:
```rust
use chzzk_load::drive::auth::{generate_pkce_codes, StoredToken};

#[test]
fn test_pkce_generation() {
    let (verifier, challenge) = generate_pkce_codes();
    assert!(verifier.len() >= 43);
    assert!(!challenge.is_empty());
}

#[test]
fn test_stored_token_serde() {
    let token = StoredToken {
        access_token: "mock_access".to_string(),
        refresh_token: Some("mock_refresh".to_string()),
        expires_at_epoch_sec: 1700000000,
    };

    let json = serde_json::to_string(&token).unwrap();
    let deserialized: StoredToken = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.access_token, "mock_access");
    assert_eq!(deserialized.refresh_token.as_deref(), Some("mock_refresh"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_drive_auth`
Expected: FAIL with compilation error (`drive` module missing).

- [ ] **Step 3: Implement `src/drive/auth.rs` and `src/drive/mod.rs`**

Create `src/drive/auth.rs`:
```rust
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tiny_http::{Response, Server};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at_epoch_sec: u64,
}

#[derive(Debug, Deserialize)]
struct ClientSecretFile {
    installed: Option<ClientSecretDetails>,
    web: Option<ClientSecretDetails>,
}

#[derive(Debug, Deserialize)]
struct ClientSecretDetails {
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    refresh_token: Option<String>,
}

pub fn generate_pkce_codes() -> (String, String) {
    let mut random_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut random_bytes);
    let verifier = URL_SAFE_NO_PAD.encode(random_bytes);

    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(hasher.finalize());

    (verifier, challenge)
}

pub struct DriveAuth {
    token_path: PathBuf,
    client_id: String,
    client_secret: String,
    token_uri: String,
    token: tokio::sync::Mutex<StoredToken>,
}

impl DriveAuth {
    pub async fn load_or_authorize(
        credentials_path: &Path,
        token_path: &Path,
    ) -> Result<Self> {
        let cred_content = fs::read_to_string(credentials_path)
            .with_context(|| format!("Google credentials file not found at {}", credentials_path.display()))?;
        let cred_file: ClientSecretFile = serde_json::from_str(&cred_content)
            .context("Invalid Google OAuth credentials.json format")?;

        let details = cred_file.installed.or(cred_file.web)
            .ok_or_else(|| anyhow!("credentials.json must contain 'installed' or 'web' client settings"))?;

        if token_path.exists() {
            let token_str = fs::read_to_string(token_path)?;
            if let Ok(token) = serde_json::from_str::<StoredToken>(&token_str) {
                return Ok(Self {
                    token_path: token_path.to_path_buf(),
                    client_id: details.client_id,
                    client_secret: details.client_secret,
                    token_uri: details.token_uri,
                    token: tokio::sync::Mutex::new(token),
                });
            }
        }

        // Run one-time browser OAuth flow via loopback
        let (verifier, challenge) = generate_pkce_codes();
        let port = 8085;
        let redirect_uri = format!("http://127.0.0.1:{}/oauth2callback", port);

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope=https://www.googleapis.com/auth/drive.file&code_challenge={}&code_challenge_method=S256&access_type=offline&prompt=consent",
            details.auth_uri, details.client_id, urlencoding_encode(&redirect_uri), challenge
        );

        println!("Starting browser authorization for Google Drive...");
        let _ = open::that(&auth_url);

        let server = Server::http(format!("127.0.0.1:{}", port))
            .map_err(|e| anyhow!("Failed to bind local OAuth server: {}", e))?;

        let code = tokio::task::spawn_blocking(move || -> Result<String> {
            for request in server.incoming_requests() {
                let url = format!("http://localhost{}", request.url());
                if let Ok(parsed) = Url::parse(&url) {
                    if let Some((_, code)) = parsed.query_pairs().find(|(k, _)| k == "code") {
                        let response = Response::from_string("Authentication successful! You can close this tab and return to chzzk-load.");
                        let _ = request.respond(response);
                        return Ok(code.to_string());
                    }
                }
                let _ = request.respond(Response::from_string("Waiting for Google authorization..."));
            }
            Err(anyhow!("OAuth server terminated without receiving authorization code"))
        }).await??;

        // Exchange code for tokens
        let client = reqwest::Client::new();
        let token_resp: TokenResponse = client
            .post(&details.token_uri)
            .form(&[
                ("code", code.as_str()),
                ("client_id", details.client_id.as_str()),
                ("client_secret", details.client_secret.as_str()),
                ("redirect_uri", redirect_uri.as_str()),
                ("grant_type", "authorization_code"),
                ("code_verifier", verifier.as_str()),
            ])
            .send().await?
            .error_for_status()?
            .json().await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let stored_token = StoredToken {
            access_token: token_resp.access_token,
            refresh_token: token_resp.refresh_token,
            expires_at_epoch_sec: now + token_resp.expires_in,
        };

        let json = serde_json::to_string_pretty(&stored_token)?;
        fs::write(token_path, json)?;

        Ok(Self {
            token_path: token_path.to_path_buf(),
            client_id: details.client_id,
            client_secret: details.client_secret,
            token_uri: details.token_uri,
            token: tokio::sync::Mutex::new(stored_token),
        })
    }

    pub async fn get_valid_access_token(&self) -> Result<String> {
        let mut guard = self.token.lock().await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        // Refresh if within 5 minutes of expiration
        if guard.expires_at_epoch_sec <= now + 300 {
            if let Some(ref refresh) = guard.refresh_token {
                let client = reqwest::Client::new();
                let resp: TokenResponse = client
                    .post(&self.token_uri)
                    .form(&[
                        ("client_id", self.client_id.as_str()),
                        ("client_secret", self.client_secret.as_str()),
                        ("refresh_token", refresh.as_str()),
                        ("grant_type", "refresh_token"),
                    ])
                    .send().await?
                    .error_for_status()?
                    .json().await?;

                guard.access_token = resp.access_token;
                guard.expires_at_epoch_sec = now + resp.expires_in;
                if resp.refresh_token.is_some() {
                    guard.refresh_token = resp.refresh_token;
                }

                let json = serde_json::to_string_pretty(&*guard)?;
                let _ = fs::write(&self.token_path, json);
            }
        }

        Ok(guard.access_token.clone())
    }
}

fn urlencoding_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
```

Create `src/drive/mod.rs`:
```rust
pub mod auth;
pub mod client;
```

Update `src/lib.rs` to export `pub mod drive;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_drive_auth`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/drive/ tests/test_drive_auth.rs src/lib.rs
git commit -m "feat: implement Google Drive OAuth2 PKCE authorization and token provider"
```

---

### Task 6: Google Drive Resumable Uploader & Zero-Leak Local Deletion Worker

**Files:**
- Create: `src/drive/client.rs`
- Create: `src/uploader/mod.rs`
- Create: `tests/test_drive_uploader.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `drive::auth::DriveAuth`
- Produces: `drive::client::DriveClient::get_or_create_folder(&self, name: &str, parent_id: Option<&str>) -> Result<String>`
- Produces: `drive::client::DriveClient::upload_file_resumable(...) -> Result<String>`
- Produces: `uploader::UploadWorker`

- [ ] **Step 1: Write failing test for Drive resumable metadata building and MIME detection**

Create `tests/test_drive_uploader.rs`:
```rust
use chzzk_load::drive::client::build_resumable_init_body;

#[test]
fn test_resumable_metadata_payload() {
    let json = build_resumable_init_body("chunk_0000.ts", Some("folder_123"));
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(val["name"], "chunk_0000.ts");
    assert_eq!(val["parents"][0], "folder_123");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_drive_uploader`
Expected: FAIL with compilation error (`build_resumable_init_body` missing).

- [ ] **Step 3: Implement `src/drive/client.rs` and `src/uploader/mod.rs`**

Create `src/drive/client.rs`:
```rust
use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use crate::drive::auth::DriveAuth;

pub fn build_resumable_init_body(filename: &str, parent_id: Option<&str>) -> String {
    let parents = match parent_id {
        Some(id) => vec![id.to_string()],
        None => vec![],
    };
    serde_json::json!({
        "name": filename,
        "parents": parents
    }).to_string()
}

#[derive(Debug, Deserialize)]
struct DriveFileList {
    files: Vec<DriveFileItem>,
}

#[derive(Debug, Deserialize)]
struct DriveFileItem {
    id: String,
    name: String,
}

#[derive(Clone)]
pub struct DriveClient {
    auth: Arc<DriveAuth>,
    client: reqwest::Client,
}

impl DriveClient {
    pub fn new(auth: Arc<DriveAuth>) -> Self {
        Self {
            auth,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_or_create_folder(&self, folder_name: &str, parent_id: Option<&str>) -> Result<String> {
        let token = self.auth.get_valid_access_token().await?;

        // Query if exists
        let mut query = format!("mimeType = 'application/vnd.google-apps.folder' and name = '{}' and trashed = false", folder_name);
        if let Some(pid) = parent_id {
            query.push_str(&format!(" and '{}' in parents", pid));
        }

        let resp: DriveFileList = self.client.get("https://www.googleapis.com/drive/v3/files")
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("q", query.as_str()), ("fields", "files(id, name)")])
            .send().await?
            .error_for_status()?
            .json().await?;

        if let Some(first) = resp.files.first() {
            return Ok(first.id.clone());
        }

        // Create folder
        let mut meta = serde_json::json!({
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder"
        });
        if let Some(pid) = parent_id {
            meta["parents"] = serde_json::json!([pid]);
        }

        let created: DriveFileItem = self.client.post("https://www.googleapis.com/drive/v3/files")
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(meta.to_string())
            .send().await?
            .error_for_status()?
            .json().await?;

        Ok(created.id)
    }

    pub async fn upload_file_resumable<F>(
        &self,
        file_path: &Path,
        parent_folder_id: &str,
        progress_cb: F,
    ) -> Result<String>
    where
        F: Fn(u64, u64) + Send + 'static,
    {
        let filename = file_path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        let meta = tokio::fs::metadata(file_path).await?;
        let file_size = meta.len();

        let token = self.auth.get_valid_access_token().await?;

        // 1. Initiate resumable upload
        let init_body = build_resumable_init_body(filename, Some(parent_folder_id));
        let init_resp = self.client.post("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable")
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header("X-Upload-Content-Type", "video/mp2t")
            .header("X-Upload-Content-Length", file_size.to_string())
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(init_body)
            .send().await?
            .error_for_status()?;

        let location = init_resp.headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| anyhow!("Missing Location header in Google Drive resumable init"))?
            .to_string();

        // 2. Stream chunk with progress
        let file = File::open(file_path).await?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let mut uploaded = 0u64;

        use futures_util::StreamExt;
        let progress_stream = stream.map(move |chunk_result| {
            if let Ok(ref bytes) = chunk_result {
                uploaded += bytes.len() as u64;
                progress_cb(uploaded, file_size);
            }
            chunk_result
        });

        let upload_resp = self.client.put(&location)
            .header(CONTENT_LENGTH, file_size.to_string())
            .header(CONTENT_TYPE, "video/mp2t")
            .body(reqwest::Body::wrap_stream(progress_stream))
            .send().await?
            .error_for_status()?;

        let created: DriveFileItem = upload_resp.json().await?;
        Ok(created.id)
    }
}
```

Create `src/uploader/mod.rs`:
```rust
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::drive::client::DriveClient;

pub struct UploadTask {
    pub session_folder_id: String,
    pub chunk_path: PathBuf,
    pub chunk_name: String,
}

pub struct UploadWorker;

impl UploadWorker {
    pub async fn upload_and_delete(
        client: &DriveClient,
        task: UploadTask,
        on_progress: impl Fn(u64, u64) + Send + 'static,
    ) -> anyhow::Result<u64> {
        let size = tokio::fs::metadata(&task.chunk_path).await?.len();
        client.upload_file_resumable(&task.chunk_path, &task.session_folder_id, on_progress).await?;
        // Deletion only runs upon successful upload confirmation
        tokio::fs::remove_file(&task.chunk_path).await?;
        Ok(size)
    }
}
```

Update `src/lib.rs` to export `pub mod uploader;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_drive_uploader`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/drive/client.rs src/uploader/ tests/test_drive_uploader.rs src/lib.rs
git commit -m "feat: implement Google Drive resumable uploader and immediate local deletion worker"
```

---

### Task 7: Central Event Bus & Background Engine Orchestrator

**Files:**
- Create: `src/tui/event.rs`
- Create: `src/engine/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_engine_events.rs`

**Interfaces:**
- Produces: `tui::event::AppEvent`
- Produces: `engine::EngineOrchestrator`

- [ ] **Step 1: Write failing test for event dispatching**

Create `tests/test_engine_events.rs`:
```rust
use chzzk_load::tui::event::AppEvent;

#[tokio::test]
async fn test_app_event_mpsc_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<AppEvent>(10);
    tx.send(AppEvent::Log("Test log".to_string())).await.unwrap();

    let received = rx.recv().await.unwrap();
    match received {
        AppEvent::Log(msg) => assert_eq!(msg, "Test log"),
        _ => panic!("Expected Log event"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_engine_events`
Expected: FAIL with compilation error (`tui::event` missing).

- [ ] **Step 3: Implement `src/tui/event.rs` and `src/engine/mod.rs`**

Create `src/tui/event.rs`:
```rust
use std::path::PathBuf;
use crossterm::event::KeyEvent;

#[derive(Debug, Clone)]
pub enum AppEvent {
    Tick,
    Key(KeyEvent),
    ChannelUpdate {
        channel_id: String,
        channel_name: String,
        is_live: bool,
        title: String,
    },
    RecordingStarted {
        channel_id: String,
        session_title: String,
    },
    ChunkSealed {
        chunk_name: String,
        size_bytes: u64,
    },
    UploadProgress {
        chunk_name: String,
        uploaded_bytes: u64,
        total_bytes: u64,
        speed_mb_s: f64,
    },
    UploadCompleted {
        chunk_name: String,
        reclaimed_bytes: u64,
    },
    Log(String),
}
```

Create `src/engine/mod.rs`:
```rust
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use chrono::Local;
use tokio::sync::mpsc::Sender;
use crate::app_path::resolve_path;
use crate::chzzk::client::ChzzkClient;
use crate::config::Settings;
use crate::drive::client::DriveClient;
use crate::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use crate::recorder::watcher::detect_sealed_chunks;
use crate::tui::event::AppEvent;
use crate::uploader::{UploadTask, UploadWorker};

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    drive: Option<DriveClient>,
    event_tx: Sender<AppEvent>,
}

impl EngineOrchestrator {
    pub fn new(
        settings: Settings,
        chzzk: ChzzkClient,
        drive: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
    ) -> Self {
        Self {
            settings,
            chzzk,
            drive,
            event_tx,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let (upload_tx, mut upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(50);

        // Upload Consumer Task
        let drive_opt = self.drive.clone();
        let event_tx_upload = self.event_tx.clone();
        tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            while let Some(task) = upload_rx.recv().await {
                if let Some(ref drive) = drive_opt {
                    let tx = event_tx_upload.clone();
                    let name = task.chunk_name.clone();
                    let n = name.clone();
                    let last_update = Arc::new(tokio::sync::Mutex::new(std::time::Instant::now()));

                    let upload_res = UploadWorker::upload_and_delete(
                        drive,
                        task,
                        move |uploaded, total| {
                            let now = std::time::Instant::now();
                            let mb_s = (uploaded as f64 / 1_048_576.0) / start_time.elapsed().as_secs_f64().max(0.1);
                            let _ = tx.try_send(AppEvent::UploadProgress {
                                chunk_name: n.clone(),
                                uploaded_bytes: uploaded,
                                total_bytes: total,
                                speed_mb_s: mb_s,
                            });
                        },
                    ).await;

                    match upload_res {
                        Ok(reclaimed) => {
                            let _ = event_tx_upload.send(AppEvent::UploadCompleted {
                                chunk_name: name.clone(),
                                reclaimed_bytes: reclaimed,
                            }).await;
                            let _ = event_tx_upload.send(AppEvent::Log(format!(
                                "[CLEAN] Uploaded & deleted {} (reclaimed {:.1} MB)",
                                name, reclaimed as f64 / 1_048_576.0
                            ))).await;
                        }
                        Err(e) => {
                            let _ = event_tx_upload.send(AppEvent::Log(format!(
                                "[ERROR] Upload failed for {}: {}", name, e
                            ))).await;
                        }
                    }
                }
            }
        });

        // Channel Monitor Loop
        let poll_interval = Duration::from_secs(self.settings.general.poll_interval_seconds);
        loop {
            for channel in &self.settings.channels {
                match self.chzzk.get_live_detail(&channel.id).await {
                    Ok(Some(info)) => {
                        let _ = self.event_tx.send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: info.streamer_name.clone(),
                            is_live: true,
                            title: info.title.clone(),
                        }).await;

                        // Spawn recorder session if not already recording
                        // (orchestrator tracks active sessions)
                    }
                    Ok(None) => {
                        let _ = self.event_tx.send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: channel.name.clone(),
                            is_live: false,
                            title: "Offline".to_string(),
                        }).await;
                    }
                    Err(e) => {
                        let _ = self.event_tx.send(AppEvent::Log(format!(
                            "[WARN] Polling failed for {}: {}", channel.id, e
                        ))).await;
                    }
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}
```

Update `src/lib.rs` to export `pub mod tui; pub mod engine;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_engine_events`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tui/event.rs src/engine/ tests/test_engine_events.rs src/lib.rs
git commit -m "feat: implement central event bus and engine orchestrator"
```

---

### Task 8: Ratatui TUI Dashboard Layout & Keyboard Navigation

**Files:**
- Create: `src/tui/app.rs`
- Create: `src/tui/ui.rs`
- Create: `src/tui/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: `tui::event::AppEvent`
- Produces: `tui::app::App`
- Produces: `tui::ui::draw_ui`

- [ ] **Step 1: Write failing test for TUI state updates**

Create `tests/test_tui_state.rs`:
```rust
use chzzk_load::tui::app::App;
use chzzk_load::tui::event::AppEvent;

#[test]
fn test_app_state_mutation_on_events() {
    let mut app = App::new();
    assert_eq!(app.reclaimed_mb, 0.0);

    app.handle_event(AppEvent::Log("Hello".to_string()));
    assert_eq!(app.logs.len(), 1);
    assert_eq!(app.logs[0], "Hello");

    app.handle_event(AppEvent::UploadCompleted {
        chunk_name: "chunk_0000.ts".to_string(),
        reclaimed_bytes: 524_288_000,
    });
    assert!((app.reclaimed_mb - 500.0).abs() < 1.0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_tui_state`
Expected: FAIL with compilation error (`tui::app` missing).

- [ ] **Step 3: Implement `src/tui/app.rs`, `src/tui/ui.rs`, and `src/tui/mod.rs`**

Create `src/tui/app.rs`:
```rust
use std::collections::HashMap;
use crate::tui::event::AppEvent;

#[derive(Debug, Clone)]
pub struct ChannelItem {
    pub id: String,
    pub name: String,
    pub is_live: bool,
    pub title: String,
}

pub struct App {
    pub channels: Vec<ChannelItem>,
    pub selected_channel_idx: usize,
    pub active_stream: Option<String>,
    pub active_upload_name: Option<String>,
    pub upload_progress_pct: u16,
    pub upload_speed: f64,
    pub uploaded_count: usize,
    pub reclaimed_mb: f64,
    pub logs: Vec<String>,
    pub should_quit: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
            selected_channel_idx: 0,
            active_stream: None,
            active_upload_name: None,
            upload_progress_pct: 0,
            upload_speed: 0.0,
            uploaded_count: 0,
            reclaimed_mb: 0.0,
            logs: Vec::new(),
            should_quit: false,
        }
    }

    pub fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::ChannelUpdate { channel_id, channel_name, is_live, title } => {
                if let Some(ch) = self.channels.iter_mut().find(|c| c.id == channel_id) {
                    ch.is_live = is_live;
                    ch.title = title;
                } else {
                    self.channels.push(ChannelItem {
                        id: channel_id,
                        name: channel_name,
                        is_live,
                        title,
                    });
                }
            }
            AppEvent::UploadProgress { chunk_name, uploaded_bytes, total_bytes, speed_mb_s } => {
                self.active_upload_name = Some(chunk_name);
                if total_bytes > 0 {
                    self.upload_progress_pct = ((uploaded_bytes as f64 / total_bytes as f64) * 100.0) as u16;
                }
                self.upload_speed = speed_mb_s;
            }
            AppEvent::UploadCompleted { chunk_name, reclaimed_bytes } => {
                self.uploaded_count += 1;
                self.reclaimed_mb += reclaimed_bytes as f64 / 1_048_576.0;
                if self.active_upload_name.as_deref() == Some(&chunk_name) {
                    self.active_upload_name = None;
                    self.upload_progress_pct = 0;
                }
            }
            AppEvent::Log(msg) => {
                self.logs.push(msg);
                if self.logs.len() > 200 {
                    self.logs.remove(0);
                }
            }
            _ => {}
        }
    }
}
```

Create `src/tui/ui.rs`:
```rust
use ratatui::prelude::*;
use ratatui::widgets::*;
use crate::tui::app::App;

pub fn draw_ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Min(10),   // Body (Channels + Active Progress)
            Constraint::Length(8),  // Logs
            Constraint::Length(1),  // Keybind footer
        ])
        .split(f.area());

    // Header
    let header = Paragraph::new(format!(
        " chzzk-load v0.1.0 │ Reclaimed Space: {:.1} MB │ Chunks Uploaded: {}",
        app.reclaimed_mb, app.uploaded_count
    ))
    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
    .block(Block::default().borders(Borders::ALL).border_type(BorderType::Rounded));
    f.render_widget(header, chunks[0]);

    // Body: Split horizontally
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(chunks[1]);

    // Channels List
    let items: Vec<ListItem> = app.channels.iter().map(|c| {
        let status = if c.is_live { "[ LIVE ]" } else { "[ OFFLINE ]" };
        let color = if c.is_live { Color::Green } else { Color::DarkGray };
        ListItem::new(format!("{} {} - {}", status, c.name, c.title)).style(Style::default().fg(color))
    }).collect();

    let channels_list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Monitored Channels ").border_type(BorderType::Rounded));
    f.render_widget(channels_list, body_chunks[0]);

    // Pipeline Panel
    let active_panel = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Length(4), Constraint::Min(2)])
        .split(body_chunks[1]);

    let upload_title = app.active_upload_name.as_deref().unwrap_or("Idle (Waiting for completed chunk)");
    let upload_gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(format!(" Cloud Upload: {} ", upload_title)))
        .gauge_style(Style::default().fg(Color::LightGreen))
        .percent(app.upload_progress_pct)
        .label(format!("{}% @ {:.1} MB/s", app.upload_progress_pct, app.upload_speed));
    f.render_widget(upload_gauge, active_panel[0]);

    // Logs Panel
    let log_items: Vec<ListItem> = app.logs.iter().rev().take(15).rev().map(|l| ListItem::new(l.as_str())).collect();
    let logs_widget = List::new(log_items)
        .block(Block::default().borders(Borders::ALL).title(" Live Activity Logs ").border_type(BorderType::Rounded));
    f.render_widget(logs_widget, chunks[2]);

    // Footer
    let footer = Paragraph::new(" [q] Quit   [↑/↓] Select Channel   [r] Refresh ")
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(footer, chunks[3]);
}
```

Create `src/tui/mod.rs`:
```rust
pub mod app;
pub mod event;
pub mod ui;
```

Update `src/lib.rs` to export `pub mod tui;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_tui_state`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tui/ tests/test_tui_state.rs src/lib.rs
git commit -m "feat: implement Ratatui dashboard widgets, state machine, and logs view"
```

---

### Task 9: CLI Entrypoint, Standalone Binary Build, and Verification

**Files:**
- Create: `src/main.rs`
- Create: `tests/test_cli_smoke.rs`

**Interfaces:**
- Produces: Executable `target/release/chzzk-load.exe`

- [ ] **Step 1: Write `src/main.rs` wiring CLI flags, config loading, and TUI loop**

Create `src/main.rs`:
```rust
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::prelude::*;
use chzzk_load::app_path::resolve_path;
use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::Settings;
use chzzk_load::drive::auth::DriveAuth;
use chzzk_load::drive::client::DriveClient;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::app::App;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::tui::ui::draw_ui;

#[derive(Parser, Debug)]
#[command(name = "chzzk-load", author, version, about = "Real-time Chzzk stream recording and Google Drive syncing")]
struct Cli {
    #[arg(short, long, help = "Path to dedicated settings.json file")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let config_path = args.config.unwrap_or_else(|| resolve_path(&PathBuf::from("settings.json")));

    let settings = Settings::load_or_create_default(&config_path)?;
    let creds_path = resolve_path(&PathBuf::from(&settings.google_drive.credentials_path));
    let token_path = resolve_path(&PathBuf::from(&settings.google_drive.token_path));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(100);

    // Initialize Google Drive Auth if credentials exist
    let drive_client = if creds_path.exists() {
        match DriveAuth::load_or_authorize(&creds_path, &token_path).await {
            Ok(auth) => {
                let _ = event_tx.send(AppEvent::Log("[INFO] Google Drive authenticated successfully".to_string())).await;
                Some(DriveClient::new(Arc::new(auth)))
            }
            Err(e) => {
                let _ = event_tx.send(AppEvent::Log(format!("[WARN] Drive auth failed: {}", e))).await;
                None
            }
        }
    } else {
        let _ = event_tx.send(AppEvent::Log(format!("[INFO] '{}' not found; running in local-only recording mode", creds_path.display()))).await;
        None
    };

    let chzzk = ChzzkClient::new(&settings.chzzk);
    let orchestrator = Arc::new(EngineOrchestrator::new(settings.clone(), chzzk, drive_client, event_tx.clone()));
    tokio::spawn(orchestrator.run());

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    for ch in &settings.channels {
        app.channels.push(chzzk_load::tui::app::ChannelItem {
            id: ch.id.clone(),
            name: ch.name.clone(),
            is_live: false,
            title: "Checking...".to_string(),
        });
    }

    // Main TUI render loop
    let tick_rate = Duration::from_millis(100);
    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }

        while let Ok(ev) = event_rx.try_recv() {
            app.handle_event(ev);
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
```

- [ ] **Step 2: Build release binary without cargo run dependency**

Run: `cargo build --release`
Expected: Binary generated at `target/release/chzzk-load.exe` with exit code 0.

- [ ] **Step 3: Verify standalone executable execution**

Run: `target/release/chzzk-load.exe --help`
Expected: Displays help message, options, and version output without needing cargo.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: implement CLI entrypoint and build standalone release executable"
```
