# Specification: Migration to `settings.toml` and Channel Aliasing Design

## 1. Executive Summary & Goals

`chzzk-load` is currently at version `0.7.0`. This design specification details the complete deprecation and removal of `settings.json` in favor of a modern, human-readable, and commentable `settings.toml` format using `toml = "1.1"` in Rust 2024 edition. Concurrently, it resolves the channel naming dissonance by introducing first-class `alias` support and autonomous channel name resolution from the Chzzk API.

### Key Objectives
1. **Clean Deprecation of `settings.json`**: Drop all runtime dependencies on and fallback logic for `settings.json`. `settings.toml` becomes the single source of configuration truth.
2. **First-Class Channel Aliasing**: Provide optional `alias` field for channels while allowing zero-boilerplate ID-only strings.
3. **Autonomous Official Name Discovery**: Automatically resolve and cache official streamer names from Chzzk API (`/service/v2/channels/{id}/live-detail`) across both `OPEN` and `CLOSE` statuses.
4. **Stable TUI and Session Directory Contracts**:
   - **TUI List Display**: Option B — Only show `alias` if configured; otherwise fall back to the official `streamer_name` (no flipping between online/offline states).
   - **Session Folder Naming**: Option A — Format as `[{timestamp}] {alias} - {title}` if `alias` is configured; otherwise `[{timestamp}] {streamer_name} - {title}`.
5. **Modern Rust 2024 Idioms**: Implement custom Serde deserialization with untagged intermediate representations for flexible configuration input.

---

## 2. Configuration Schema & Types (`src/config.rs`)

### 2.1 TOML Dependency
In `Cargo.toml`:
```toml
[dependencies]
toml = "1.1"
```

### 2.2 Default `settings.toml` Structure
If no configuration file exists at startup, `chzzk-load` automatically generates `settings.toml` with informative comments:

```toml
# chzzk-load configuration

[general]
# Chunk duration for MPEG-TS video segments in seconds
chunk_duration_seconds = 60
# Interval between stream polling cycles in seconds
poll_interval_seconds = 20
# Cooldown window in seconds to prevent duplicate sessions from CDN caching
stream_cooldown_seconds = 0
# Directory to store local session recordings
recordings_dir = "recordings"
# Minimum required free disk space in GB before pausing/warning
min_free_disk_gb = 2.0
# Record live chat messages concurrently into JSON Lines chunks
record_chat = true
# Buffer flush interval for chat writer in seconds
chat_flush_interval_seconds = 30

[rclone]
# Target remote path (e.g. "gdrive:Chzzk_Recordings" or "" for local-only mode)
remote_path = "gdrive:Chzzk_Recordings"
# Maximum concurrent uploads across different channels
upload_concurrency = 3
# Path to rclone binary
rclone_bin = "rclone"
# Additional arguments passed to rclone child process
extra_args = []

[chzzk]
# Optional Naver session cookies for age-restricted (19+) or subscriber streams
nid_aut = ""
nid_ses = ""

# Monitored Channels:
# Channels can be specified as a list of strings (shorthand ID) or an array of tables.
#
# Option A: Shorthand string array (official channel name resolved automatically via Chzzk API):
# channels = [
#     "dc7fb0d085cfbbe90e11836e3b85b784",
#     "c8adce2ff4a3618931e07c327e1fa070",
# ]
#
# Option B: Array of tables with optional alias:
[[channels]]
id = "4c3b44869c9b1399723ec28ec236f736"
alias = "SampleStreamer"
```

### 2.3 Data Models & Deserializer (`ChannelConfig`)

`ChannelConfig` in `src/config.rs`:
```rust
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChannelConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl ChannelConfig {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            alias: None,
        }
    }

    pub fn with_alias(id: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            alias: Some(alias.into()),
        }
    }
}

impl<'de> serde::Deserialize<'de> for ChannelConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum RawChannel {
            Id(String),
            Table { id: String, alias: Option<String> },
        }

        match RawChannel::deserialize(deserializer)? {
            RawChannel::Id(id) => Ok(ChannelConfig { id, alias: None }),
            RawChannel::Table { id, alias } => Ok(ChannelConfig { id, alias }),
        }
    }
}
```

### 2.4 Configuration Loader
`Settings::load_or_create_default(path: &Path) -> anyhow::Result<Self>`:
- Reads file content as UTF-8 string.
- Deserializes using `toml::from_str::<Settings>(&content)`.
- If file does not exist (`io::ErrorKind::NotFound`), generates parent directories, serializes default template using `toml::to_string_pretty(&settings)` (or a predefined commented template), writes to disk, and returns default instance.

---

## 3. Metadata Discovery & Engine Architecture

### 3.1 Chzzk API Metadata Extraction (`src/chzzk/models.rs` & `src/chzzk/client.rs`)
In Chzzk's `/service/v2/channels/{channel_id}/live-detail` response:
- Even when `status == "CLOSE"`, `content.channel.channelName` is populated.
- Update `LiveDetail` enum:
  ```rust
  pub enum LiveDetail {
      Open(LiveStreamInfo),
      Restricted {
          channel_id: String,
          live_id: Option<u64>,
          streamer_name: String,
          title: String,
          chat_channel_id: Option<String>,
          adult: bool,
      },
      Close {
          streamer_name: Option<String>,
      },
  }
  ```
- In `ChzzkClient::get_live_detail`:
  ```rust
  let streamer_name = body.content.as_ref().map(|c| c.channel.channel_name.clone());
  if let Some(content) = body.content.filter(|c| c.status == "OPEN") {
      // Return LiveDetail::Open(...) or LiveDetail::Restricted(...)
  } else {
      Ok(LiveDetail::Close { streamer_name })
  }
  ```

### 3.2 In-Memory Name Cache in `EngineOrchestrator` (`src/engine.rs`)
- Add `channel_names: Arc<tokio::sync::RwLock<HashMap<String, String>>>` to `EngineOrchestrator`.
- During `poll_channels_once`:
  - When receiving `LiveDetail::Open(info)`: insert `(info.channel_id, info.streamer_name)` into `channel_names`.
  - When receiving `LiveDetail::Restricted { channel_id, streamer_name, .. }`: insert `(channel_id, streamer_name)` into `channel_names`.
  - When receiving `LiveDetail::Close { streamer_name: Some(name) }`: insert `(channel.id, name)` into `channel_names`.
- Name Resolution Helper:
  ```rust
  pub fn resolve_display_name(
      channel: &ChannelConfig,
      cached_names: &HashMap<String, String>,
  ) -> String {
      if let Some(alias) = &channel.alias {
          alias.clone()
      } else if let Some(official) = cached_names.get(&channel.id) {
          official.clone()
      } else {
          channel.id.clone()
      }
  }
  ```

### 3.3 TUI Display Rules
- Initial TUI channel list initialization in `App::new`:
  - `name = channel.alias.clone().unwrap_or_else(|| channel.id.clone())`
- When `AppEvent::ChannelUpdate` is dispatched:
  - Passes `channel_name: resolve_display_name(channel, &channel_names)`
  - When stream goes offline (`LiveDetail::Close`), `channel_name` remains `resolve_display_name(channel, &channel_names)` (never resets to raw ID or causes UI flickering).
- Display in `src/tui/ui.rs`:
  - Formats as `{name} - {title}` (Option B: uses `alias` if configured, otherwise falls back to `streamer_name`).

### 3.4 Session Folder & Cloud Upload Directory Naming
In `ActiveSessionState` (`src/engine.rs`):
- `ActiveSessionState::new(start_timestamp, display_name, initial_title)`
- When starting a recording session in `EngineOrchestrator`:
  ```rust
  let display_name = channel.alias.clone().unwrap_or_else(|| info.streamer_name.clone());
  ActiveSessionState::new(start_timestamp, display_name, info.title.clone())
  ```
- Resulting directory name:
  `format!("[{start_timestamp}] {display_name} - {title}")`
  - If `alias` is configured: `[{timestamp}] {alias} - {title}`
  - If `alias` is not configured: `[{timestamp}] {streamer_name} - {title}`
- Cloud storage destination under `rclone` uses this directory name directly, ensuring remote uploads mirror the local naming convention.

---

## 4. CLI, Path Resolution & Clean Deprecation

### 4.1 CLI Argument (`src/main.rs`)
- Update `clap` CLI argument:
  ```rust
  #[arg(short, long, help = "Path to dedicated settings.toml file")]
  config: Option<PathBuf>,
  ```
- Default path resolution:
  ```rust
  let config_path = args
      .config
      .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.toml")));
  ```

### 4.2 Legacy Artifact Cleanup
1. **`.gitignore`**: Remove `settings.json`, add `settings.toml`.
2. **Project Root**: Remove local `settings.json`, create template `settings.toml`.
3. **Documentation**: Update `README.md`, `README.ko.md`, and `AGENTS.md` to reference `settings.toml` and document the new `alias` and shorthand syntax.
4. **Mock Scripts**: Update `scripts/test-npm-packages.js` to look for `settings.toml` instead of `settings.json`.

---

## 5. Testing & Quality Assurance Plan

1. **Configuration Unit Tests (`tests/test_config.rs`)**:
   - `test_default_settings_toml_roundtrip`: Verifies serialization and deserialization of `Settings` to/from TOML.
   - `test_channel_config_shorthand_string`: Verifies `channels = ["id1", "id2"]` parses into `ChannelConfig { id: "id1", alias: None }`.
   - `test_channel_config_table_with_alias`: Verifies `[[channels]] id = "..." alias = "..."` parses correctly.
   - `test_channel_config_table_without_alias`: Verifies table without `alias` defaults `alias` to `None`.
   - `test_load_or_create_creates_settings_toml`: Verifies non-existent path creates `settings.toml` with default template.
2. **Path Resolution Tests (`tests/test_app_path.rs`)**:
   - Update all `settings.json` assertions to `settings.toml`.
3. **Chzzk Client Unit Tests (`tests/test_chzzk_client.rs`)**:
   - Verify `LiveDetail::Close` returns `streamer_name` when `content.channel.channelName` is in the mock response.
4. **Engine Integration Tests (`tests/test_engine_events.rs`, `tests/test_engine_chat.rs`)**:
   - Verify `ChannelConfig::new` and `ChannelConfig::with_alias` usage across mock engines.
   - Verify folder name uses `alias` when provided and `streamer_name` when omitted.
   - Verify TUI channel updates preserve display names across stream state transitions.
5. **NPM Package Verification (`scripts/test-npm-packages.js`)**:
   - Run Node.js test suite to ensure npm binary launcher tests pass with `settings.toml`.
6. **Linter & Formatting**:
   - Verify `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` pass with 0 warnings.
