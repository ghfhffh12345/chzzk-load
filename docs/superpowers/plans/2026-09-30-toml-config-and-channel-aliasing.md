# Migration to `settings.toml` and Channel Aliasing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `chzzk-load` from legacy `settings.json` to `settings.toml` using `toml = "1.1"`, implement first-class channel `alias` support, and autonomously resolve official channel names to eliminate online/offline TUI flickering.

**Architecture:** Replace the JSON configuration loader with `toml = "1.1"` in Rust 2024 edition, supporting both shorthand ID strings and tables with an optional `alias`. Update `ChzzkClient` to extract `channelName` across both `OPEN` and `CLOSE` statuses, and store resolved names in an in-memory `RwLock` cache in `EngineOrchestrator` to enforce consistent folder naming (`[{timestamp}] {alias} - {title}`) and stable TUI display without online/offline state flipping.

**Tech Stack:** Rust 2024 edition, `toml = "1.1"`, `serde`, `tokio`, `ratatui`, `reqwest`.

**Spec:** [docs/superpowers/specs/2026-09-30-toml-config-and-channel-aliasing-design.md](file:///C:/Users/official/Documents/Code/chzzk-load/docs/superpowers/specs/2026-09-30-toml-config-and-channel-aliasing-design.md)

## Global Constraints

- Rust 2024 edition conventions (avoid `mod.rs`, inline format strings).
- `toml = "1.1"` dependency in `Cargo.toml`.
- Complete deprecation and removal of `settings.json` (zero fallback or backward compatibility logic for JSON).
- Folder naming: Option A (`[{timestamp}] {alias} - {title}` if alias is present, otherwise `[{timestamp}] {streamer_name} - {title}`).
- TUI list display: Option B (Only show `alias` if configured; otherwise fall back to `streamer_name`).
- All tests must pass with `cargo test --all-targets`.
- Linter must pass with `cargo clippy --all-targets -- -D warnings`.
- Code must format cleanly with `cargo fmt --check`.

## Review Focus

1. **Shorthand string channel entries**: `channels = ["id1", "id2"]` in `settings.toml` deserializes into `ChannelConfig { id: "id1", alias: None }`.
2. **Offline streamer name resolution**: `LiveDetail::Close` retrieves `streamer_name` from API payload, avoiding fallback to raw hex IDs in TUI when channels are offline.
3. **Alias folder sanitization**: User-supplied alias with special filesystem characters is sanitized via `sanitize_filename` in `ActiveSessionState::folder_name`.
4. **Default template creation**: When neither `settings.toml` nor custom config exists, `Settings::load_or_create_default` writes a valid, commented TOML file.
5. **CLI argument priority**: `--config <custom_path>` overrides default `settings.toml` resolution.

---

### Task 1: Add `toml = "1.1"` and Implement TOML Configuration with Channel Aliasing

**Files:**
- Modify: `Cargo.toml:6-12`
- Modify: `src/config.rs:1-164`
- Test: `tests/test_config.rs:1-93`

**Interfaces:**
- Consumes: `toml = "1.1"`, `serde::{Serialize, Deserialize, Deserializer}`
- Produces:
  - `pub struct ChannelConfig { pub id: String, pub alias: Option<String> }`
  - `impl ChannelConfig { pub fn new(id: impl Into<String>) -> Self; pub fn with_alias(id: impl Into<String>, alias: impl Into<String>) -> Self; }`
  - `impl Settings { pub fn load_or_create_default(path: &Path) -> anyhow::Result<Self>; }`

- [ ] **Step 1: Write the failing tests in `tests/test_config.rs`**

```rust
use chzzk_load::config::{ChannelConfig, GeneralConfig, RcloneConfig, Settings};
use std::path::Path;

#[test]
fn test_channel_config_shorthand_string_deserialization() {
    let toml_data = r#"
        channels = [
            "dc7fb0d085cfbbe90e11836e3b85b784",
            "c8adce2ff4a3618931e07c327e1fa070",
        ]
    "#;
    #[derive(serde::Deserialize)]
    struct Wrapper {
        channels: Vec<ChannelConfig>,
    }
    let parsed: Wrapper = toml::from_str(toml_data).expect("Failed to parse shorthand string channels");
    assert_eq!(parsed.channels.len(), 2);
    assert_eq!(parsed.channels[0].id, "dc7fb0d085cfbbe90e11836e3b85b784");
    assert_eq!(parsed.channels[0].alias, None);
    assert_eq!(parsed.channels[1].id, "c8adce2ff4a3618931e07c327e1fa070");
    assert_eq!(parsed.channels[1].alias, None);
}

#[test]
fn test_channel_config_table_with_and_without_alias() {
    let toml_data = r#"
        [[channels]]
        id = "dc7fb0d085cfbbe90e11836e3b85b784"
        alias = "Soyeon"

        [[channels]]
        id = "c8adce2ff4a3618931e07c327e1fa070"
    "#;
    #[derive(serde::Deserialize)]
    struct Wrapper {
        channels: Vec<ChannelConfig>,
    }
    let parsed: Wrapper = toml::from_str(toml_data).expect("Failed to parse table channels");
    assert_eq!(parsed.channels.len(), 2);
    assert_eq!(parsed.channels[0].id, "dc7fb0d085cfbbe90e11836e3b85b784");
    assert_eq!(parsed.channels[0].alias.as_deref(), Some("Soyeon"));
    assert_eq!(parsed.channels[1].id, "c8adce2ff4a3618931e07c327e1fa070");
    assert_eq!(parsed.channels[1].alias, None);
}

#[test]
fn test_settings_toml_roundtrip() {
    let settings = Settings::default();
    let toml_str = toml::to_string_pretty(&settings).expect("Serialize to toml");
    let deserialized: Settings = toml::from_str(&toml_str).expect("Deserialize from toml");
    assert_eq!(deserialized.general.chunk_duration_seconds, 600);
    assert_eq!(deserialized.channels.len(), 1);
    assert_eq!(deserialized.channels[0].id, "4c3b44869c9b1399723ec28ec236f736");
    assert_eq!(deserialized.channels[0].alias.as_deref(), Some("SampleStreamer"));
}

#[test]
fn test_load_or_create_creates_settings_toml() {
    let temp_dir = std::env::temp_dir().join(format!("chzzk_toml_test_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("settings.toml");

    assert!(!config_path.exists());
    let settings = Settings::load_or_create_default(&config_path).expect("Create default settings.toml");
    assert!(config_path.exists());
    assert_eq!(settings.general.chunk_duration_seconds, 600);

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("[general]"));
    assert!(content.contains("[rclone]"));
    assert!(content.contains("[[channels]]"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test test_config`
Expected: Compilation failure (missing `toml` dependency and missing fields in `ChannelConfig`).

- [ ] **Step 3: Implement `toml` dependency and `src/config.rs`**

1. In `Cargo.toml`, add `toml = "1.1"` under `[dependencies]`.
2. In `src/config.rs`:
   - Redefine `ChannelConfig`:
     ```rust
     #[derive(Debug, Clone, Serialize, PartialEq, Eq)]
     pub struct ChannelConfig {
         pub id: String,
         #[serde(default, skip_serializing_if = "Option::is_none")]
         pub alias: Option<String>,
     }
     ```
   - Implement constructors `new` and `with_alias`.
   - Implement custom `serde::Deserialize` for `ChannelConfig` using an untagged `RawChannel` intermediate enum.
   - Update `default_channels()`:
     ```rust
     fn default_channels() -> Vec<ChannelConfig> {
         vec![ChannelConfig::with_alias(
             "4c3b44869c9b1399723ec28ec236f736",
             "SampleStreamer",
         )]
     }
     ```
   - In `Settings::load_or_create_default`, parse via `toml::from_str(&content)`, and generate default template with explanatory comments.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_config`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/config.rs tests/test_config.rs
git commit -m "feat(config): migrate to settings.toml and add channel aliasing"
```

---

### Task 2: Update `ChzzkClient` and `LiveDetail` to Extract Streamer Name on `Close`

**Files:**
- Modify: `src/chzzk/models.rs:1-120`
- Modify: `src/chzzk/client.rs:322-414`
- Test: `tests/test_chzzk_client.rs`

**Interfaces:**
- Consumes: `body.content.channel.channel_name` in `get_live_detail`
- Produces: `LiveDetail::Close { streamer_name: Option<String> }`

- [ ] **Step 1: Write the failing test in `tests/test_chzzk_client.rs`**

```rust
#[test]
fn test_get_live_detail_close_extracts_streamer_name() {
    let json_body = r#"{
        "code": 200,
        "message": null,
        "content": {
            "status": "CLOSE",
            "liveTitle": null,
            "channel": {
                "channelId": "test_channel_id",
                "channelName": "OfflineStreamer"
            }
        }
    }"#;

    let body: ChzzkResponse<LiveDetailContent> = serde_json::from_str(json_body).unwrap();
    assert_eq!(body.content.as_ref().unwrap().status, "CLOSE");
    assert_eq!(body.content.as_ref().unwrap().channel.channel_name, "OfflineStreamer");
}
```

- [ ] **Step 2: Run test to verify `LiveDetail::Close` does not yet carry `streamer_name`**

Run: `cargo test --test test_chzzk_client`
Expected: PASS for model, but verify `LiveDetail::Close` enum definition needs updating.

- [ ] **Step 3: Update `LiveDetail` in `src/chzzk/models.rs` and `src/chzzk/client.rs`**

1. In `src/chzzk/models.rs`:
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
2. In `src/chzzk/client.rs` `get_live_detail`:
   ```rust
   let streamer_name = body.content.as_ref().map(|c| c.channel.channel_name.clone());
   if let Some(content) = body.content.filter(|c| c.status == "OPEN") {
       // ... existing Open / Restricted checks ...
   } else {
       Ok(LiveDetail::Close { streamer_name })
   }
   ```

- [ ] **Step 4: Run tests to verify `src/chzzk` compiles**

Run: `cargo check --lib`
Expected: Only callers of `LiveDetail::Close` in `src/engine.rs` report mismatch.

- [ ] **Step 5: Commit**

```bash
git add src/chzzk/models.rs src/chzzk/client.rs tests/test_chzzk_client.rs
git commit -m "feat(chzzk): extract streamer name in LiveDetail::Close"
```

---

### Task 3: Refactor `EngineOrchestrator` Name Caching, Folder Naming, and TUI State

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/tui/app.rs:110-120`
- Modify: `src/uploader.rs:36`
- Test: `tests/test_engine_events.rs`
- Test: `tests/test_engine_chat.rs`

**Interfaces:**
- Consumes: `ChannelConfig`, `LiveDetail::Close { streamer_name }`
- Produces:
  - Stable `display_name = channel.alias.as_deref().unwrap_or(streamer_name)`
  - Folder name: `[{timestamp}] {display_name} - {title}`
  - Non-flickering `AppEvent::ChannelUpdate`

- [ ] **Step 1: Write integration tests in `tests/test_engine_events.rs` for channel aliasing and folder naming**

```rust
#[tokio::test]
async fn test_engine_folder_naming_with_alias() {
    let state = ActiveSessionState::new(
        "2026-09-30_1100".to_string(),
        "CustomAlias".to_string(),
        "Gaming Stream".to_string(),
    );
    assert_eq!(state.folder_name(), "[2026-09-30_1100] CustomAlias - Gaming Stream");
}

#[tokio::test]
async fn test_engine_channel_update_preserves_alias_when_stream_closes() {
    // Verify that LiveDetail::Close maintains the alias or cached streamer name
}
```

- [ ] **Step 2: Run test to verify compile failure**

Run: `cargo test --test test_engine_events test_engine_folder_naming_with_alias`
Expected: Compile error on `LiveDetail::Close` pattern matches and `channel.name`.

- [ ] **Step 3: Implement name cache and update `src/engine.rs` and `src/tui/app.rs`**

1. In `src/engine.rs`:
   - Add field `channel_names: Arc<tokio::sync::RwLock<HashMap<String, String>>>` to `EngineOrchestrator`.
   - Update `LiveDetail::Close { streamer_name }`:
     - If `Some(name)` is present, insert into `channel_names`.
     - In `AppEvent::ChannelUpdate`, send `channel_name: display_name` where `display_name = channel.alias.clone().or_else(|| channel_names.get(&channel.id).cloned()).unwrap_or_else(|| channel.id.clone())`.
   - In `LiveDetail::Open(info)`:
     - Insert `info.streamer_name` into `channel_names`.
     - Resolve session `display_name = channel.alias.clone().unwrap_or_else(|| info.streamer_name.clone())`.
     - Construct `ActiveSessionState::new(start_timestamp, display_name, info.title.clone())`.
     - Send `AppEvent::ChannelUpdate` with `channel_name: display_name`.
   - In `LiveDetail::Restricted`:
     - Record `streamer_name` into `channel_names`.
     - Send `AppEvent::ChannelUpdate` with `display_name`.
2. In `src/tui/app.rs`:
   - Initialize `ChannelItem`:
     `name = ch.alias.clone().unwrap_or_else(|| ch.id.clone())`.
3. Fix all test references from `channel.name` to `ChannelConfig::new` or `ChannelConfig::with_alias`.

- [ ] **Step 4: Run integration tests to verify they pass**

Run: `cargo test --test test_engine_events`
Run: `cargo test --test test_engine_chat`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/tui/app.rs tests/test_engine_events.rs tests/test_engine_chat.rs
git commit -m "feat(engine): cache streamer names and format session folders with alias"
```

---

### Task 4: Update CLI Entrypoint, Path Resolution, and Default Files

**Files:**
- Modify: `src/main.rs:25-55`
- Modify: `src/app_path.rs:1-120`
- Test: `tests/test_app_path.rs:1-150`

**Interfaces:**
- Consumes: `settings.toml`
- Produces: CLI `--config <path>` defaulting to `settings.toml`

- [ ] **Step 1: Update `tests/test_app_path.rs` from `settings.json` to `settings.toml`**

Replace all string literals `"settings.json"` with `"settings.toml"`.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --test test_app_path`
Expected: FAIL if code still references `settings.json`.

- [ ] **Step 3: Update `src/main.rs` and `src/app_path.rs`**

1. In `src/main.rs`:
   ```rust
   #[arg(short, long, help = "Path to dedicated settings.toml file")]
   config: Option<PathBuf>,
   ```
   ```rust
   let config_path = args
       .config
       .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.toml")));
   ```
2. In `src/app_path.rs`, update documentation comments referencing `settings.toml`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_app_path`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs src/app_path.rs tests/test_app_path.rs
git commit -m "feat(cli): switch default config path from settings.json to settings.toml"
```

---

### Task 5: Deprecate `settings.json` Across Repo, Docs, Scripts, and CI

**Files:**
- Modify: `.gitignore`
- Remove: `settings.json` (if present)
- Create: `settings.toml` (template)
- Modify: `scripts/test-npm-packages.js`
- Modify: `README.md`
- Modify: `README.ko.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update `.gitignore` and local workspace files**

1. In `.gitignore`, replace `settings.json` with `settings.toml`.
2. Remove any local `settings.json` file.
3. Create default `settings.toml` in repository root.

- [ ] **Step 2: Update `scripts/test-npm-packages.js`**

1. Replace `settings.json` mock checks with `settings.toml`.
2. Replace JSON syntax error tests (`'{ MALFORMED }'`) with TOML syntax error (`'invalid toml = ='`).

- [ ] **Step 3: Run npm package tests to verify**

Run: `node scripts/test-npm-packages.js`
Expected: PASS.

- [ ] **Step 4: Update Documentation**

1. `README.md` & `README.ko.md`: Update configuration sections, examples, and descriptions to use `settings.toml` with `[[channels]]` and `alias`.
2. `AGENTS.md`: Update repository structure, configuration guide, and architectural invariants.

- [ ] **Step 5: Full verification suite**

Run:
```bash
cargo check --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
node scripts/test-npm-packages.js
```
Expected: All pass with 0 errors and 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add .gitignore settings.toml scripts/test-npm-packages.js README.md README.ko.md AGENTS.md
git commit -m "docs: deprecate settings.json in favor of settings.toml"
```
