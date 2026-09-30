# Specification: Stream Metadata Event Tracking Design (`metadata.jsonl`)

## 1. Executive Summary & Goals

`chzzk-load` currently tracks live broadcast title changes during recording sessions by recording timestamps and strings to `title_history.txt`, uploading this file to remote storage via `rclone rcat` (`backend.upload_text`) upon each change and upon session completion.

This specification details the complete deprecation and removal of `title_history.txt` in favor of a structured, extensible **JSON Lines event stream (`metadata.jsonl`)**. This new subsystem tracks and records **all broadcast metadata state transitions** throughout a live stream. The primary objective is to preserve a comprehensive historical record of all broadcast state transitions so that third-party replay/VOD tools, analytics platforms, and web players can accurately recreate the live viewing experience with sub-second synchronization.

### Key Objectives
1. **Clean Deprecation of `title_history.txt`**: Completely replace `title_history.txt` with `metadata.jsonl` across local storage, cloud synchronization (`UploadBackend::upload_text`), documentation, and unit/integration tests.
2. **Comprehensive State Transition Tracking**: Capture initial state (`INITIAL_STATE`) and all subsequent discrete changes (`METADATA_CHANGED`) across:
   - **Stream Identity & Classification**: Broadcast title (`live_title`), category type (`category_type`), category display value (`live_category_value`), category slug (`live_category`), and hashtags (`tags`).
   - **Broadcaster & Channel Profile**: Broadcaster display name (`channel_name` / `streamer_name`), channel avatar URL (`channel_image_url`), and channel identifier (`channel_id`).
   - **Unified Access Gating (`access_tier`)**: A mutually exclusive enum representing the stream's primary authorization tier: `PUBLIC`, `ADULT_ONLY`, `CHEAT_KEY`, `CHANNEL_SUBSCRIPTION`, `NAVER_PLUS`, and `PAY_PER_VIEW`.
   - **Co-streaming & Watch Parties (`watch_party`)**: Track official watch-alongs (e.g. Asian Games, LCK, World Cup) including `watch_party_no`, `watch_party_tag`, `party_type`, and `paid_product_id`.
   - **Platform Policies & Restrictions (`policies`)**: Geo-blocking (`kr_only_viewing`), platform moderation status (`playable_status`), DVR live rewind (`time_machine_active`), viewer clipping (`clip_active`), and TV app policies (`tv_app_viewing_policy_type`).
   - **Chat Interaction Rules (`chat_rules`)**: Chat availability tiers (`chat_available_group`), follower duration requirements (`min_follower_minute`), and subscriber bypass rules (`allow_subscriber_in_follower_mode`).
   - **Contextual Telemetry**: Instantaneous concurrent viewer count (`concurrent_user_count`) embedded inside each state snapshot.
3. **Microsecond-Accurate Video Synchronization**: Every event records `stream_offset_ms`—the elapsed duration in milliseconds from the exact moment recording began (`std::time::Instant`)—enabling VOD replay players to seek and synchronize metadata changes directly against the video timeline ($O(1)$ random seek) without relying on chunk indices.
4. **Self-Contained Snapshots with Field-Level Deltas**: Every event contains both a complete normalized state snapshot (`state`) and an explicit diff (`changes`) detailing which fields changed (`old` vs `new`).
5. **Flash-Friendly & Zero-Allocation Streaming**:
   - Zero temporary disk files for cloud sync: streamed directly from memory via `rclone rcat` (`backend.upload_text`).
   - Local persistence via append to `<session_dir>/metadata.jsonl`.
   - Zero write amplification: metadata transitions occur infrequently (2–30 times across a 6-hour broadcast).
6. **Cross-API Parity (Official Open API & Internal Service API)**: Fully aligned with both official Chzzk Open API conventions (`openapi.chzzk.naver.com`) and internal service API responses (`api.chzzk.naver.com/service/v2/channels/{id}/live-detail`), verified against active September 2026 production endpoints.

---

## 2. Data Schema & Models (`src/chzzk/models_metadata.rs`)

A new dedicated module [`src/chzzk/models_metadata.rs`](file:///C:/Users/official/Documents/Code/chzzk-load/src/chzzk/models_metadata.rs) will house all metadata event structures, enums, diffing utilities, and Serde deserialization logic.

### 2.1 Enums & Sub-Structures

```rust
use serde::{Deserialize, Serialize};

/// Mutually exclusive access gating tier required to view the live broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamAccessTier {
    /// Free, public broadcast accessible without special credentials.
    Public,
    /// 19+ age restriction requiring Naver adult age verification.
    AdultOnly,
    /// Platform-wide Cheat Key subscription pass required.
    CheatKey,
    /// Streamer-specific channel subscription required (Member-only).
    ChannelSubscription,
    /// Naver Plus membership required (e.g. licensed sports 1080p).
    NaverPlus,
    /// One-off pay-per-view ticket required (paidProduct).
    PayPerView,
}

/// Broad category grouping, forward-compatible with future Chzzk additions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CategoryType {
    Game,
    Sports,
    Etc,
    Talk,
    #[serde(other)]
    Unknown,
}

/// Official co-streaming and watch-along state (e.g. Asian Games, LCK).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchPartyState {
    pub is_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub party_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_product_id: Option<serde_json::Value>,
}

/// Geo-blocking, moderation enforcement, and playback feature flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BroadcastPolicies {
    pub kr_only_viewing: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playable_status: Option<String>,
    pub time_machine_active: bool,
    pub clip_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tv_app_viewing_policy_type: Option<String>,
}

/// Channel chat interaction rules and access requirements.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatRulesState {
    pub chat_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_available_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_follower_minute: Option<u32>,
    pub allow_subscriber_in_follower_mode: bool,
}

/// Complete normalized snapshot of broadcast state at a specific point in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMetadataState {
    #[serde(default)]
    pub live_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_date: Option<String>,
    pub channel_id: String,
    #[serde(alias = "streamer_name")]
    pub channel_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_image_url: Option<String>,
    #[serde(alias = "title")]
    pub live_title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_type: Option<CategoryType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category_value: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub access_tier: StreamAccessTier,
    pub policies: BroadcastPolicies,
    pub watch_party: WatchPartyState,
    pub chat_rules: ChatRulesState,
    pub paid_promotion: bool,
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "live_image_url")]
    pub live_thumbnail_image_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrent_user_count: Option<u64>,
}
```

### 2.2 Event Envelope & Delta Model

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataEventType {
    InitialState,
    MetadataChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDiff<T> {
    pub old: T,
    pub new: T,
}

impl<T> FieldDiff<T> {
    pub fn new(old: T, new: T) -> Self {
        Self { old, new }
    }
}

/// Detailed field-level diff between state transitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_title: Option<FieldDiff<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_name: Option<FieldDiff<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category_type: Option<FieldDiff<Option<CategoryType>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_category_value: Option<FieldDiff<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_category: Option<FieldDiff<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<FieldDiff<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_tier: Option<FieldDiff<StreamAccessTier>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<FieldDiff<BroadcastPolicies>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_party: Option<FieldDiff<WatchPartyState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_rules: Option<FieldDiff<ChatRulesState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_promotion: Option<FieldDiff<bool>>,
}

impl MetadataDelta {
    pub fn is_empty(&self) -> bool {
        self.live_title.is_none()
            && self.channel_name.is_none()
            && self.category_type.is_none()
            && self.live_category_value.is_none()
            && self.live_category.is_none()
            && self.tags.is_none()
            && self.access_tier.is_none()
            && self.policies.is_none()
            && self.watch_party.is_none()
            && self.chat_rules.is_none()
            && self.paid_promotion.is_none()
    }
}

/// A single JSON Lines record in `metadata.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEvent {
    pub version: u8,
    pub event: MetadataEventType,
    pub timestamp: String,
    pub time_local: String,
    pub stream_offset_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<MetadataDelta>,
    pub state: StreamMetadataState,
}
```

### 2.3 Diff Computation Engine (`compute_delta`)

```rust
impl StreamMetadataState {
    /// Computes discrete field diffs between self (old) and incoming (new).
    /// Telemetry fields (concurrent_user_count, live_thumbnail_image_url) do not trigger events on their own.
    pub fn compute_delta(&self, new: &Self) -> Option<MetadataDelta> {
        let mut delta = MetadataDelta::default();
        let mut changed = false;

        if self.live_title != new.live_title {
            delta.live_title = Some(FieldDiff::new(self.live_title.clone(), new.live_title.clone()));
            changed = true;
        }
        if self.channel_name != new.channel_name {
            delta.channel_name = Some(FieldDiff::new(self.channel_name.clone(), new.channel_name.clone()));
            changed = true;
        }
        if self.category_type != new.category_type {
            delta.category_type = Some(FieldDiff::new(self.category_type.clone(), new.category_type.clone()));
            changed = true;
        }
        if self.live_category_value != new.live_category_value {
            delta.live_category_value = Some(FieldDiff::new(self.live_category_value.clone(), new.live_category_value.clone()));
            changed = true;
        }
        if self.live_category != new.live_category {
            delta.live_category = Some(FieldDiff::new(self.live_category.clone(), new.live_category.clone()));
            changed = true;
        }
        if self.tags != new.tags {
            delta.tags = Some(FieldDiff::new(self.tags.clone(), new.tags.clone()));
            changed = true;
        }
        if self.access_tier != new.access_tier {
            delta.access_tier = Some(FieldDiff::new(self.access_tier, new.access_tier));
            changed = true;
        }
        if self.policies != new.policies {
            delta.policies = Some(FieldDiff::new(self.policies.clone(), new.policies.clone()));
            changed = true;
        }
        if self.watch_party != new.watch_party {
            delta.watch_party = Some(FieldDiff::new(self.watch_party.clone(), new.watch_party.clone()));
            changed = true;
        }
        if self.chat_rules != new.chat_rules {
            delta.chat_rules = Some(FieldDiff::new(self.chat_rules.clone(), new.chat_rules.clone()));
            changed = true;
        }
        if self.paid_promotion != new.paid_promotion {
            delta.paid_promotion = Some(FieldDiff::new(self.paid_promotion, new.paid_promotion));
            changed = true;
        }

        if changed { Some(delta) } else { None }
    }
}
```

---

## 3. Chzzk API Integration (`src/chzzk/`)

### 3.1 `LiveDetailContent` Expansion (`src/chzzk/models.rs`)
Expand `LiveDetailContent` with fields returned by `/service/v2/channels/{id}/live-detail`:

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
    #[serde(default)]
    pub adult: Option<bool>,
    #[serde(default)]
    pub open_date: Option<String>,
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
    pub min_follower_minute: Option<u32>,
    #[serde(default)]
    pub allow_subscriber_in_follower_mode: Option<bool>,
    #[serde(default)]
    pub live_image_url: Option<String>,
    #[serde(default)]
    pub concurrent_user_count: Option<u64>,
    #[serde(default)]
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
}
```

### 3.2 Access Tier Resolution Logic (`determine_access_tier`)
A helper function resolves the mutually exclusive `StreamAccessTier` in precedence order:

```rust
pub fn resolve_access_tier(
    content: &LiveDetailContent,
    playback_meta: Option<&PlaybackMeta>,
) -> StreamAccessTier {
    // 1. One-off Paid Product / Pay-Per-View Ticket
    if content.paid_product.is_some() || playback_meta.and_then(|m| m.paid_live).unwrap_or(false) {
        return StreamAccessTier::PayPerView;
    }

    // 2. Channel Subscriber-Only (Member-only)
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
```

### 3.3 `LiveStreamInfo` Update
`LiveStreamInfo` is updated to include the fully assembled `StreamMetadataState`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStreamInfo {
    pub channel_id: String,
    pub live_id: Option<u64>,
    pub streamer_name: String,
    pub title: String,
    pub hls_url: String,
    pub chat_channel_id: Option<String>,
    pub metadata: StreamMetadataState,
}
```

`ChzzkClient::get_live_detail` populates `metadata` on every call across both `LiveDetail::Open` and `LiveDetail::Restricted`.

---

## 4. Engine Orchestration & Session Tracking (`src/engine.rs`)

### 4.1 `ActiveSessionState` Modifications
`ActiveSessionState` transitions from storing title tuples to managing the complete metadata timeline:

```rust
#[derive(Debug, Clone)]
pub struct ActiveSessionState {
    pub start_timestamp: String,
    pub session_start_instant: std::time::Instant,
    pub streamer_name: String,
    pub alias: Option<String>,
    pub initial_title: String,
    pub current_metadata: StreamMetadataState,
    pub metadata_history: Vec<MetadataEvent>,
}

impl ActiveSessionState {
    pub fn new(
        start_timestamp: String,
        streamer_name: String,
        alias: Option<String>,
        initial_metadata: StreamMetadataState,
    ) -> Self {
        let now = chrono::Local::now();
        let utc_now = chrono::Utc::now();
        let initial_title = initial_metadata.live_title.clone();

        let initial_event = MetadataEvent {
            version: 1,
            event: MetadataEventType::InitialState,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms: 0,
            changes: None,
            state: initial_metadata.clone(),
        };

        Self {
            start_timestamp,
            session_start_instant: std::time::Instant::now(),
            streamer_name,
            alias,
            initial_title,
            current_metadata: initial_metadata,
            metadata_history: vec![initial_event],
        }
    }

    pub fn record_metadata_change(
        &mut self,
        new_metadata: StreamMetadataState,
    ) -> Option<(MetadataDelta, MetadataEvent)> {
        let delta = self.current_metadata.compute_delta(&new_metadata)?;
        let now = chrono::Local::now();
        let utc_now = chrono::Utc::now();
        let stream_offset_ms = self.session_start_instant.elapsed().as_millis() as u64;

        let event = MetadataEvent {
            version: 1,
            event: MetadataEventType::MetadataChanged,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms,
            changes: Some(delta.clone()),
            state: new_metadata.clone(),
        };

        self.current_metadata = new_metadata;
        self.metadata_history.push(event.clone());
        Some((delta, event))
    }

    pub fn format_metadata_jsonl(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        for event in &self.metadata_history {
            if let Ok(line) = serde_json::to_string(event) {
                let _ = writeln!(out, "{line}");
            }
        }
        out
    }
}
```

### 4.2 Polling Loop Tracking (`EngineOrchestrator::poll_channels_once`)
In `src/engine.rs`:

```rust
if is_recording {
    let metadata_change = {
        let mut sessions = self.active_sessions.lock().await;
        if let Some(session) = sessions.get_mut(&channel.id) {
            if let Some((delta, event)) = session.record_metadata_change(info.metadata.clone()) {
                let remote_dir = session.folder_name();
                let full_jsonl = session.format_metadata_jsonl();
                Some((delta, event, remote_dir, full_jsonl))
            } else {
                None
            }
        } else {
            None
        }
    };

    if let Some((delta, event, remote_dir, full_jsonl)) = metadata_change {
        // 1. Append locally to <session_dir>/metadata.jsonl
        let recordings_base = resolve_path(Path::new(&self.settings.general.recordings_dir));
        let session_dir = recordings_base.join(&remote_dir);
        let event_line = serde_json::to_string(&event).unwrap_or_default();
        if !event_line.is_empty() {
            use tokio::io::AsyncWriteExt;
            if let Ok(mut file) = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(session_dir.join("metadata.jsonl"))
                .await
            {
                let _ = file.write_all(format!("{event_line}\n").as_bytes()).await;
            }
        }

        // 2. Synchronize to cloud storage via rcat
        if let Some(backend) = self.backend.as_ref() {
            let res = backend.upload_text(&remote_dir, "metadata.jsonl", &full_jsonl).await;
            match res {
                Ok(_) => {
                    let _ = self.event_tx.send(AppEvent::Log(LogEntry::rec(format!(
                        "[{}] Stream metadata changed. Updated 'metadata.jsonl'",
                        channel.id
                    )))).await;
                }
                Err(e) => {
                    let _ = self.event_tx.send(AppEvent::Log(LogEntry::warn(format!(
                        "[{}] Failed to update 'metadata.jsonl': {e}",
                        channel.id
                    )))).await;
                }
            }
        }
    }
}
```

### 4.3 Session Startup & Termination Flushes
1. **Startup (`spawn_recording_session`)**:
   - Write initial `metadata.jsonl` to `<session_dir>/metadata.jsonl`.
   - Upload initial `metadata.jsonl` via `backend.upload_text(&session_folder_name, "metadata.jsonl", &initial_jsonl)`.
2. **Termination (`spawn_recording_session` exit)**:
   - On natural EOF or cancellation, perform a final flush of `metadata.jsonl` to remote storage.

---

## 5. File System & Cloud Sync Comparison

```
Local Recordings Folder:
recordings/
└── [2026-09-30_143000] [Handongsuk] 한동숙 - 2026 아시안게임 롤 결승전 같이보기!/
    ├── chunk_0000.ts
    ├── chunk_0001.ts
    ├── chat_0000.jsonl
    └── metadata.jsonl      <-- Single append-only timeline file (replaces title_history.txt)

Remote Cloud Storage (via rclone rcat):
remote:chzzk/
└── [2026-09-30_143000] [Handongsuk] 한동숙 - 2026 아시안게임 롤 결승전 같이보기!/
    ├── chunk_0000.ts
    ├── chat_0000.jsonl
    └── metadata.jsonl      <-- Automatically updated on every state transition
```

---

## 6. Deprecation & Clean Up Checklist

1. **Delete `title_history.txt` References**:
   - Remove `title_history: Vec<(String, String)>`, `record_title_change`, `format_title_history` from `ActiveSessionState`.
   - Replace all `upload_text(&remote_dir, "title_history.txt", ...)` calls with `"metadata.jsonl"`.
2. **Documentation Updates**:
   - Update `README.md` and `README.ko.md` to reflect dynamic metadata transition tracking (`metadata.jsonl`).
   - Update `AGENTS.md` (Section 1 and 3.4) to replace `title_history.txt` descriptions with `metadata.jsonl`.
3. **Integration Test Migrations**:
   - Update `tests/test_engine_events.rs`:
     - Rename and update `test_engine_orchestrator_stream_title_change_updates_title_history_file` to verify `metadata.jsonl`.
     - Rename and update `test_engine_orchestrator_stream_title_change_uploads_title_history_text` to verify `metadata.jsonl` events, `stream_offset_ms`, and delta fields.
     - Add new test `test_engine_orchestrator_stream_category_and_watch_party_metadata_transition`.

---

## 7. Quality Assurance & Test Plan

1. **Unit Tests (`tests/test_metadata_events.rs`)**:
   - `test_metadata_delta_computation_title_change`: Verifies `compute_delta` detects title changes and ignores unchanged fields.
   - `test_metadata_delta_computation_category_and_tags`: Verifies tag array additions/removals and category switching.
   - `test_metadata_delta_computation_watch_party_activation`: Verifies transition from inactive to active watch party with ID and tag.
   - `test_metadata_access_tier_resolution`: Verifies precedence resolution: PPV > Member-Only > Cheat Key > Adult > Public.
   - `test_metadata_jsonl_serialization_roundtrip`: Verifies `MetadataEvent` serializes to and from valid JSON Lines.
2. **Integration Tests (`tests/test_engine_events.rs`)**:
   - `test_engine_orchestrator_stream_metadata_change_updates_metadata_jsonl`: Mock Chzzk server with 2 poll steps: initial stream state followed by title, category, and watch party update. Verifies `MockUploadBackend.texts` receives `metadata.jsonl` containing both `INITIAL_STATE` and `METADATA_CHANGED` events.
   - `test_engine_orchestrator_stream_offset_ms_monotonically_increases`: Verifies subsequent events have non-decreasing `stream_offset_ms`.
3. **Linter & Formatting**:
   - `cargo check --all-targets`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo fmt --check`
   - `cargo test`
