# Stream Metadata Event Tracking (`metadata.jsonl`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Completely replace `title_history.txt` with an extensible, append-only JSON Lines stream (`metadata.jsonl`) tracking all broadcast state transitions with millisecond-accurate video synchronization.

**Architecture:** Introduce `models_metadata.rs` providing normalized metadata snapshots (`StreamMetadataState`), delta diffing (`MetadataDelta`), and event envelopes (`MetadataEvent`) with microsecond-accurate elapsed recording offsets (`stream_offset_ms`). Integrate with `ChzzkClient::get_live_detail` to parse all production Chzzk live fields, and modify `EngineOrchestrator` to detect transitions on each poll, append events locally to `<session_dir>/metadata.jsonl`, and stream updates to remote cloud storage via `rclone rcat` (`backend.upload_text`).

**Tech Stack:** Rust 2024 edition, Tokio, Serde / Serde JSON, Chrono, Reqwest.

**Spec:** [`docs/superpowers/specs/2026-09-30-stream-metadata-tracking-design.md`](file:///C:/Users/official/Documents/Code/chzzk-load/docs/superpowers/specs/2026-09-30-stream-metadata-tracking-design.md)

## Global Constraints

- **No C / FFI Dependencies**: Must not introduce SQLite or C dependencies. Keep the binary 100% pure Rust to preserve static musl cross-compilation with `cargo-zigbuild` on Linux x86_64 and ARM64.
- **Zero Allocations for Telemetry Churn**: Discrete state transitions trigger `METADATA_CHANGED` events; high-frequency telemetry churn (`concurrent_user_count`, `accumulate_count`) is embedded in state snapshots but must never trigger events on its own.
- **Flash Longevity & Bounded Footprint**: Never write temporary disk files for cloud synchronization. Stream updates directly via in-memory strings with `rclone rcat` (`UploadBackend::upload_text`).
- **Complete Deprecation**: No backward compatibility for `title_history.txt`. Remove all references from code, cloud upload calls, tests, and documentation.
- **Modern Rust 2024 Idioms**: Use inline string interpolation (`format!("{channel_id}")`), avoid `mod.rs`, maintain non-blocking mutex scoping in async loops, and preserve all `AGENTS.md` invariants.

## Review Focus

1. **Sub-second seek synchronization**: `stream_offset_ms` must be calculated from `session_start_instant.elapsed().as_millis() as u64`, ensuring monotonic non-decreasing timing across all events.
2. **Precedence order in access tier resolution**: PayPerView > ChannelSubscription > NaverPlus > CheatKey > AdultOnly > Public must be strictly enforced.
3. **Flexible Deserialization of Union Types**: `drops_campaign_no`, `live_id`, and `watch_party_no` must handle string, integer, or null without deserialization errors.
4. **Resilient Local & Cloud Dual-Write**: If cloud backend is unconfigured (local-only mode), `metadata.jsonl` must still be created and appended locally without crashing or erroring.
5. **Session EOF Flush Completeness**: Upon natural stream completion or cancellation, the final state with `close_date` must be sealed and uploaded.

---

### Task 1: Metadata Models & Delta Diffing Engine

**Files:**
- Create: `src/chzzk/models_metadata.rs`
- Modify: `src/chzzk.rs:1-30`
- Test: `tests/test_metadata_events.rs`

**Interfaces:**
- Produces:
  - `StreamAccessTier`: `Public`, `AdultOnly`, `CheatKey`, `ChannelSubscription`, `NaverPlus`, `PayPerView`
  - `CategoryType`: `Game`, `Sports`, `Etc`, `Talk`, `Unknown`
  - `WatchPartyState`: `is_active`, `no`, `tag`, `party_type`, `paid_product_id`
  - `BroadcastPolicies`: `kr_only_viewing`, `playable_status`, `blind_type`, `time_machine_active`, `clip_active`, `tv_app_viewing_policy_type`
  - `ChatRulesState`: `chat_active`, `chat_available_group`, `chat_available_condition`, `min_follower_minute`, `allow_subscriber_in_follower_mode`, `chat_slow_mode_sec`, `chat_emoji_mode`, `chat_donation_ranking_exposure`
  - `StreamMetadataState`: normalized snapshot struct with `compute_delta(&self, new: &Self) -> Option<MetadataDelta>`
  - `MetadataDelta`: diff struct with `is_empty(&self) -> bool`
  - `MetadataEvent`: JSON line container with `stream_offset_ms`, `changes`, `state`

- [ ] **Step 1: Write the failing unit tests for metadata models and delta computation**

Create `tests/test_metadata_events.rs`:
```rust
use chzzk_load::chzzk::models_metadata::{
    BroadcastPolicies, CategoryType, ChatRulesState, MetadataDelta, MetadataEvent,
    MetadataEventType, StreamAccessTier, StreamMetadataState, WatchPartyState,
};

fn sample_metadata_state() -> StreamMetadataState {
    StreamMetadataState {
        live_id: Some(21378610),
        open_date: Some("2026-09-30 14:00:00".to_string()),
        close_date: None,
        channel_id: "chan_123".to_string(),
        channel_name: "TestStreamer".to_string(),
        channel_image_url: Some("https://test.com/pfp.png".to_string()),
        verified_mark: true,
        live_title: "Initial Title".to_string(),
        category_type: Some(CategoryType::Talk),
        live_category: Some("talk".to_string()),
        live_category_value: Some("Just Chatting".to_string()),
        tags: vec!["소통".to_string()],
        access_tier: StreamAccessTier::Public,
        policies: BroadcastPolicies {
            kr_only_viewing: false,
            playable_status: Some("PLAYABLE".to_string()),
            blind_type: None,
            time_machine_active: true,
            clip_active: true,
            tv_app_viewing_policy_type: None,
        },
        watch_party: WatchPartyState::default(),
        chat_rules: ChatRulesState {
            chat_active: true,
            chat_available_group: Some("ALL".to_string()),
            chat_available_condition: Some("NONE".to_string()),
            min_follower_minute: Some(0),
            allow_subscriber_in_follower_mode: false,
            chat_slow_mode_sec: Some(0),
            chat_emoji_mode: false,
            chat_donation_ranking_exposure: true,
        },
        paid_promotion: false,
        drops_campaign_no: None,
        log_power_active: false,
        live_thumbnail_image_url: Some("https://test.com/thumb.jpg".to_string()),
        default_thumbnail_image_url: None,
        concurrent_user_count: Some(100),
        accumulate_count: Some(500),
    }
}

#[test]
fn test_metadata_delta_computation_no_change() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    // Changing only telemetry should NOT produce a delta
    state2.concurrent_user_count = Some(200);
    state2.accumulate_count = Some(600);
    state2.live_thumbnail_image_url = Some("https://test.com/thumb2.jpg".to_string());

    assert!(state1.compute_delta(&state2).is_none());
}

#[test]
fn test_metadata_delta_computation_title_and_category_change() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    state2.live_title = "New Game Broadcast".to_string();
    state2.category_type = Some(CategoryType::Game);
    state2.live_category_value = Some("Valorant".to_string());

    let delta = state1.compute_delta(&state2).expect("delta must exist");
    assert_eq!(delta.live_title.unwrap().new, "New Game Broadcast");
    assert_eq!(delta.category_type.unwrap().new, Some(CategoryType::Game));
    assert_eq!(delta.live_category_value.unwrap().new, Some("Valorant".to_string()));
    assert!(delta.watch_party.is_none());
}

#[test]
fn test_metadata_delta_computation_watch_party_and_drops() {
    let state1 = sample_metadata_state();
    let mut state2 = state1.clone();
    state2.watch_party = WatchPartyState {
        is_active: true,
        no: Some(520),
        tag: Some("2026아시안게임".to_string()),
        party_type: Some("RS".to_string()),
        paid_product_id: None,
    };
    state2.drops_campaign_no = Some("camp_val_99".to_string());
    state2.policies.kr_only_viewing = true;

    let delta = state1.compute_delta(&state2).expect("delta must exist");
    assert_eq!(delta.watch_party.unwrap().new.no, Some(520));
    assert_eq!(delta.drops_campaign_no.unwrap().new, Some("camp_val_99".to_string()));
    assert!(delta.policies.unwrap().new.kr_only_viewing);
}

#[test]
fn test_metadata_jsonl_serialization_roundtrip() {
    let state = sample_metadata_state();
    let event = MetadataEvent {
        version: 1,
        event: MetadataEventType::InitialState,
        timestamp: "2026-09-30T05:00:00Z".to_string(),
        time_local: "2026-09-30 14:00:00".to_string(),
        stream_offset_ms: 0,
        changes: None,
        state,
    };

    let serialized = serde_json::to_string(&event).unwrap();
    let deserialized: MetadataEvent = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized.version, 1);
    assert_eq!(deserialized.stream_offset_ms, 0);
    assert_eq!(deserialized.state.channel_name, "TestStreamer");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_metadata_events`
Expected: FAIL (cannot find module `models_metadata`)

- [ ] **Step 3: Implement `src/chzzk/models_metadata.rs` and re-export in `src/chzzk.rs`**

Create `src/chzzk/models_metadata.rs` implementing all structs, enums, `FieldDiff`, `MetadataDelta`, and `compute_delta`. Add custom deserializer `deserialize_optional_string_or_number` for `drops_campaign_no` and IDs.
In `src/chzzk.rs`:
```rust
pub mod models_metadata;
pub use models_metadata::*;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_metadata_events`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/chzzk/models_metadata.rs src/chzzk.rs tests/test_metadata_events.rs
git commit -m "feat(chzzk): implement metadata models and delta computation engine"
```

---

### Task 2: Chzzk API Model Expansion & Access Tier Resolution

**Files:**
- Modify: `src/chzzk/models.rs:20-80, 120-150`
- Modify: `src/chzzk/client.rs:160-210, 320-365`
- Test: `tests/test_chzzk_client.rs`

**Interfaces:**
- Consumes: `StreamMetadataState`, `StreamAccessTier`, `CategoryType`, `WatchPartyState`, `BroadcastPolicies`, `ChatRulesState` from Task 1.
- Produces:
  - `resolve_access_tier(content: &LiveDetailContent, playback_meta: Option<&PlaybackMeta>) -> StreamAccessTier`
  - `LiveStreamInfo.metadata: StreamMetadataState`
  - Expanded `LiveDetailContent` deserializing all 2026 Chzzk fields.

- [ ] **Step 1: Write unit tests for `resolve_access_tier` and `LiveDetailContent` deserialization**

In `tests/test_chzzk_client.rs`, add:
```rust
#[test]
fn test_resolve_access_tier_precedence() {
    use chzzk_load::chzzk::client::resolve_access_tier;
    use chzzk_load::chzzk::models::{LiveDetailContent, PlaybackMeta, ChannelInfo};
    use chzzk_load::chzzk::models_metadata::StreamAccessTier;

    let base_content = LiveDetailContent {
        live_id: Some(123),
        status: "OPEN".to_string(),
        live_title: Some("Title".to_string()),
        channel: ChannelInfo { channel_id: "c1".to_string(), channel_name: "Name".to_string(), verified_mark: Some(true) },
        live_playback_json: None,
        chat_channel_id: None,
        adult: Some(false),
        open_date: None,
        close_date: None,
        category_type: None,
        live_category: None,
        live_category_value: None,
        tags: vec![],
        paid_promotion: None,
        drops_campaign_no: None,
        kr_only_viewing: None,
        clip_active: None,
        time_machine_active: None,
        chat_active: None,
        chat_available_group: None,
        chat_available_condition: None,
        min_follower_minute: None,
        allow_subscriber_in_follower_mode: None,
        chat_slow_mode_sec: None,
        chat_emoji_mode: None,
        chat_donation_ranking_exposure: None,
        live_image_url: None,
        default_thumbnail_image_url: None,
        concurrent_user_count: None,
        accumulate_count: None,
        watch_party_no: None,
        watch_party_tag: None,
        watch_party_type: None,
        watch_party_paid_product_id: None,
        paid_product: None,
        live_polling_status_json: None,
        user_adult_status: None,
        membership_benefit_type: None,
        tv_app_viewing_policy_type: None,
        blind_type: None,
        log_power_active: None,
    };

    // Public
    assert_eq!(resolve_access_tier(&base_content, None), StreamAccessTier::Public);

    // AdultOnly
    let mut adult_content = base_content.clone();
    adult_content.adult = Some(true);
    assert_eq!(resolve_access_tier(&adult_content, None), StreamAccessTier::AdultOnly);

    // CheatKey beats AdultOnly
    let cheat_meta = PlaybackMeta {
        video_id: None, stream_seq: None, live_id: None, paid_live: None,
        playback_auth_type: Some("CHZZK_CHEAT_KEY".to_string()),
    };
    assert_eq!(resolve_access_tier(&adult_content, Some(&cheat_meta)), StreamAccessTier::CheatKey);

    // ChannelSubscription beats CheatKey
    let mut sub_content = adult_content.clone();
    sub_content.membership_benefit_type = Some("MEMBER_ONLY".to_string());
    assert_eq!(resolve_access_tier(&sub_content, Some(&cheat_meta)), StreamAccessTier::ChannelSubscription);

    // PayPerView beats ChannelSubscription
    let mut ppv_content = sub_content.clone();
    ppv_content.paid_product = Some(serde_json::json!({"sku": 1}));
    assert_eq!(resolve_access_tier(&ppv_content, Some(&cheat_meta)), StreamAccessTier::PayPerView);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_chzzk_client test_resolve_access_tier_precedence`
Expected: FAIL (function `resolve_access_tier` not found)

- [ ] **Step 3: Implement model expansion in `src/chzzk/models.rs` and resolution logic in `src/chzzk/client.rs`**

1. Expand `ChannelInfo` with `pub verified_mark: Option<bool>`.
2. Expand `LiveDetailContent` with all newly audited fields.
3. Update `LiveStreamInfo` to carry `pub metadata: StreamMetadataState`.
4. Implement `pub fn resolve_access_tier(content: &LiveDetailContent, playback_meta: Option<&PlaybackMeta>) -> StreamAccessTier`.
5. In `ChzzkClient::get_live_detail`, construct `StreamMetadataState` and populate `LiveStreamInfo { ..., metadata }`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_chzzk_client`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/chzzk/models.rs src/chzzk/client.rs tests/test_chzzk_client.rs
git commit -m "feat(chzzk): expand live detail models and implement access tier resolution"
```

---

### Task 3: Engine Session State & Metadata History

**Files:**
- Modify: `src/engine.rs:25-105`
- Test: `tests/test_engine_events.rs`

**Interfaces:**
- Consumes: `StreamMetadataState`, `MetadataDelta`, `MetadataEvent` from Task 1.
- Produces:
  - `ActiveSessionState::new(start_timestamp, streamer_name, alias, initial_metadata)`
  - `ActiveSessionState::record_metadata_change(&mut self, new_metadata) -> Option<(MetadataDelta, MetadataEvent)>`
  - `ActiveSessionState::format_metadata_jsonl(&self) -> String`
  - Completely drops `title_history`, `record_title_change`, and `format_title_history`.

- [ ] **Step 1: Write unit tests for `ActiveSessionState` metadata history and formatting**

In `src/engine.rs` tests (or `tests/test_engine_events.rs`):
```rust
#[test]
fn test_active_session_state_metadata_jsonl_formatting() {
    let initial_meta = sample_test_metadata("Initial Title");
    let mut session = ActiveSessionState::new(
        "2026-09-30_140000".to_string(),
        "TestStreamer".to_string(),
        Some("Alias".to_string()),
        initial_meta.clone(),
    );

    assert_eq!(session.metadata_history.len(), 1);
    assert_eq!(session.metadata_history[0].event, MetadataEventType::InitialState);
    assert_eq!(session.metadata_history[0].stream_offset_ms, 0);

    let mut updated_meta = initial_meta.clone();
    updated_meta.live_title = "Second Title".to_string();
    let change = session.record_metadata_change(updated_meta);
    assert!(change.is_some());
    assert_eq!(session.metadata_history.len(), 2);
    assert_eq!(session.metadata_history[1].event, MetadataEventType::MetadataChanged);

    let jsonl = session.format_metadata_jsonl();
    let lines: Vec<&str> = jsonl.trim().lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"INITIAL_STATE\""));
    assert!(lines[1].contains("\"METADATA_CHANGED\""));
    assert!(lines[1].contains("\"Second Title\""));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib test_active_session_state_metadata_jsonl_formatting`
Expected: FAIL

- [ ] **Step 3: Implement `ActiveSessionState` with `metadata_history` and `format_metadata_jsonl`**

Replace `title_history` with `metadata_history: Vec<MetadataEvent>`, `session_start_instant: std::time::Instant`, and `current_metadata: StreamMetadataState`. Implement `record_metadata_change` and `format_metadata_jsonl`. Remove `record_title_change` and `format_title_history`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib test_active_session_state_metadata_jsonl_formatting`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): transition ActiveSessionState to metadata_history and jsonl formatting"
```

---

### Task 4: Recording Session Lifecycle & Polling Integration

**Files:**
- Modify: `src/engine.rs:610-650, 1240-1275, 1450-1515`

**Interfaces:**
- Consumes: `ActiveSessionState` methods from Task 3, `UploadBackend::upload_text`.
- Produces:
  - Startup emission of initial `metadata.jsonl` (local file + cloud upload).
  - Polling loop delta detection: appends event to `<session_dir>/metadata.jsonl` and uploads updated `metadata.jsonl` via `upload_text`.
  - Stream completion: final flush of `metadata.jsonl` to cloud storage.

- [ ] **Step 1: Write integration tests for startup, transition, and EOF metadata uploads**

In `tests/test_engine_events.rs`, update existing title tests to test `metadata.jsonl`:
- Test that initial startup creates local `<session_dir>/metadata.jsonl` and uploads `metadata.jsonl` to `MockUploadBackend`.
- Test that mid-stream category/title/watch-party change triggers `backend.upload_text("metadata.jsonl", ...)` containing both events.
- Test that `AppEvent::Log` emits rec log with summary of change.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --test test_engine_events test_engine_orchestrator_stream_metadata`
Expected: FAIL

- [ ] **Step 3: Update `src/engine.rs` recording session and polling loop**

1. In `spawn_recording_session` startup:
   - Initialize `ActiveSessionState::new(..., info.metadata.clone())`.
   - Write initial event to `session_dir.join("metadata.jsonl")`.
   - If backend is active: `backend.upload_text(&session_folder_name, "metadata.jsonl", &initial_jsonl).await`.
2. In `spawn_recording_session` shutdown (clean exit & cancellation):
   - Flush latest `metadata.jsonl` to remote via `backend.upload_text(&session_folder_name, "metadata.jsonl", &final_jsonl).await`.
3. In `poll_channels_once`:
   - When `is_recording`: call `session.record_metadata_change(info.metadata.clone())`.
   - If changed:
     - Append line to local `session_dir.join("metadata.jsonl")`.
     - If backend active: `backend.upload_text(&remote_dir, "metadata.jsonl", &full_jsonl).await`.
     - Log transition via `AppEvent::Log(LogEntry::rec(...))`.

- [ ] **Step 4: Run integration tests to verify they pass**

Run: `cargo test --test test_engine_events`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs tests/test_engine_events.rs
git commit -m "feat(engine): integrate metadata.jsonl tracking into recording lifecycle and polling loop"
```

---

### Task 5: Integration Tests & Backend Migration

**Files:**
- Modify: `src/uploader/backend.rs`
- Modify: `tests/test_rclone_backend.rs`
- Modify: `tests/test_engine_events.rs`
- Modify: `examples/preview.rs`

**Interfaces:**
- Removes all remaining references to `title_history.txt`.
- Verifies full test suite passes with zero warnings.

- [ ] **Step 1: Search and replace all remaining references to `title_history.txt` across the codebase**

Run: `git grep -n "title_history"`
Update `src/uploader/backend.rs`, `tests/test_rclone_backend.rs`, `examples/preview.rs`, and remaining tests in `tests/test_engine_events.rs` to refer to `metadata.jsonl`.

- [ ] **Step 2: Run full cargo test suite**

Run: `cargo test --all-targets`
Expected: All tests pass.

- [ ] **Step 3: Run clippy and format checks**

Run: `cargo clippy --all-targets -- -D warnings`
Run: `cargo fmt --check`
Expected: 0 warnings, 0 format issues.

- [ ] **Step 4: Commit**

```bash
git add src/ tests/ examples/
git commit -m "refactor(tests): migrate backend and integration tests from title_history to metadata.jsonl"
```

---

### Task 6: Documentation & Agent Guide Migration

**Files:**
- Modify: `README.md`
- Modify: `README.ko.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update documentation to describe real-time stream metadata tracking (`metadata.jsonl`)**

1. In `README.md` and `README.ko.md`:
   - Replace title history feature bullets with metadata event tracking (`metadata.jsonl` tracking titles, categories, tags, access tiers, watch parties, policies, and chat rules with sub-second VOD replay sync).
2. In `AGENTS.md`:
   - Section 1 (Project Overview) & Section 3.4 (Cloud Storage Upload Pipeline & Metadata Sync): Update architecture descriptions to document `metadata.jsonl` and remove `title_history.txt`.
   - Update file tree diagram in Section 2.

- [ ] **Step 2: Verify git status and check for broken doc links or outdated comments**

Run: `git diff README.md README.ko.md AGENTS.md`
Run: `cargo test --all-targets`
Run: `node scripts/test-npm-packages.js`

- [ ] **Step 3: Commit**

```bash
git add README.md README.ko.md AGENTS.md
git commit -m "docs: document stream metadata tracking and update architecture invariants for metadata.jsonl"
```
