# Rclone Cloud Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `chzzk-load` to use `rclone` for cloud uploads, replacing direct Google Drive API/OAuth integration with a modular `UploadBackend` trait and CLI-based `RcloneBackend`.

**Architecture:** Define an asynchronous `UploadBackend` trait and implement `RcloneBackend` which spawns `rclone copyto`/`rcat` subprocesses, streams real-time JSON log stats to the Ratatui TUI, and deletes local files strictly upon exit code 0 confirmation. The engine orchestrator uses the trait, supported by an in-memory `MockUploadBackend` for fast, deterministic unit and integration tests.

**Tech Stack:** Rust 2024 edition, Tokio, `tokio::process::Command`, Ratatui, Crossterm, Serde, Serde JSON.

**Spec:** `docs/superpowers/specs/2026-09-29-rclone-cloud-storage-design.md`

## Global Constraints
- Rust 2024 edition idioms: inlined format args, module layout `foo.rs` + `foo/`, zero clippy warnings.
- Bounded disk footprint: `.ts` segments and `chat.jsonl` are deleted locally ONLY upon exit code 0 upload confirmation.
- Subprocess isolation: `stdin`, `stdout`, `stderr` must be redirected (`null` or `piped`), never leaking into terminal raw mode.
- Non-breaking local-only mode: When `remote_path` is empty (`""`), recording continues locally without errors.
- Test determinism: CI and integration tests must run without requiring an external `rclone` binary or network access.

## Review Focus
1. Nonexistent or invalid rclone binary: Handled gracefully on startup or upload failure with clear error log, without panicking or hanging.
2. Trailing slash inconsistencies in `remote_path`: Sanitized properly so paths like `gdrive:path/` or `gdrive:path` do not produce double slashes `//`.
3. Partial/failed rclone transfers: Local file is preserved and not deleted when rclone exits with non-zero status.
4. rclone JSON log parser on unexpected stderr output: Non-JSON or warning stderr lines must be handled without parser panic or dropped error messages.
5. Title changes mid-broadcast: Uploads updated `title_history.txt` to the existing session directory without renaming the remote directory.

---

### Task 1: Configuration & Dependencies

**Files:**
- Modify: `Cargo.toml:21`
- Modify: `src/config.rs:60-138`
- Modify: `tests/test_config.rs:1-120`

**Interfaces:**
- Produces: `RcloneConfig { remote_path: String, upload_concurrency: usize, rclone_bin: String, extra_args: Vec<String> }` in `src/config.rs`
- Produces: `Settings.rclone: RcloneConfig` replacing `Settings.google_drive`

- [ ] **Step 1: Write failing tests in `tests/test_config.rs` for `RcloneConfig`**

Add tests for:
1. `test_default_rclone_config`: Verifies defaults (`remote_path == "remote:chzzk"`, `upload_concurrency == 3`, `rclone_bin == "rclone"`, `extra_args.is_empty()`).
2. `test_rclone_config_custom_deserialization`: Deserializes custom JSON with `remote_path`, `upload_concurrency`, `rclone_bin`, and `extra_args`.
3. `test_rclone_local_only_mode`: Verifies deserialization when `remote_path` is empty `""`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test test_config test_default_rclone_config`
Expected: FAIL due to missing `RcloneConfig` and missing field `rclone` on `Settings`.

- [ ] **Step 3: Implement `RcloneConfig` and update `Settings` in `src/config.rs` & update `Cargo.toml`**

1. In `Cargo.toml`: Remove `tiny_http = "0.12"`.
2. In `src/config.rs`:
   - Replace `GoogleDriveConfig` with `RcloneConfig`:
     ```rust
     #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
     pub struct RcloneConfig {
         #[serde(default = "default_remote_path")]
         pub remote_path: String,
         #[serde(default = "default_upload_concurrency")]
         pub upload_concurrency: usize,
         #[serde(default = "default_rclone_bin")]
         pub rclone_bin: String,
         #[serde(default)]
         pub extra_args: Vec<String>,
     }
     ```
   - Update `Settings` struct: Replace `pub google_drive: GoogleDriveConfig` with `pub rclone: RcloneConfig`.
   - Update `Settings::default()` to use `RcloneConfig::default()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_config`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add Cargo.toml src/config.rs tests/test_config.rs
git commit -m "feat(config): replace GoogleDriveConfig with RcloneConfig"
```

---

### Task 2: Storage Abstraction: `UploadBackend` Trait & `MockUploadBackend`

**Files:**
- Create: `src/uploader/backend.rs`
- Modify: `src/uploader.rs:1-50`

**Interfaces:**
- Produces: `UploadBackend` trait with `upload_file_and_delete`, `upload_text`, `check_connection` in `src/uploader/backend.rs`
- Produces: `MockUploadBackend` in `src/uploader/backend.rs` for deterministic testing
- Produces: Updated `UploadTask { channel_id: String, remote_dir: String, chunk_path: PathBuf, chunk_name: String, streamer_name: String }` in `src/uploader.rs`

- [ ] **Step 1: Write unit tests for `MockUploadBackend`**

In `src/uploader/backend.rs` (under `#[cfg(test)]`):
1. `test_mock_backend_upload_and_delete`: Creates a temp file, calls `mock.upload_file_and_delete(...)`, verifies local file is removed, callback receives byte count, and record is saved in `uploads`.
2. `test_mock_backend_upload_text`: Calls `mock.upload_text(...)`, verifies text entry recorded in `texts`.
3. `test_mock_backend_simulated_error`: Sets `should_fail = true`, verifies `upload_file_and_delete` returns error and leaves local file intact.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --lib uploader::backend`
Expected: FAIL with module not found.

- [ ] **Step 3: Implement `UploadBackend` trait and `MockUploadBackend`**

In `src/uploader/backend.rs`:
```rust
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ProgressCallback = Box<dyn Fn(u64, u64, f64) + Send + Sync + 'static>;

pub trait UploadBackend: Send + Sync {
    fn upload_file_and_delete<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;

    fn upload_text<'a>(
        &'a self,
        remote_dir: &'a str,
        file_name: &'a str,
        content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>>;

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>>;
}

#[derive(Default, Clone)]
pub struct MockUploadBackend {
    pub uploads: Arc<Mutex<Vec<(PathBuf, String)>>>,
    pub texts: Arc<Mutex<Vec<(String, String, String)>>>,
    pub should_fail: Arc<AtomicBool>,
}

impl UploadBackend for MockUploadBackend {
    // Implement mock behavior: if should_fail -> Err, else record + remove local file + Ok(len)
}
```

In `src/uploader.rs`:
- Remove `crate::drive::client::DriveClient`.
- Update `UploadTask`: replace `session_folder_id: String` with `remote_dir: String`.
- Re-export `backend::{BoxFuture, ProgressCallback, UploadBackend, MockUploadBackend}`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib uploader::backend`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/uploader/backend.rs src/uploader.rs
git commit -m "feat(uploader): introduce UploadBackend trait and MockUploadBackend"
```

---

### Task 3: `RcloneBackend` CLI Subprocess & JSON Log Parser

**Files:**
- Create: `src/uploader/rclone.rs`
- Modify: `src/uploader.rs`
- Create: `tests/test_rclone_backend.rs`

**Interfaces:**
- Consumes: `UploadBackend` from `src/uploader/backend.rs`
- Produces: `RcloneBackend::new(config: RcloneConfig) -> Self`
- Produces: `RcloneBackend::with_bin(mut self, bin: impl Into<String>) -> Self`
- Produces: `format_destination(remote_path: &str, remote_dir: &str, file_name: &str) -> String`
- Produces: `parse_rclone_log_line(line: &str) -> Option<RcloneStats>`

- [ ] **Step 1: Write unit tests in `tests/test_rclone_backend.rs`**

1. `test_format_destination_standard`: `format_destination("gdrive:Chzzk", "session_1", "chunk_0000.ts")` == `"gdrive:Chzzk/session_1/chunk_0000.ts"`.
2. `test_format_destination_trailing_slashes`: Handles `gdrive:Chzzk/` and `session_1/` without producing `//`.
3. `test_format_destination_empty_remote_dir`: If `remote_dir == ""` -> `"gdrive:Chzzk/chunk_0000.ts"`.
4. `test_parse_rclone_log_progress`: Parses JSON containing `"stats": {"bytes": 5242880, "totalBytes": 10485760, "speed": 1048576.0}`.
5. `test_parse_rclone_log_non_json_or_notice`: Handles non-JSON text lines and plain log lines gracefully without panicking, returning `None`.
6. `test_rclone_backend_command_builder`: Validates that the constructed `copyto` command includes `--use-json-log`, `--stats 250ms`, `--stats-log-level NOTICE`, and `extra_args`.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test --test test_rclone_backend`
Expected: FAIL due to missing `RcloneBackend` and helpers.

- [ ] **Step 3: Implement `RcloneBackend` and parser in `src/uploader/rclone.rs`**

1. Implement `format_destination`.
2. Implement JSON log types `RcloneLogEntry`, `RcloneStats` and `parse_rclone_log_line`.
3. Implement `RcloneBackend`:
   - `resolve_bin(&self) -> String`: Checks `std::env::var("CHZZK_LOAD_RCLONE_BIN")`, else `config.rclone_bin`.
   - `UploadBackend::upload_file_and_delete`:
     Spawns `rclone copyto <local> <dest> --use-json-log --stats 250ms --stats-log-level NOTICE <extra_args...>` with `stdin(Stdio::null())`, `stdout(Stdio::null())`, `stderr(Stdio::piped())`.
     Asynchronously streams `stderr` with `BufReader`, parses progress events, triggers `on_progress(bytes, total_bytes, speed_mb_s)`.
     On exit status 0: `tokio::fs::remove_file(local_path).await` and returns `Ok(size)`.
     On exit non-zero: returns `anyhow::anyhow!("rclone failed with status {status}")`.
   - `UploadBackend::upload_text`:
     Spawns `rclone rcat <dest> <extra_args...>` with `stdin(Stdio::piped())`.
     Writes text to stdin, flushes, closes, verifies exit status 0.
   - `UploadBackend::check_connection`:
     Spawns `rclone lsf --max-depth 1 <remote_path> <extra_args...>`.
     Returns `Ok(())` on status 0, or captures stderr on failure.
4. Re-export `RcloneBackend` in `src/uploader.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_rclone_backend`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/uploader/rclone.rs src/uploader.rs tests/test_rclone_backend.rs
git commit -m "feat(uploader): implement RcloneBackend with progress parsing and text upload"
```

---

### Task 4: Engine Orchestrator Refactor

**Files:**
- Modify: `src/engine.rs:1-1750`

**Interfaces:**
- Consumes: `UploadBackend` and `UploadTask` from `src/uploader.rs`
- Produces: Updated `ActiveSessionState` with `initial_title` and stable `folder_name()`
- Produces: `EngineOrchestrator::new(settings, chzzk, backend: Option<Arc<dyn UploadBackend>>, event_tx)`

- [ ] **Step 1: Write a focused unit test in `src/engine.rs` for `ActiveSessionState`**

Add tests for:
1. `test_active_session_state_stable_folder_name`: Verifies `folder_name()` uses `initial_title` even after `record_title_change(new_title, timestamp)`.
2. `test_active_session_state_title_history_formatting`: Verifies `format_title_history()` formats all title changes chronologically.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib engine::tests::test_active_session_state_stable_folder_name`
Expected: FAIL due to missing `initial_title` in `ActiveSessionState`.

- [ ] **Step 3: Refactor `src/engine.rs` to use `UploadBackend`**

1. Replace `drive: Option<DriveClient>` with `backend: Option<Arc<dyn UploadBackend>>` in `EngineOrchestrator`.
2. Refactor `ActiveSessionState`:
   - Store `initial_title` and `current_title`.
   - Remove `session_folder_id` and `title_history_file_id`.
   - `folder_name(&self)` formats `[{start_timestamp}] {sanitized_streamer} - {sanitized_initial_title}`.
3. Update `spawn_upload_consumer_with_concurrency`:
   - Accept `backend_opt: Option<Arc<dyn UploadBackend>>`.
   - In worker task: Call `backend.upload_file_and_delete(&chunk_path, &remote_dir, progress_cb).await`.
   - On success: emit `AppEvent::UploadCompleted`, log reclaimed size, clean up empty local session directory.
   - On error: emit `AppEvent::UploadFailed`, log error message.
4. Update `process_sealed_chunk` and `seal_and_enqueue_chunks`:
   - Enqueue `UploadTask` with `remote_dir: session_folder_name`.
5. Update `spawn_recording_session`:
   - Remove Google Drive folder creation calls (`ensure_session_folder`).
   - Drain sealed chunks directly to `upload_tx` with `remote_dir = session_folder_name`.
   - On session end: upload `chat.jsonl` using `backend.upload_file_and_delete(&chat_path, &remote_dir, ...)` and delete locally.
   - Upload final `title_history.txt` via `backend.upload_text(&remote_dir, "title_history.txt", &history)`.
6. Update title change handling in `poll_channels_once`:
   - Call `backend.upload_text(&remote_dir, "title_history.txt", &history)`.
   - Log `[CLOUD] Updated 'title_history.txt' for {channel_id}`.

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `cargo test --lib engine`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add src/engine.rs
git commit -m "refactor(engine): migrate orchestrator from DriveClient to UploadBackend"
```

---

### Task 5: TUI Badges, CLI Entrypoint & Legacy Cleanup

**Files:**
- Modify: `src/tui/event.rs:1-100`
- Modify: `src/tui/ui.rs:1-30`
- Modify: `src/main.rs:1-120`
- Delete: `src/drive.rs`
- Delete: `src/drive/auth.rs`
- Delete: `src/drive/client.rs`
- Delete: `tests/test_drive_auth.rs`
- Delete: `tests/test_drive_uploader.rs`

**Interfaces:**
- Produces: `LogKind::Cloud` with badge `" CLOUD  "`
- Produces: `LogEntry::cloud(msg)`

- [ ] **Step 1: Write test for `LogKind::Cloud` badge formatting**

In `src/tui/event.rs`:
Verify `LogKind::Cloud.as_str() == "CLOUD"` and `LogEntry::cloud("test message")` formats as `[CLOUD] test message`.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test --lib tui::event`
Expected: FAIL due to missing `LogKind::Cloud`.

- [ ] **Step 3: Update TUI event & UI, main entrypoint, and delete legacy drive files**

1. In `src/tui/event.rs`:
   - Replace `LogKind::Drive` with `LogKind::Cloud`.
   - Replace `LogEntry::drive` with `LogEntry::cloud`.
2. In `src/tui/ui.rs`:
   - Update `log_kind_badge_and_style`: `LogKind::Cloud => (" CLOUD  ", Style::default().fg(theme::BLUE))`.
3. In `src/main.rs`:
   - Remove Google Drive imports (`DriveAuth`, `DriveClient`).
   - Import `RcloneBackend` and `UploadBackend`.
   - If `!settings.rclone.remote_path.is_empty()`:
     - Instantiate `backend = Arc::new(RcloneBackend::new(settings.rclone.clone()))`.
     - Perform `backend.check_connection().await`: on success log `[CLOUD] Rclone remote '{remote_path}' verified successfully`, on error log warning.
     - Pass `Some(backend)` to `EngineOrchestrator`.
   - Else: log `[CLOUD] 'remote_path' is empty; running in local-only recording mode`, pass `None`.
4. Delete legacy files:
   - `src/drive.rs`, `src/drive/auth.rs`, `src/drive/client.rs`.
   - `tests/test_drive_auth.rs`, `tests/test_drive_uploader.rs`.
5. Remove `pub mod drive;` from `src/lib.rs`.

- [ ] **Step 4: Run compiler check across all targets**

Run: `cargo check --all-targets`
Expected: Only tests referencing Drive remain to be updated in Task 6.

- [ ] **Step 5: Commit changes**

```bash
git add src/tui/event.rs src/tui/ui.rs src/main.rs src/lib.rs
git rm src/drive.rs src/drive/auth.rs src/drive/client.rs tests/test_drive_auth.rs tests/test_drive_uploader.rs
git commit -m "feat(tui, main): switch to LogKind::Cloud, initialize RcloneBackend, delete legacy drive module"
```

---

### Task 6: Migrate Engine Integration Tests

**Files:**
- Modify: `tests/test_engine_events.rs`
- Modify: `tests/test_engine_chat.rs` (if referencing drive)

**Interfaces:**
- Consumes: `MockUploadBackend` from `chzzk_load::uploader::MockUploadBackend`
- Validates: Full session recording, chunk sealing, upload consumer queuing, rate-limit retries, and chat upload with mock backend

- [ ] **Step 1: Check existing integration test failures**

Run: `cargo test --test test_engine_events`
Expected: Compiler errors where `DriveClient` or `create_mock_drive_auth` were used.

- [ ] **Step 2: Replace mock Google Drive server with `MockUploadBackend` in `tests/test_engine_events.rs`**

1. Replace `create_mock_drive_auth` and `tiny_http::Server` with `Arc::new(MockUploadBackend::default())`.
2. Update orchestrator initialization calls to pass `Some(mock_backend.clone())`.
3. Update assertions checking uploaded files: assert that `mock_backend.uploads.lock().await` contains the sealed chunk and chat logs.
4. Ensure all concurrency, race condition, cooldown, and cancellation tests pass with the mock backend.

- [ ] **Step 3: Run integration test suite**

Run: `cargo test --test test_engine_events`
Expected: PASS for all tests.

- [ ] **Step 4: Run chat integration tests**

Run: `cargo test --test test_engine_chat`
Expected: PASS

- [ ] **Step 5: Commit changes**

```bash
git add tests/test_engine_events.rs tests/test_engine_chat.rs
git commit -m "test(engine): migrate engine integration tests to MockUploadBackend"
```

---

### Task 7: Verification, Documentation & CI Validation

**Files:**
- Modify: `README.md`
- Modify: `README.ko.md`
- Modify: `AGENTS.md`
- Modify: `scripts/test-npm-packages.js` (if applicable)

- [ ] **Step 1: Update Documentation**
Update `README.md`, `README.ko.md`, and `AGENTS.md`:
- Document `rclone` requirements (rclone must be installed and configured with a remote).
- Document new `settings.json` `rclone` section (`remote_path`, `upload_concurrency`, `rclone_bin`, `extra_args`).
- Update architectural diagrams and module descriptions to reflect `UploadBackend` and `RcloneBackend`.

- [ ] **Step 2: Run linter and formatting checks**

Run: `cargo fmt --check`
Expected: PASS (0 diffs)

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS (0 warnings)

- [ ] **Step 3: Run full test suite**

Run: `cargo test --all-targets`
Expected: PASS (100% tests passing)

- [ ] **Step 4: Run npm package tests**

Run: `node scripts/test-npm-packages.js`
Expected: PASS

- [ ] **Step 5: Commit documentation and final verification**

```bash
git add README.md README.ko.md AGENTS.md
git commit -m "docs: update documentation and architectural guides for rclone"
```
