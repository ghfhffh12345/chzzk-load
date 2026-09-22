# Codebase Refactor and Multi-Agent Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor and optimize `chzzk-load` internal architecture by decoupling log kinds from message strings into structured enums, eliminating per-frame UI allocations and $O(N)$ buffer shifts, consolidating engine chunk sealing, and optimizing filesystem and track selection logic.

**Architecture:** Introduce `LogKind` and `LogEntry` types in `src/tui/event.rs` with `Display` and `From` trait implementations. Replace `Vec<String>` in `App` with an $O(1)$ ring buffer `VecDeque<LogEntry>`, and render log lines directly in `draw_ui` without per-frame string parsing. Deduplicate recording session chunk sealing and folder naming in `EngineOrchestrator`, optimize directory metadata traversal in `SegmentWatcher`, and streamline HLS track prioritization.

**Tech Stack:** Rust (edition 2024), Tokio, Ratatui, Crossterm, Reqwest.

**Spec:** `docs/superpowers/specs/2026-09-22-codebase-refactor-and-optimization-design.md`

## Global Constraints

- Standalone executable runnable without cargo (`FFmpeg` must be on `PATH`).
- Zero CPU transcoding: FFmpeg must strictly keep `-c copy` lossless stream copying.
- N+1 segment boundary safety: Chunk $N$ sealed only when chunk $N+1$ exists on disk with size $> 0$ (or upon final stream termination).
- Strictly bounded disk footprint: Local chunks are deleted immediately upon Google Drive HTTP 200/201 confirmation.
- Terminal panic recovery: Maintain panic hook restoring terminal mode on abnormal termination.
- No direct terminal pollution: No `println!` or raw stdout leaking into TUI buffer; all diagnostics must flow through `AppEvent::Log`.
- Path portability: Relative paths resolved via `app_path::resolve_path`.

---

### Task 1: Structured Logging Model (`LogKind`, `LogEntry`) & AppEvent Integration

**Files:**
- Modify: `src/tui/event.rs`
- Modify: `src/tui/mod.rs`
- Modify: `src/main.rs`
- Modify: `tests/test_engine_events.rs`
- Modify: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: Nothing
- Produces:
  ```rust
  pub enum LogKind { Info, Warn, Error, Clean, Rec, Ffmpeg, Drive, Poll }
  pub struct LogEntry { pub kind: LogKind, pub message: String }
  pub enum AppEvent { ... Log(LogEntry) }
  ```

- [ ] **Step 1: Write failing unit test for LogKind and LogEntry**

In `src/tui/event.rs`, add unit tests verifying `LogKind::as_str()`, `LogEntry::new()`, `Display` formatting (`[INFO] ...`), and `From<String>` parsing fallback:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_display_and_parsing() {
        let entry = LogEntry::clean("Uploaded & deleted chunk_0000.ts");
        assert_eq!(entry.kind, LogKind::Clean);
        assert_eq!(entry.to_string(), "[CLEAN] Uploaded & deleted chunk_0000.ts");

        let from_str: LogEntry = "[FFMPEG] frame= 100 fps=30".into();
        assert_eq!(from_str.kind, LogKind::Ffmpeg);
        assert_eq!(from_str.message, "frame= 100 fps=30");

        let unformatted: LogEntry = "Generic message".into();
        assert_eq!(unformatted.kind, LogKind::Info);
        assert_eq!(unformatted.message, "Generic message");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib tui::event::tests`
Expected: FAIL with unknown types `LogKind`, `LogEntry`.

- [ ] **Step 3: Implement LogKind and LogEntry in `src/tui/event.rs`**

Add `LogKind` enum, `LogEntry` struct, convenience constructors (`info`, `warn`, `error`, `clean`, `rec`, `ffmpeg`, `drive`, `poll`), `Display`, `From<String>`, `From<&str>`, and update `AppEvent::Log(LogEntry)`. Re-export `LogKind` and `LogEntry` in `src/tui/mod.rs`.

- [ ] **Step 4: Update existing references in `src/main.rs` and tests to use typed `LogEntry`**

Update `src/main.rs` log emission calls:
```rust
LogEntry::info("Google Drive authenticated successfully")
LogEntry::warn(format!("Drive auth failed: {}", e))
LogEntry::info(format!("'{}' not found; running in local-only recording mode", creds_path.display()))
LogEntry::info("Manual refresh triggered...")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib tui::event::tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/tui/event.rs src/tui/mod.rs src/main.rs
git commit -m "feat(tui): introduce structured LogKind and LogEntry model"
```

---

### Task 2: Chzzk Client Track Extraction Optimization

**Files:**
- Modify: `src/chzzk/client.rs:9-49`
- Test: `tests/test_chzzk_client.rs`

**Interfaces:**
- Consumes: `crate::chzzk::models::PlaybackJson`
- Produces: `pub fn extract_best_hls_url(playback_json_str: &Option<String>) -> Result<String>`

- [ ] **Step 1: Verify existing test suite for `extract_best_hls_url`**

Run: `cargo test --test test_chzzk_client`
Expected: 12 passed.

- [ ] **Step 2: Refactor `extract_best_hls_url` into a clean single-pass selection**

In `src/chzzk/client.rs`:
```rust
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

    // Prioritize 1080p, then 720p, then any video track (excluding audioOnly)
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
```

- [ ] **Step 3: Run test suite to verify behavior preserved**

Run: `cargo test --test test_chzzk_client`
Expected: PASS (all 12 tests pass).

- [ ] **Step 4: Commit**

```bash
git add src/chzzk/client.rs
git commit -m "refactor(chzzk): optimize HLS track prioritization in single pass"
```

---

### Task 3: Engine Session Chunk Sealing & Folder Naming Consolidation

**Files:**
- Modify: `src/engine/mod.rs`
- Test: `tests/test_engine_events.rs`

**Interfaces:**
- Consumes: `LogEntry`, `AppEvent`, `SegmentWatcher`
- Produces:
  `ActiveSessionState::folder_name(&self) -> String`
  `EngineOrchestrator::seal_and_enqueue_chunks(...)`

- [ ] **Step 1: Write a test verifying `ActiveSessionState::folder_name`**

In `tests/test_engine_events.rs`, add:
```rust
#[test]
fn test_active_session_state_folder_name() {
    use chzzk_load::engine::ActiveSessionState;
    let state = ActiveSessionState {
        start_timestamp: "2026-09-22_2200".to_string(),
        streamer_name: "Streamer/Name?".to_string(),
        current_title: "Title: With Colon?".to_string(),
        session_folder_id: None,
    };
    assert_eq!(state.folder_name(), "[2026-09-22_2200] Streamer_Name? - Title_ With Colon?");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_engine_events test_active_session_state_folder_name`
Expected: FAIL with `folder_name` not found.

- [ ] **Step 3: Implement `ActiveSessionState::folder_name` and `EngineOrchestrator::seal_and_enqueue_chunks`**

In `src/engine/mod.rs`:
1. Add `folder_name` to `ActiveSessionState`:
```rust
impl ActiveSessionState {
    pub fn folder_name(&self) -> String {
        format!(
            "[{}] {} - {}",
            self.start_timestamp,
            sanitize_filename(&self.streamer_name),
            sanitize_filename(&self.current_title)
        )
    }
}
```
2. Implement `seal_and_enqueue_chunks`:
```rust
#[allow(clippy::too_many_arguments)]
async fn seal_and_enqueue_chunks(
    watcher: &mut SegmentWatcher,
    session_folder_id: &mut Option<String>,
    drive_opt: Option<&DriveClient>,
    root_name: &str,
    subfolder_name: &str,
    channel_id: &str,
    streamer_name: &str,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    is_finished: bool,
) {
    let sealed_chunks = watcher.detect_sealed(is_finished);
    for chunk_path in sealed_chunks {
        Self::process_sealed_chunk(
            &chunk_path,
            session_folder_id,
            drive_opt,
            root_name,
            subfolder_name,
            channel_id,
            streamer_name,
            upload_tx,
            event_tx,
        )
        .await;
    }
}
```
3. In `spawn_recording_session`, replace lines 507-540 and 574-607 with calls to `seal_and_enqueue_chunks`.
4. Update all `AppEvent::Log(format!(...))` in `src/engine/mod.rs` to typed `AppEvent::Log(LogEntry::clean(...))` / `LogEntry::rec(...)` / `LogEntry::ffmpeg(...)` / `LogEntry::drive(...)` / `LogEntry::warn(...)` / `LogEntry::error(...)` / `LogEntry::poll(...)` / `LogEntry::info(...)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test test_engine_events`
Expected: PASS (all 22 tests pass).

- [ ] **Step 5: Commit**

```bash
git add src/engine/mod.rs tests/test_engine_events.rs
git commit -m "refactor(engine): consolidate chunk sealing and folder naming"
```

---

### Task 4: Watcher Filesystem Metadata Query Optimization

**Files:**
- Modify: `src/recorder/watcher.rs`
- Test: `tests/test_recorder_watcher.rs`

**Interfaces:**
- Consumes: `fs::DirEntry`
- Produces: `pub fn detect_sealed_chunks(...) -> Vec<PathBuf>`

- [ ] **Step 1: Check existing watcher tests**

Run: `cargo test --test test_recorder_watcher`
Expected: 5 passed.

- [ ] **Step 2: Optimize `detect_sealed_chunks`**

In `src/recorder/watcher.rs`:
Replace `fs::metadata(&path).or_else(|_| entry.metadata())` and `path.is_file()` with:
```rust
if let Ok(entries) = fs::read_dir(session_dir) {
    for entry in entries.flatten() {
        let is_file = match entry.file_type() {
            Ok(ft) => ft.is_file(),
            Err(_) => entry.path().is_file(),
        };
        if !is_file {
            continue;
        }

        let path = entry.path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("ts"))
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
        {
            let meta = entry.metadata().or_else(|_| fs::metadata(&path));
            if let Ok(meta) = meta
                && meta.len() > 0
            {
                chunks.push((name.to_string(), path, meta.len()));
            }
        }
    }
}
```

- [ ] **Step 3: Run tests to verify correctness**

Run: `cargo test --test test_recorder_watcher`
Expected: PASS (all 5 tests pass).

- [ ] **Step 4: Commit**

```bash
git add src/recorder/watcher.rs
git commit -m "perf(watcher): leverage DirEntry metadata cache during segment scan"
```

---

### Task 5: TUI App State Ring Buffer (`VecDeque`) & Upload Progress Deduplication

**Files:**
- Modify: `src/tui/app.rs`
- Test: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: `LogEntry`, `AppEvent`
- Produces:
  `pub logs: std::collections::VecDeque<LogEntry>`
  `update_primary_upload(&mut self, chunk_name: &str)`

- [ ] **Step 1: Write test verifying VecDeque ring buffer O(1) behavior & LogEntry storage**

In `tests/test_tui_state.rs`, update `test_app_log_fifo_cap` to check `LogEntry` storage and boundary:
```rust
#[test]
fn test_app_log_fifo_cap() {
    use chzzk_load::tui::event::{AppEvent, LogEntry, LogKind};
    let mut app = App::new();
    for i in 0..250 {
        app.handle_event(AppEvent::Log(LogEntry::info(format!("Log message {}", i))));
    }
    assert_eq!(app.logs.len(), 200);
    assert_eq!(app.logs.front().unwrap().message, "Log message 50");
    assert_eq!(app.logs.front().unwrap().kind, LogKind::Info);
    assert_eq!(app.logs.back().unwrap().message, "Log message 249");
}
```

- [ ] **Step 2: Update `App` in `src/tui/app.rs`**

1. Change `pub logs: Vec<String>` to `pub logs: std::collections::VecDeque<LogEntry>`.
2. In `App::new`, initialize `logs: std::collections::VecDeque::with_capacity(200)`.
3. In `handle_event(AppEvent::Log(entry))`:
```rust
AppEvent::Log(entry) => {
    if self.logs.len() >= 200 {
        self.logs.pop_front();
    }
    self.logs.push_back(entry);
}
```
4. Extract `update_primary_upload`:
```rust
fn update_primary_upload(&mut self, chunk_name: &str) {
    if self.active_upload_name.as_deref() == Some(chunk_name) {
        if let Some(first) = self.active_uploads.values().next() {
            self.active_upload_name = Some(first.chunk_name.clone());
            if first.total_bytes > 0 {
                self.upload_progress_pct =
                    ((first.uploaded_bytes as f64 / first.total_bytes as f64) * 100.0)
                        .round()
                        .min(100.0) as u16;
            }
            self.upload_speed = first.speed_mb_s;
        } else {
            self.active_upload_name = None;
            self.upload_progress_pct = 0;
            self.upload_speed = 0.0;
        }
    }
}
```
5. Use `update_primary_upload` inside `AppEvent::UploadCompleted` and `AppEvent::UploadFailed`.

- [ ] **Step 3: Run tests to verify**

Run: `cargo test --test test_tui_state`
Expected: PASS (or compilation error only in `ui.rs` which will be addressed in Task 6).

- [ ] **Step 4: Commit**

```bash
git add src/tui/app.rs tests/test_tui_state.rs
git commit -m "refactor(tui): use VecDeque ring buffer for logs and deduplicate upload state"
```

---

### Task 6: TUI Dashboard Zero-Allocation Log Rendering

**Files:**
- Modify: `src/tui/ui.rs`
- Test: `tests/test_tui_state.rs`

**Interfaces:**
- Consumes: `App::logs: VecDeque<LogEntry>`
- Produces: `pub fn draw_ui(f: &mut Frame, app: &App)`

- [ ] **Step 1: Check existing UI tests**

Run: `cargo test --test test_tui_state test_draw_ui`

- [ ] **Step 2: Update `draw_ui` in `src/tui/ui.rs` for direct badge styling**

In `src/tui/ui.rs`:
Define a static mapping function:
```rust
fn log_kind_badge_and_style(kind: LogKind) -> (&'static str, Style) {
    match kind {
        LogKind::Error => (" ERROR ", Style::default().fg(Color::Red).add_modifier(Modifier::DIM)),
        LogKind::Warn => (" WARN  ", Style::default().fg(Color::Yellow).add_modifier(Modifier::DIM)),
        LogKind::Clean => (" CLEAN ", Style::default().fg(Color::Green).add_modifier(Modifier::DIM)),
        LogKind::Rec => (" REC   ", Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM)),
        LogKind::Ffmpeg => (" FFMPEG", Style::default().fg(Color::Magenta).add_modifier(Modifier::DIM)),
        LogKind::Drive => (" DRIVE ", Style::default().fg(Color::Blue).add_modifier(Modifier::DIM)),
        LogKind::Poll => (" POLL  ", Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM)),
        LogKind::Info => (" INFO  ", Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM)),
    }
}
```
Flatten log entries into `(LogKind, &str)` pairs:
```rust
let mut flattened_lines: Vec<(LogKind, &str)> = Vec::new();
for entry in &app.logs {
    for line in entry.message.lines() {
        flattened_lines.push((entry.kind, line));
    }
}
```
Render directly using `log_kind_badge_and_style(kind)` and the message slice, eliminating string prefix parsing and string allocations.

- [ ] **Step 3: Run all TUI state & rendering tests**

Run: `cargo test --test test_tui_state`
Expected: PASS (all 18 tests pass).

- [ ] **Step 4: Commit**

```bash
git add src/tui/ui.rs
git commit -m "perf(tui): eliminate per-frame runtime string parsing in draw_ui"
```

---

### Task 7: Full System Integration Verification & Quality Gates

**Files:**
- Verify: Full workspace and all integration test suites.

- [ ] **Step 1: Run full test suite**

Run: `cargo test --all-targets`
Expected: All unit and integration tests pass (0 failures).

- [ ] **Step 2: Run clippy linter with zero warnings**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: 0 warnings.

- [ ] **Step 3: Verify formatting**

Run: `cargo fmt --check`
Expected: Clean formatting.

- [ ] **Step 4: Verify npm packaging and launcher scripts**

Run: `node scripts/test-npm-packages.js`
Expected: All tests pass.

- [ ] **Step 5: Smoke test release build**

Run: `cargo build --release`
Run: `target/release/chzzk-load.exe --help`
Expected: Success.

- [ ] **Step 6: Commit all remaining cleanups**

```bash
git commit -m "chore: complete codebase refactoring and optimization"
```
