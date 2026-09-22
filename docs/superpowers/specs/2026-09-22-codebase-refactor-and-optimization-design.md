# Architecture & Design Specification: Codebase Refactor & Optimization

- **Topic**: Codebase Refactor, Log Decoupling & Multi-Agent Optimization
- **Date**: 2026-09-22
- **Status**: Approved

---

## 1. Executive Summary

This specification establishes an architectural refactoring and optimization for `chzzk-load`. The project currently couples logging messages with arbitrary string prefixes (`[INFO]`, `[WARN]`, `[REC]`, etc.), causing runtime overhead due to per-frame string parsing, bracket stripping, and heap allocations in the Ratatui dashboard. Additionally, session chunk sealing contains copy-pasted logic across cancellation and normal recording loops, and directory traversal incurs avoidable filesystem syscalls.

This refactoring decouples log messages into strongly typed `LogKind` and `LogEntry` domain structures, transitions TUI log buffering to an $O(1)$ ring buffer (`VecDeque`), eliminates per-frame string parsing in `draw_ui`, consolidates engine recording logic, optimizes filesystem metadata lookups, and streamlines HLS video track selection.

---

## 2. Goals & Invariants

### 2.1. Functional Goals
- **Structured Logging Core**: Introduce `LogKind` enum and `LogEntry` struct to represent all diagnostic log messages cleanly.
- **Backward Compatibility**: Provide `Display` and `From<String>` implementations for `LogEntry` so existing assertions, log formatting, and tests remain seamless.
- **Zero-Allocation Log Rendering**: Render log lines in `draw_ui` by styling badge spans and message content directly without per-frame string parsing or string allocations.
- **Bounded O(1) Log History**: Replace `Vec::remove(0)` in `App` with `VecDeque::pop_front()` to eliminate $O(N)$ memory shifts.
- **Consolidated Chunk Sealing**: Replace duplicated chunk sealing blocks in `EngineOrchestrator` with a reusable `seal_and_enqueue_chunks` helper.
- **Canonical Session Naming**: Centralize folder name formatting on `ActiveSessionState`.
- **Filesystem Traversal Optimization**: Leverage `DirEntry::metadata()` before `fs::metadata()` to avoid redundant `GetFileAttributesEx` syscalls.
- **Single-Pass Track Selection**: Refactor `extract_best_hls_url` in `ChzzkClient` to prioritize tracks in a single pass.

### 2.2. Architectural Invariants
1. **Zero CPU Transcoding**: FFmpeg must strictly keep `-c copy` lossless stream copying.
2. **N+1 Segment Boundary Safety**: A chunk $N$ must only be sealed when chunk $N+1$ exists on disk with size $> 0$ (or upon final stream termination).
3. **Strictly Bounded Disk Footprint**: Local chunks are deleted immediately upon Google Drive HTTP 200/201 confirmation.
4. **Terminal Panic Recovery**: Maintain panic hook restoring terminal mode on abnormal termination.
5. **No Direct Terminal Pollution**: No `println!` or raw stdout leaking into TUI buffer; all diagnostics must flow through `AppEvent::Log`.

---

## 3. Detailed Component Architecture

### 3.1. Structured Logging Core (`src/tui/event.rs`)

A new module or submodule under `src/tui/` introducing:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogKind {
    Info,
    Warn,
    Error,
    Clean,
    Rec,
    Ffmpeg,
    Drive,
    Poll,
}

impl LogKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogKind::Info => "INFO",
            LogKind::Warn => "WARN",
            LogKind::Error => "ERROR",
            LogKind::Clean => "CLEAN",
            LogKind::Rec => "REC",
            LogKind::Ffmpeg => "FFMPEG",
            LogKind::Drive => "DRIVE",
            LogKind::Poll => "POLL",
        }
    }
}
```

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub kind: LogKind,
    pub message: String,
}

impl LogEntry {
    pub fn new(kind: LogKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn info(message: impl Into<String>) -> Self { Self::new(LogKind::Info, message) }
    pub fn warn(message: impl Into<String>) -> Self { Self::new(LogKind::Warn, message) }
    pub fn error(message: impl Into<String>) -> Self { Self::new(LogKind::Error, message) }
    pub fn clean(message: impl Into<String>) -> Self { Self::new(LogKind::Clean, message) }
    pub fn rec(message: impl Into<String>) -> Self { Self::new(LogKind::Rec, message) }
    pub fn ffmpeg(message: impl Into<String>) -> Self { Self::new(LogKind::Ffmpeg, message) }
    pub fn drive(message: impl Into<String>) -> Self { Self::new(LogKind::Drive, message) }
    pub fn poll(message: impl Into<String>) -> Self { Self::new(LogKind::Poll, message) }
}

impl std::fmt::Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind.as_str(), self.message)
    }
}

impl<T: Into<String>> From<T> for LogEntry {
    fn from(s: T) -> Self {
        let text = s.into();
        if let Some(rest) = text.strip_prefix('[') {
            if let Some((tag, msg)) = rest.split_once(']') {
                let kind = match tag.trim() {
                    "ERROR" => LogKind::Error,
                    "WARN" => LogKind::Warn,
                    "CLEAN" => LogKind::Clean,
                    "REC" => LogKind::Rec,
                    "FFMPEG" => LogKind::Ffmpeg,
                    "DRIVE" => LogKind::Drive,
                    "POLL" => LogKind::Poll,
                    _ => LogKind::Info,
                };
                return Self {
                    kind,
                    message: msg.trim_start().to_string(),
                };
            }
        }
        Self {
            kind: LogKind::Info,
            message: text,
        }
    }
}
```

`AppEvent::Log` is updated:
```rust
pub enum AppEvent {
    // ...
    Log(LogEntry),
}
```
Helper methods on `AppEvent`:
```rust
impl AppEvent {
    pub fn log(kind: LogKind, message: impl Into<String>) -> Self {
        AppEvent::Log(LogEntry::new(kind, message))
    }
}
```

### 3.2. Engine & Pipeline Consolidation (`src/engine/mod.rs`)

1. **ActiveSessionState Folder Name**:
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
2. **Chunk Sealing Deduplication**:
   A single private helper replaces duplicate loops in `spawn_recording_session`:
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

### 3.3. Segment Watcher Filesystem Optimization (`src/recorder/watcher.rs`)

In `detect_sealed_chunks`:
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
            if let Ok(meta) = meta && meta.len() > 0 {
                chunks.push((name.to_string(), path, meta.len()));
            }
        }
    }
}
```

### 3.4. TUI Dashboard & Buffer Optimization (`src/tui/app.rs` & `src/tui/ui.rs`)

1. **Ring Buffer in `App`**:
   `pub logs: std::collections::VecDeque<LogEntry>` with `logs.pop_front()` when `len() >= 200`.
2. **Unified Upload Transition**:
   A helper method in `App`:
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
3. **Zero-Allocation Rendering in `draw_ui`**:
   Render each visible log line directly using mapped styles:
   - `LogKind::Error` -> Red
   - `LogKind::Warn` -> Yellow
   - `LogKind::Clean` -> Green
   - `LogKind::Rec` -> Cyan
   - `LogKind::Ffmpeg` -> Magenta
   - `LogKind::Drive` -> Blue
   - `LogKind::Poll` -> DarkGray
   - `LogKind::Info` -> DarkGray

---

## 4. Multi-Agent Execution Plan

The implementation is broken down into structured, isolated subtasks:

- **Agent Alpha (Core Models & Types)**:
  - Add `LogKind` and `LogEntry` to `src/tui/event.rs`.
  - Update `AppEvent::Log(LogEntry)`.
  - Refactor `extract_best_hls_url` in `src/chzzk/client.rs`.
  - Verify and update `tests/test_chzzk_client.rs`.
- **Agent Beta (Engine, Watcher & Pipeline)**:
  - Update `src/engine/mod.rs` to emit `LogEntry` variants.
  - Implement `seal_and_enqueue_chunks` and `ActiveSessionState::folder_name`.
  - Optimize `src/recorder/watcher.rs` filesystem calls.
  - Verify `tests/test_engine_events.rs` and `tests/test_recorder_watcher.rs`.
- **Agent Gamma (TUI State & UI Rendering)**:
  - Update `src/tui/app.rs` with `VecDeque<LogEntry>` and `update_primary_upload`.
  - Update `src/tui/ui.rs` for direct badge styling with zero runtime parsing.
  - Update `src/main.rs` log emission sites.
  - Verify `tests/test_tui_state.rs`.
- **Agent Delta (Integration & Verification)**:
  - Run full test suite: `cargo test`.
  - Run clippy: `cargo clippy --all-targets -- -D warnings`.
  - Run formatter: `cargo fmt --check`.
  - Run npm launcher verification: `node scripts/test-npm-packages.js`.

---

## 5. Verification & Acceptance Criteria
- All 96 existing tests pass without regressions.
- No per-frame string parsing in `draw_ui`.
- Clippy passes with 0 warnings.
- Code formatting adheres strictly to `rustfmt`.
- Standalone execution and npm test suite pass cleanly.
