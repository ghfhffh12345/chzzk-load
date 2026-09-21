# Developer & AI Agent Guide (`AGENTS.md`)

Welcome to `chzzk-load`. This document serves as the primary technical specification, operational guide, and architectural reference for AI agents and human developers maintaining or extending this codebase.

---

## 1. Project Overview

`chzzk-load` is a high-performance, standalone Rust application equipped with a modern Ratatui Terminal User Interface (TUI). It monitors Naver Chzzk live broadcasts, losslessly segments live video into MPEG-TS chunks via stream-copied FFmpeg (`-c copy`), concurrently uploads completed chunks to Google Drive using resumable chunked uploads, and immediately deletes local files upon confirmed upload to maintain a strictly bounded disk footprint.

### Key System Characteristics
- **Standalone Binary**: Compiles directly into an independent executable (`chzzk-load.exe`) runnable without Cargo or external runtime environments (FFmpeg must be installed and available on `PATH`).
- **Zero CPU Transcoding**: Uses FFmpeg stream-copy (`-c copy`) to segment raw HLS video streams into `.ts` files with near-zero CPU and RAM overhead.
- **Strictly Bounded Disk Footprint**: Only 1–2 segments reside on disk simultaneously per active stream. A chunk is deleted immediately upon receiving an HTTP 200/201 upload confirmation.
- **N+1 Segment Boundary Safety**: Chunk $N$ is only sealed and queued for upload after chunk $N+1$ exists on disk with file size $> 0$ bytes (or upon final stream termination), guaranteeing no partial chunks are uploaded.
- **Anti-Race Cache Deduplication**: Protects against Chzzk CDN cache TTL delays (10–30s) by tracking finished broadcast `live_id`s and enforcing a post-recording cooldown to prevent duplicate sessions.

---

## 2. Repository Structure

```
chzzk-load/
├── Cargo.toml                # Dependencies and binary target definitions
├── settings.json             # Dedicated configuration file (portable)
├── src/
│   ├── main.rs               # CLI entrypoint, signals, panic hooks, TUI event loop
│   ├── lib.rs                # Module root and library exports
│   ├── app_path.rs           # Portable executable-relative path resolution
│   ├── config.rs             # Settings structs, defaults, and serde loaders
│   ├── chzzk/                # Chzzk API integration
│   │   ├── mod.rs
│   │   ├── client.rs         # ChzzkClient: live detail polling, HLS URL extraction
│   │   └── models.rs         # Data structures: LiveDetailContent, LiveStreamInfo, etc.
│   ├── recorder/             # FFmpeg process management & chunk detection
│   │   ├── mod.rs
│   │   ├── ffmpeg.rs         # Command builder, arguments (-extension_picky 0, piped stderr)
│   │   └── watcher.rs        # SegmentWatcher, N+1 chunk sealing logic
│   ├── drive/                # Google Drive API v3 client & OAuth2
│   │   ├── mod.rs
│   │   ├── auth.rs           # DriveAuth: PKCE authorization flow, token refresh
│   │   └── client.rs         # DriveClient: folder search/creation, resumable uploads
│   ├── uploader/             # Upload pipeline
│   │   ├── mod.rs            # UploadTask, UploadWorker (upload-and-delete pipeline)
│   ├── engine/               # Central orchestrator
│   │   └── mod.rs            # EngineOrchestrator: channel polling, sessions, upload consumer
│   └── tui/                  # Ratatui Dashboard
│       ├── mod.rs
│       ├── app.rs            # App state, key event handling, channel list state
│       ├── event.rs          # Central AppEvent enum
│       └── ui.rs             # draw_ui: Layout constraints, strictly bounded logs view
└── tests/                    # Integration and smoke tests
    ├── test_app_path.rs
    ├── test_chzzk_client.rs
    ├── test_cli_smoke.rs
    ├── test_config.rs
    ├── test_drive_auth.rs
    ├── test_drive_uploader.rs
    ├── test_engine_events.rs
    ├── test_recorder_watcher.rs
    └── test_tui_state.rs
```

---

## 3. Core Architecture & Lifecycle

### 3.1. Stream Polling & Session Orchestration (`src/engine/mod.rs`)
1. `EngineOrchestrator::run` executes a continuous loop polling monitored channels every `poll_interval_seconds`.
2. When a channel returns `status == "OPEN"`:
   - Evaluates `is_recording` against `active_recordings`.
   - Checks `finished_sessions` against `info.live_id` and `stream_cooldown_seconds`.
   - If the broadcast has the same `live_id` as the session that just finished, or is within the cooldown window without a distinct `live_id`, spawning is blocked to avoid spawning redundant sessions caused by CDN caching.
   - If valid and unrecorded, registers the channel into `active_recordings` and spawns `spawn_recording_session`.
3. When the channel returns `status == "CLOSE"`:
   - Clears any `finished_sessions` tracking entry, resetting the channel for future broadcasts.

### 3.2. FFmpeg Recording & N+1 Watcher (`src/recorder/`)
- `build_ffmpeg_command` spawns an independent FFmpeg child process with:
  - `-extension_picky 0`: Required for modern FFmpeg builds to demux Naver CDN `.m4v` video segments containing query tokens.
  - `-c copy`: Zero re-encoding overhead.
  - `stdin(Stdio::piped())`: Enables graceful termination via `"q\n"`.
  - `stdout(Stdio::null())` & `stderr(Stdio::piped())`: Prevents raw child process output from leaking into the terminal and corrupting the TUI raw mode buffer. Stderr lines are asynchronously drained into `AppEvent::Log("[FFMPEG] ...")`.
- `SegmentWatcher` polls the session folder every second:
  - Finds `.ts` files sorted lexicographically (`chunk_0000.ts`, `chunk_0001.ts`, ...).
  - Emits chunk $N$ as sealed only when chunk $N+1$ exists with size $> 0$.
  - When the child process exits (`is_stream_finished = true`), seals the final lingering chunk.

### 3.3. Google Drive Upload Pipeline (`src/drive/` & `src/uploader/`)
- Sealed chunks are sent over an unbounded or bounded `mpsc::Sender<UploadTask>` channel to `spawn_upload_consumer`.
- Drive subfolders are created lazily by `process_sealed_chunk` only when the first valid chunk is confirmed sealed.
- Resumable upload initiates with a `POST /upload/drive/v3/files?uploadType=resumable` metadata request, obtaining a session URI.
- The chunk byte stream is transmitted with progress tracking callbacks updating `AppEvent::UploadProgress`.
- On HTTP 200/201 response, `tokio::fs::remove_file(&chunk_path)` executes immediately.

### 3.4. Ratatui TUI Dashboard (`src/tui/`)
- Uses strict vertical layout constraints:
  - Header: `Constraint::Length(3)`
  - Body: `Constraint::Length(9)` (Horizontal split: Channels list on left, Upload gauge + Recorder status on right)
  - Logs: `Constraint::Fill(1)` (Fills all remaining vertical space dynamically)
  - Footer: `Constraint::Length(1)`
- **Log Slicing & Clipping**: Logs are flattened by `\n` to support multi-line error traces, and sliced to exactly `inner_height = (log_area.height - 2)`. Lines exceeding `inner_width` are horizontally truncated with `…` to guarantee zero word-wrap overflow or terminal buffer scrolling.
- **Log Scrolling**: Tail auto-scroll is maintained by default (`log_scroll = 0`). Users can navigate history using `PageUp`, `PageDown`, `Home`, and `End`.

---

## 4. Development & Testing Workflow

Always adhere to **Test-Driven Development (TDD)** when modifying functionality or fixing bugs:
1. Write a focused reproduction test in the `tests/` directory.
2. Verify the test fails (`cargo test --test <name> <filter>`).
3. Implement the minimal fix or feature code.
4. Verify the test passes, and ensure the full test suite passes.

### Build and Test Commands
```bash
# Check code without building
cargo check --all-targets

# Run the full test suite
cargo test

# Run a specific test suite or test case
cargo test --test test_engine_events
cargo test --test test_engine_events test_engine_orchestrator_prevents_duplicate_session_race_condition

# Run linter (must pass with 0 warnings)
cargo clippy --all-targets -- -D warnings

# Format code
cargo fmt --check

# Compile optimized standalone binary
cargo build --release

# Smoke test release binary
target/release/chzzk-load.exe --help
```

---

## 5. Architectural Invariants for Agents

When implementing changes, AI agents must strictly preserve the following rules:

1. **No Direct Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while the TUI is active. All diagnostic output must be routed through `AppEvent::Log(...)`.
2. **Terminal Panic Recovery**: Maintain the panic hook in `src/main.rs` that calls `disable_raw_mode()` and `execute!(stdout, LeaveAlternateScreen, Show)` before invoking the default panic handler.
3. **MPEG-TS Stream Copy**: Never introduce re-encoding flags (`-c:v libx264`, etc.) into `build_ffmpeg_command`. Recording must remain strictly lossless stream-copy (`-c copy`).
4. **Resilient HTTP Mocking**: In unit tests, avoid binding fixed ports or connecting to external network endpoints. Use `tiny_http::Server::http("127.0.0.1:0")` to allocate dynamic local test ports.
5. **Safe File Operations**: All tests performing filesystem mutations must operate strictly within `std::env::temp_dir()`.
6. **Path Portability**: Always resolve relative file and directory paths using `app_path::resolve_path(...)` so the binary operates predictably regardless of the working directory.
