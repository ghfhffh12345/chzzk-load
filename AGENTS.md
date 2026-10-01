# Developer & AI Agent Guide (`AGENTS.md`)

`chzzk-load` is a standalone Rust application with a Ratatui TUI that monitors Naver Chzzk streams, losslessly segments video via stream-copied FFmpeg (`-c copy`), concurrently archives live chat into structured JSON Lines (`chat_%04d.jsonl`), uploads completed chunks via rclone (or retains them in local-only mode), and immediately purges local files to maintain a strictly bounded disk footprint.

---

## 1. Quick Commands

```bash
# Check, lint, and format
cargo check --all-targets && cargo clippy --all-targets -- -D warnings && cargo fmt --check

# Test suite
cargo test
cargo test --test test_engine_events <filter>
node scripts/test-npm-packages.js

# Build release binary
cargo build --release
```

---

## 2. Architectural Invariants

### 2.1. Video Recording & FFmpeg (`src/recorder/`)
- **Lossless Stream-Copy**: Always use `-c copy` with piped stdin and stderr. Never transcode or re-encode video (`-c:v libx264`).
- **Clean EOF Termination**: Omit `-reconnect*` flags so natural broadcast end causes instant manifest EOF exit.
- **P2P/Grid Bypass**: Decode base64 `cdn_url` in `p2pPath`/`p2pPathUrlEncoding` for direct CDN HLS streams.
- **Binary Resolution**: Respect `CHZZK_LOAD_FFMPEG_BIN` override before falling back to `"ffmpeg"`.
- **Numeric N+1 Boundary Safety**: In `SegmentWatcher`, seal chunk $N$ only after chunk $N+1$ exists (>0 bytes) or child process exits. Parse chunk numeric index (`chunk_%04d.ts`) rather than array length to prevent false boundary seals during retries.

### 2.2. Live Chat Archiving (`src/chzzk/chat.rs`, `src/recorder/chat_writer.rs`)
- **Flash-Friendly Batched I/O**: Serialize chat messages directly to in-memory `Vec<u8>` buffers in `ChatWriter`. Flush on dual triggers (500 messages or 64 KB) or periodic timer. Never flush or sync disk per message.
- **Chunk Rotation**: Rotate chat chunks aligned with `chunk_duration_seconds` into `chat_%04d.jsonl`, send completed paths via channel to upload queue, and delete locally upon confirmed upload.
- **WebSocket Protocol**: Connect to `wss://kr-ss{n}.chat.naver.com/chat`. Handle handshake (`cmd: 100`), auto-respond to ping (`cmd: 0` $\to$ pong `cmd: 10000`), send 20s heartbeat, and deserialize chat messages (`cmd: 93101`, `93102`).

### 2.3. Upload Pipeline & Disk Management (`src/uploader/`, `src/engine/`)
- **Universal Cloud & Local Sync**: Use `RcloneBackend` for remote uploads (`rclone copyto ... --progress`, `rcat` for metadata). Respect `CHZZK_LOAD_RCLONE_BIN` override before `settings.rclone.rclone_bin` and `"rclone"` on `PATH`. If `remote_path` is empty (`""`), skip upload and retain files locally.
- **Non-Blocking DLQ**: Process uploads through a non-blocking primary queue with cross-channel concurrency (default: 3). Failed uploads divert immediately to a Dead-Letter Queue (DLQ) with exponential backoff (2s, 4s, 8s, max 3 retries, capped at 20 tasks/channel in RAM) so subsequent chunks proceed without head-of-line blocking.
- **Strictly Bounded Disk Footprint**: Delete local video and chat chunks immediately upon confirmed upload (maintaining 1–2 video segments and at most 1 chat segment on disk per active stream).
- **Disk Circuit Breaker**: Periodically check free disk space against `min_free_disk_gb`. If breached, gracefully terminate FFmpeg (`"q\n"`), seal/upload final chunks, and block new sessions until disk space recovers.
- **Crash Recovery & Reconciliation**: Detect orphaned chunks on startup, validate contiguity, and quarantine partial tail chunks.

### 2.4. Stream Polling & Orchestration (`src/engine/`)
- **Anti-Race Cooldown**: Deduplicate CDN cache TTL (10–30s) using finished `live_id`s and post-recording cooldown.
- **Non-Blocking Mutex Scoping**: Never hold `active_sessions` or `active_recordings` mutex across async network I/O or upload tasks.
- **Stream Metadata Tracking**: Dual-write state changes (title, category, tags, rules) to `metadata.jsonl` with monotonic `stream_offset_ms` and sync via `rcat`. Exclude high-frequency telemetry (`concurrent_user_count`).

### 2.5. TUI & Terminal Safety (`src/tui/`)
- **Zero Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while TUI is active. Route all logs to `AppEvent::Log(...)`.
- **Panic Recovery Hook**: Maintain a panic hook that restores terminal raw mode and leaves the alternate screen before invoking the default panic handler.
- **Non-Blocking Telemetry**: Always use `try_send` for TUI telemetry and stats; never block WebSockets or recording loops.
- **Headless Mode**: Auto-detect non-TTY environments or `--headless`/`--no-tui` flags to run safely as a background daemon.

### 2.6. Configuration & Testing Conventions
- **TOML Configuration**: Adhere to `settings.toml` (`toml = "1.1"`). Directory format: `[{timestamp}] [{alias}] {streamer} - {title}` (omit `[{alias}]` if none). Sanitize `\/:*?"<>|` and control characters. Display channel alias consistently in TUI without flicker.
- **Portable Path Resolution**: Resolve relative paths via `app_path::resolve_path(...)` (prioritizes CWD, falls back to executable directory, avoids `node_modules`).
- **Resilient Test Ports & Paths**: Use dynamic ephemeral port binding (`127.0.0.1:0`), never hardcoded ports. All test filesystem mutations must operate strictly within `std::env::temp_dir()`.

---

## Agent skills

### Issue tracker

GitHub issues via `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context (`GLOSSARY.md` + `docs/adr/`). See `docs/agents/domain.md`.
