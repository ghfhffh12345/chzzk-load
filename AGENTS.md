# Developer & AI Agent Guide (`AGENTS.md`)

`chzzk-load` is a standalone Rust application with a Ratatui TUI that monitors Naver Chzzk streams, losslessly segments video via stream-copied FFmpeg (`-c copy`), concurrently archives live chat into structured JSON Lines (`chat_%04d.jsonl`), uploads completed chunks via rclone (or retains them in local-only mode), and immediately purges local files to maintain a strictly bounded disk footprint.

---

## 1. Key System Characteristics

- **Standalone Binary**: Compiles to independent executable (`chzzk-load.exe`); FFmpeg resolved from `PATH` or `CHZZK_LOAD_FFMPEG_BIN`.
- **Zero Transcoding & Clean EOF Exit**: FFmpeg stream-copy (`-c copy`) with piped stdin/stderr. HTTP reconnect flags are intentionally omitted so natural broadcast end causes instant manifest EOF exit.
- **P2P/Grid Bypass**: Decodes base64 `cdn_url` in `p2pPath`/`p2pPathUrlEncoding` for direct CDN HLS (1080p, 720p).
- **Chat Archiving**: Concurrently connects to Chzzk WebSockets, capturing structured JSON Lines (`chat_%04d.jsonl`) aligned with video chunk intervals.
- **Flash-Friendly I/O (SBC Optimized)**: In-memory byte-buffer serialization in `ChatWriter` with dual-trigger flush (500 msgs / 64 KB or periodic timer); skips empty intervals to preserve flash longevity.
- **Strictly Bounded Disk Footprint**: 1–2 video segments and at most 1 chat segment on disk per active stream; chunks deleted immediately upon confirmed upload.
- **N+1 Segment Boundary Safety**: Chunk $N$ sealed only after chunk $N+1$ exists (>0 bytes) or child process exits; parsed via numeric index.
- **Metadata Event Tracking**: State transitions (title, category, tags, rules) tracked in `metadata.jsonl` with millisecond `stream_offset_ms` and synced via `backend.upload_text` (`rclone rcat`).
- **Universal Cloud & Local Sync**: Rclone integration (70+ providers, progress parsing, non-blocking check) or local-only mode (`remote_path = ""`).
- **Anti-Race Cooldown**: Deduplicates CDN cache TTL (10–30s) using finished `live_id`s and post-recording cooldown.
- **Event-Driven TUI**: Ratatui + `crossterm::event::EventStream`; zero-alloc log slicing, no CPU spinning, lockstep view scrolling.

---

## 2. Repository Structure

```
chzzk-load/
├── .github/workflows/ # ci.yml (fmt/clippy/matrix tests/npm), release.yml (cross-build/gh release/npm)
├── npm/chzzk-load/    # Root wrapper CLI package & launcher (bin/chzzk-load.js)
├── scripts/           # prepare-npm.js (--resolve-release), test-npm-packages.js
├── src/
│   ├── main.rs, lib.rs, app_path.rs, config.rs
│   ├── chzzk/         # client.rs (API/CDN extract), chat.rs (WebSocket), models.rs, models_chat.rs
│   ├── recorder/      # ffmpeg.rs (process/flags), watcher.rs (numeric N+1 sealing), chat_writer.rs (batched I/O)
│   ├── uploader/      # backend.rs (trait/mock), rclone.rs (CLI/rcat), worker.rs (non-blocking primary + DLQ)
│   ├── engine/        # session.rs, dispatcher.rs, recording.rs, state.rs, cleanup.rs
│   └── tui/           # app.rs, ui.rs (layout/zero-alloc logs), event.rs, theme.rs, console.rs
└── tests/             # Integration tests (chat, recorder, rclone, engine events, config, TUI)
```

---

## 3. Core Architecture & Lifecycle

### 3.1. Stream Polling & Orchestration (`src/engine/`)
- `EngineOrchestrator::run` continuously polls monitored channels every `poll_interval_seconds`.
- On `status == "OPEN"`: Checks `active_recordings` and `finished_sessions` (`live_id` and `stream_cooldown_seconds`). If valid, registers channel and spawns `RecordingSession`.
- On `status == "CLOSE"`: Resets finished session tracking entry for future broadcasts.
- Releases mutex locks before issuing network requests, uploads, or child process management.

### 3.2. FFmpeg Recording & Watcher (`src/recorder/`)
- `build_ffmpeg_command` spawns FFmpeg with `-extension_picky 0`, `-c copy`, `stdin(Stdio::piped())`, `stdout(Stdio::null())`, and `stderr(Stdio::piped())` (drained to `AppEvent::Log`). No reconnect flags (instant EOF exit).
- `SegmentWatcher` polls every 1s, using zero-allocation `.ts` checks and numeric index parsing (`chunk_%04d.ts`) to seal chunk $N$ when $N+1$ exists (>0 bytes). Seals final chunk when stream ends.

### 3.3. Live Chat Recording (`src/chzzk/chat.rs`, `src/recorder/chat_writer.rs`)
- Fetches chat token from `https://comm-api.game.naver.com/nng_main/v1/chats/access-token`, connects to `wss://kr-ss{n}.chat.naver.com/chat`.
- Sends handshake (`cmd: 100`), auto-responds to server ping (`cmd: 0` $\to$ pong `cmd: 10000`), and sends 20s heartbeat. Deserializes messages (`cmd: 93101`, `93102`).
- `ChatWriter` serializes directly to `Vec<u8>` without intermediate strings; flushes on 500 msgs / 64 KB / timer interval. Rotates chunks aligned with `chunk_duration_seconds` and forwards sealed paths via `mpsc` to upload queue.

### 3.4. Upload Pipeline (`src/uploader/`)
- `UploadWorker` processes chunks via a non-blocking primary queue with cross-channel concurrency (default: 3).
- On upload failure, chunks immediately transfer to a Dead-Letter Queue (DLQ) with exponential backoff (2s, 4s, 8s, max 3 retries); subsequent chunks advance without head-of-line blocking.
- DLQ tasks are capped (20/channel in RAM) and evict on exhaustion (preserved on disk for startup reconciliation).
- `RcloneBackend` executes `rclone copyto ... --progress`, streams `metadata.jsonl` via `rcat`, and verifies connection (10s timeout, bypassable).
- Deletes local chunk immediately on exit status 0. If `remote_path` is empty (`""`), uploads are skipped and files are retained locally.

### 3.5. Ratatui TUI Dashboard (`src/tui/`)
- Event-driven render loop via `crossterm::event::EventStream` and biased `tokio::select!` (renders on state change or 250ms animation tick).
- Fixed vertical layout: Header (1), Divider (1), Body (10 or Fill(1) when logs hidden), Logs Divider (1), Logs (Fill(1)), Footer Divider (1), Keybinds (1).
- Logs sliced directly from borrowed lines up to viewport height; lines exceeding width truncated with `…`. Dividers use zero-alloc static rule slicing.
- Keybinds: View scroll (`Up`/`Down`/`j`/`k`), log scroll (`PageUp`/`PageDown`/`Home`/`End`), log toggle (`l`), quit (`q`/`Ctrl+C`).

---

## 4. Multi-Platform CI/CD & Distribution

- **CI (`.github/workflows/ci.yml`)**: Runs fmt, clippy, multi-OS tests (`ubuntu-latest`, `windows-latest` with setup-ffmpeg), and npm test suite.
- **Release (`.github/workflows/release.yml`)**: Triggers on `v*` tags/dispatch; resolves version, detects pre-releases, and compiles across platforms:
  - Linux: `cargo-zigbuild` on Ubuntu creates static musl binaries (`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`).
  - macOS: `x86_64-apple-darwin` and `aarch64-apple-darwin` on `macos-14`.
  - Windows: `x86_64-pc-windows-msvc` on `windows-latest`.
  - Publishes GitHub release with SHA256 checksums and publishes npm packages with provenance under resolved dist-tag.
- **npm Multi-Package Architecture**: Root `chzzk-load` has optionalDependencies on 5 platform packages (`windows-x64`, `linux-x64`, `linux-arm64`, `darwin-x64`, `darwin-arm64`). Launcher `bin/chzzk-load.js` detects platform/arch or `CHZZK_LOAD_BIN`, ensures chmod permissions, and relays signals/stdio.
- **OIDC Publishing**: Uses GitHub Actions OIDC Trusted Publisher authentication on npmjs.com (no long-lived `NPM_TOKEN`).

---

## 5. Development & Testing Workflow

> [!IMPORTANT]
> **Implementation Gate**: AI agents must NEVER modify or create code/test files without explicit user approval. Always complete design alignment and obtain direct confirmation first.

Adhere to **Test-Driven Development (TDD)**:
1. Write focused reproduction test in `tests/`.
2. Verify test failure (`cargo test --test <name> <filter>`).
3. Implement minimal fix or feature code.
4. Verify tests pass and full test suite succeeds.

```bash
# Check, lint, and format
cargo check --all-targets && cargo clippy --all-targets -- -D warnings && cargo fmt --check

# Test suites
cargo test
cargo test --test test_engine_events <filter>
node scripts/test-npm-packages.js

# Build release binary
cargo build --release
```

---

## 6. Architectural Invariants for Agents

1. **No Direct Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while TUI is active. Route all logs to `AppEvent::Log(...)`.
2. **Terminal Panic Recovery**: Maintain panic hook restoring raw mode and leaving alternate screen before default panic handler.
3. **MPEG-TS Stream Copy**: Strictly `-c copy`; never introduce re-encoding flags (`-c:v libx264`). Recording must remain strictly lossless stream-copy (`-c copy`).
4. **Resilient Test Ports**: Dynamic ephemeral binding (`127.0.0.1:0`), never hardcoded ports.
5. **Safe File Operations in Tests**: All test filesystem mutations must operate strictly within `std::env::temp_dir()`.
6. **Path Resolution**: Resolve relative paths via `app_path::resolve_path(...)` (prioritizes CWD, falls back to portable exe dir, avoids `node_modules`).
7. **Flash Longevity**: Never sync/flush chat per message. Must use `ChatWriter` with batching thresholds (500 msgs / 64 KB / timer). Chunks rotate to `chat_%04d.jsonl`, upload, and delete immediately.
8. **Non-Blocking Telemetry**: Always use `try_send` for TUI telemetry and stats; never block WebSockets or recording loops.
9. **FFmpeg Binary Resolution**: Respect `CHZZK_LOAD_FFMPEG_BIN` override before defaulting to `"ffmpeg"`.
10. **Clean HLS Stream Termination**: Never add `-reconnect*` flags to FFmpeg; live manifests must exit cleanly on natural stream EOF.
11. **Non-Blocking Mutex Scoping**: Never hold `active_sessions` or `active_recordings` mutex across async network I/O or upload tasks.
12. **Rclone Binary Resolution**: Respect `CHZZK_LOAD_RCLONE_BIN` override before `settings.rclone.rclone_bin` and `"rclone"` on `PATH`.
13. **TOML Configuration & Sanitization**: Adhere to `settings.toml` (`toml = "1.1"`). Directory format: `[{timestamp}] [{alias}] {streamer} - {title}` (omit `[{alias}]` if none). Sanitize `\/:*?"<>|` and control chars. Alias fallback in TUI without flicker.
14. **Stream Metadata Tracking**: Dual-write events to `metadata.jsonl` with monotonic `stream_offset_ms` and sync via `rcat`. Exclude high-frequency telemetry (`concurrent_user_count`).
15. **Disk Space Circuit Breaker**: Periodically inspect free disk space against `min_free_disk_gb`. If breached, gracefully terminate FFmpeg (`"q\n"`), seal/upload final chunks, and block new sessions until disk recovers.
16. **Numeric N+1 Boundary Safety**: Segment sealing must strictly enforce N+1 boundary via numeric chunk index parsing (`chunk_%04d.ts`) rather than array length, preventing false boundary seals during DLQ retries.
17. **Explicit User Authorization Before Implementation**: AI agents must present proposals/plans and wait for explicit user confirmation before touching repository files.
18. **Plans Directory**: Save all design specs, readiness analyses, and implementation plans in `plans/YYYY-MM-DD-<topic>.md`.
