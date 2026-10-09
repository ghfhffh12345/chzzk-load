# Developer & AI Agent Guide (`AGENTS.md`)

`chzzk-load` is a standalone Rust application with a Ratatui TUI that monitors Naver Chzzk streams, losslessly segments video via stream-copied FFmpeg (`-c copy`), concurrently archives live chat into structured JSON Lines (`chat_%04d.jsonl`), uploads completed chunks via rclone (or retains them in local-only mode), and immediately purges local files to maintain a strictly bounded disk footprint.

---

## 1. Quick Commands

```bash
# Check, lint, and format (PowerShell / Bash compatible; calibrate WaitMsBeforeAsync: 20000 for chained commands)
cargo check --tests; cargo clippy --tests           # Fast test linter loop (<1s, catch style/import errors before test runs)
cargo check --all-targets; cargo clippy --all-targets -- -D warnings; cargo fmt --check

# Test suite (Tiered Fast Feedback; calibrate WaitMsBeforeAsync: 20000 for suites / chained runs)
cargo test --test test_cli_smoke                    # CLI & binary startup smoke tests (<1s)
cargo test --test test_channel_lifecycle_registry   # Fast registry unit tests (<1s)
cargo test --test test_engine_orchestrator_registry # Typed engine seam unit tests (<1s)
cargo test --test test_recorder_ffmpeg              # FfmpegSession unit tests (<1s)
cargo test --test test_recorder_watcher             # FFmpeg watcher unit tests (<1s)
cargo test --test test_tui_state                    # TUI state unit tests (<1s)
cargo test --test test_shutdown_cleanup             # Shutdown & cleanup tests (calibrate WaitMsBeforeAsync: 20000)
cargo test --test test_engine_events <filter>       # Heavy async integration tests (20-25s; calibrate WaitMsBeforeAsync: 20000)
cargo test                                          # Full test suite (final verification gate; calibrate WaitMsBeforeAsync: 20000)
node scripts/test-npm-packages.js                   # Node packaging and CLI launcher suite

# Git commit (triggers .githooks/pre-commit: fmt, check, clippy, fast unit tests; calibrate WaitMsBeforeAsync: 20000)
git commit -m "feat/fix: ..."

# Build release binary
cargo build --release
```

---

## 2. Architectural Invariants

### 2.1. Video Recording & FFmpeg (`src/recorder/`, `src/consolidation/`)
- **Lossless Stream-Copy**: Always use `-c copy` with piped stdin and stderr. Never transcode or re-encode video (`-c:v libx264`).
- **Clean EOF Termination**: Omit `-reconnect*` flags so natural broadcast end causes instant manifest EOF exit.
- **P2P/Grid Bypass**: Decode base64 `cdn_url` in `p2pPath`/`p2pPathUrlEncoding` for direct CDN HLS streams.
- **Binary Resolution**: Respect `CHZZK_LOAD_FFMPEG_BIN` override before falling back to `"ffmpeg"`.
- **Numeric N+1 Boundary Safety**: In `SegmentWatcher`, seal chunk $N$ only after chunk $N+1$ exists (>0 bytes) or child process exits. Parse chunk numeric index (`chunk_%04d.ts`) rather than array length to prevent false boundary seals during retries.
- **Post-Recording Consolidation & Concat Demuxer**: When consolidating recorded streams into final MP4 ([ADR 0010](docs/adr/0010-concat-demuxer-timestamp-normalization.md), `src/consolidation/video.rs`), use FFmpeg's concat demuxer (`-f concat -safe 0`) with `-avoid_negative_ts make_zero`. Never feed raw chunk byte streams into `pipe:0`. Local mode produces seekable MP4 with `+faststart`; remote mode streams via an ephemeral read-only HTTP loopback server (`rclone serve http`) into `rclone rcat`. Manage manifest scripts with RAII `ConcatScriptGuard`.

### 2.2. Live Chat Archiving (`src/chzzk/chat.rs`, `src/recorder/chat_writer.rs`)
- **Flash-Friendly Batched I/O**: Serialize chat messages directly to in-memory `Vec<u8>` buffers in `ChatWriter`. Flush on dual triggers (500 messages or 64 KB) or periodic timer. Never flush or sync disk per message.
- **Chunk Rotation**: Rotate chat chunks aligned with `chunk_duration_seconds` into `chat_%04d.jsonl`, send completed paths via channel to upload queue, and delete locally upon confirmed upload.
- **WebSocket Protocol**: Connect to `wss://kr-ss{n}.chat.naver.com/chat`. Handle handshake (`cmd: 100`), auto-respond to ping (`cmd: 0` $\to$ pong `cmd: 10000`), send 20s heartbeat, and deserialize chat messages (`cmd: 93101`, `93102`).

### 2.3. Upload Pipeline & Disk Management (`src/uploader/`, `src/engine/`)
- **Universal Cloud & Local Sync**: Use `RcloneBackend` for remote uploads (`rclone copyto ... --progress`). All file transfers—video segments, chat logs, and stream metadata snapshots (`metadata.jsonl`)—are unified under `UploadBackend::upload_file`. Eliminate `upload_text` and `rcat` streaming entirely. Respect `CHZZK_LOAD_RCLONE_BIN` override before `settings.rclone.rclone_bin` and `"rclone"` on `PATH`. If `remote_path` is empty (`""`), skip upload and retain files locally.
- **Non-Blocking DLQ & Task Retention Policy**: Process uploads through a non-blocking primary queue with cross-channel concurrency (default: 3). Failed uploads divert immediately to a Dead-Letter Queue (DLQ) with exponential backoff (initial 2s, capped at 5m, infinite retries, capped at 20 tasks/channel in RAM) so subsequent chunks proceed without head-of-line blocking. Each `UploadTask` specifies an explicit `delete_on_success` retention policy: `true` for chunks, final stream teardown metadata, and orphaned reconciliation; `false` for intermediate live metadata snapshots. Under disk pressure, DLQ evicts oldest chunk pairs (`.ts` and `.jsonl`) while metadata tasks remain strictly immune from eviction.
- **Strictly Bounded Disk Footprint**: Delete local video and chat chunks immediately upon confirmed upload (maintaining 1–2 video segments and at most 1 chat segment on disk per active stream).
- **Strict Directory Emptiness & Quiescence**: Local file unlinking is driven exclusively by `UploadWorker` upon upload confirmation when `delete_on_success` is true, signaling `drain_notify`. `SessionCustodian` enforces strict directory emptiness (exactly 0 entries, without heuristic file-name sniffing) before purging session folders once all in-flight uploads and unlinks reach quiescence.
- **Disk Circuit Breaker**: Periodically check free disk space against `min_free_disk_gb`. If breached, gracefully terminate FFmpeg (`"q\n"`), seal/upload final chunks, and block new sessions until disk space recovers.
- **Crash Recovery & Reconciliation**: Detect orphaned chunks and `metadata.jsonl` on startup, validate contiguity, enqueue pending files into `upload_tx` (metadata with `delete_on_success: true`), and quarantine partial tail chunks.
- **Graceful Shutdown Barrier**: Maintain strict exit order: (1) terminate FFmpeg gracefully, (2) seal lingering chunks (`is_stream_finished = true`), (3) await all session tasks, (4) drop `upload_tx`, (5) drain and await `UploadWorker`, and (6) purge empty local session directories. Never invoke folder cleanup before the upload worker finishes in-flight chunk deletions.
- **Windows File Lock Resilience**: Bounded exponential backoff retries when removing files post-upload (`worker.rs`) or purging directories (`custodian.rs`) to absorb transient Windows sharing violations (32), access denied errors (5), and asynchronous unlink latency (145).
- **Post-Recording Consolidation Pipeline**: For offline/post-stream consolidation ([ADR 0010](docs/adr/0010-concat-demuxer-timestamp-normalization.md)), consolidate video via concat demuxer and merge chat logs into `chat.jsonl.part` using the two-stage atomic staging pattern. In remote consolidation, stream output to cloud destination via `rclone rcat` over ephemeral HTTP loopback without local staging; upon orchestrator-level success, purge source chunk files atomically using `unlink_local_file_with_retry`.

### 2.4. Stream Polling & Orchestration (`src/engine/`)
- **Module Topology**:
  - `src/engine.rs`: Top-level `EngineOrchestrator` runtime coordinator and poll loop.
  - `src/engine/recording.rs`: Per-stream `RecordingSession` subprocess and chunk lifecycle.
  - `src/engine/session.rs`: Active recording session state, metadata history, and folder naming.
  - `src/engine/registry.rs`: Atomic lifecycle state machine (`ChannelLifecycleRegistry`).
  - `src/engine/custodian.rs`: Lifecycle coordinator for recording session directory tracking, safe quiescence-based purging, and unmanaged empty directory sweeps (`SessionCustodian`).
  - `src/engine/reconciliation.rs`: Startup crash recovery and orphaned chunk reconciliation.
- **Anti-Race Cooldown**: Deduplicate CDN cache TTL (10–30s) using finished `live_id`s and post-recording cooldown.
- **Atomic Lifecycle Transitions**: State transitions across channel states (Idle, Recording, Cooldown, Restricted) are mediated exclusively by `ChannelLifecycleRegistry` under a short-lived sync mutex; never perform async I/O while holding registry locks.
- **Stream Metadata Tracking**: Dual-write state changes (title, category, tags, flags) to `metadata.jsonl` with monotonic `stream_offset_ms` and non-blockingly enqueue `UploadTask::metadata` snapshots to `upload_tx` (live updates with `delete_on_success: false`, stream teardown with `delete_on_success: true`). Exclude high-frequency telemetry (`concurrent_user_count`). Zero synchronous child process awaits in the orchestrator poll loop.

### 2.5. TUI & Terminal Safety (`src/tui/`)
- **Zero Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while TUI is active. Route all logs to `AppEvent::Log(...)`.
- **Panic Recovery Hook**: Maintain a panic hook that restores terminal raw mode and leaves the alternate screen before invoking the default panic handler.
- **Non-Blocking Telemetry**: Always use `try_send` for TUI telemetry and stats; never block WebSockets or recording loops.
- **Headless Mode**: Auto-detect non-TTY environments or `--headless`/`--no-tui` flags to run safely as a background daemon.

### 2.6. Configuration & Testing Conventions
- **TOML Configuration**: Adhere to `settings.toml` (`toml = "1.1"`). Directory format: `[{timestamp}] [{alias}] {streamer} - {title}` (omit `[{alias}]` if none). Sanitize `\/:*?"<>|` and control characters. Display channel alias consistently in TUI without flicker.
- **Portable Path Resolution**: Resolve relative paths via `app_path::resolve_path(...)` (prioritizes CWD, falls back to executable directory, avoids `node_modules`).
- **Resilient Test Ports & Paths**: Use dynamic ephemeral port binding (`127.0.0.1:0`), never hardcoded ports. All test filesystem mutations must operate strictly within `std::env::temp_dir()`.

### 2.7. Testing & Fast-Feedback Discipline
- **Tiered Test Execution**: Always run targeted unit and smoke tests first (`test_cli_smoke`, `test_channel_lifecycle_registry`, `test_engine_orchestrator_registry`, `test_recorder_watcher`, `test_recorder_ffmpeg`, `test_tui_state`, running in <1s) during tight TDD loops. Run `cargo check --tests; cargo clippy --tests` immediately after drafting test files to catch mechanical style and unused import issues in <1 second before launching multi-second test suites. Reserve heavy async integration suites (`test_engine_events`, taking 20–25s) and full `cargo test` for the final verification gate before commit.
- **Subprocess Hermeticity**: Orchestrator, lifecycle, chat, and uploader integration tests must never spawn real external binaries against dummy ports or endpoints. Use shared subprocess mock fixtures (`tests/common/mock_ffmpeg.rs` via `get_mock_ffmpeg_bin()`, which automatically sets `CHZZK_LOAD_FFMPEG_BIN`, and `tests/common/mock_rclone.rs` via `get_mock_rclone_bin()`). When introducing mock binaries, register their temporary build directories with `register_mock_temp_dir` in `tests/common/mod.rs` rather than duplicating C-ABI `atexit` handlers. Real FFmpeg and rclone subprocesses are reserved exclusively for wrapper unit tests (`test_recorder_ffmpeg.rs`, `test_recorder_watcher.rs`).
- **State Machine Invariant Coverage**: Every channel state transition guard, restriction condition, and cancellation behavior in `ChannelLifecycleRegistry` must have a dedicated zero-overhead unit test in `tests/test_channel_lifecycle_registry.rs`. Never rely exclusively on integration suites to catch lifecycle state regressions.
- **Ephemeral Port & Directory Isolation**: Tests must never bind hardcoded network ports (use `127.0.0.1:0`) and must isolate all filesystem activity inside `std::env::temp_dir()`. Clean up directories upon test completion.
- **Cross-Platform Shell Compatibility**: Write command snippets using semicolon statement separators `;` or separate lines rather than Bash-only `&&` operators to ensure compatibility with Windows PowerShell and POSIX shells.
- **Pre-Commit Hook & Command Calibration**: The repository enforces pre-commit hooks via git `core.hooksPath = .githooks` (`cargo fmt`, `cargo check`, `cargo clippy`, and the 6 fast unit test suites). When running `git commit` or executing test suites / chained cargo commands (`test_engine_events`, `test_shutdown_cleanup`, `cargo test`, `cargo check ...; cargo clippy ...`) via `run_command`, calibrate with `WaitMsBeforeAsync: 20000` to prevent premature backgrounding near the default 10,000ms threshold. Slower suites run only during `cargo test` as the full verification gate.

### 2.8. Tool Economy & Async Execution
- **Reactive Yielding**: Stop calling tools and yield the turn immediately after launching background commands (`run_command`) or subagents (`invoke_subagent`) when no local work remains. Rely exclusively on reactive environment wakeup messages on task exit or subagent reply. Never poll or loop over `manage_task(Action='status')` or `manage_subagents(Action='list')`, and never set `schedule` timers on active background task IDs (`task-XXX`) since the system automatically notifies and wakes upon task completion.
- **Native Tools over Shell Utilities**: Prioritize native agent tools (e.g., `view_file`) for file inspection. Use `git grep -n <query>` via `run_command` for repository-wide audits when searching for symbols or strings across multiple files. Avoid ad-hoc shell navigation commands (`cat`, `ls`).
- **Scratch Space for Prototyping**: Do not run brittle, multi-line PowerShell scripts inside a single `run_command` string. Instead, create temporary scripts inside the artifact scratch space (`<appDataDir>\brain\<conversation-id>/scratch/`) using `write_to_file`, then execute them.
- **Conditional Instructions**: Always read conditional documents (like `GLOSSARY.md` or ADRs) when a skill explicitly instructs you to check them.

---

## Agent skills

### Rust tasks

Proactively use the `rust-pro` skill whenever handling Rust-related tasks. See `.agents/skills/rust-pro/SKILL.md`.

### Issue tracker

GitHub issues via `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context (`GLOSSARY.md` + `docs/adr/`). See `docs/agents/domain.md`.

### Coding standards

Authoritative review-stage rules and code smell definitions. See `CODING_STANDARDS.md`.
