# Developer & AI Agent Guide (`AGENTS.md`)

`chzzk-load` is a standalone Rust application with a Ratatui TUI that monitors Naver Chzzk streams, losslessly segments video via stream-copied FFmpeg (`-c copy`), concurrently archives live chat into structured JSON Lines (`chat_%04d.jsonl`), uploads completed chunks via rclone (or retains them locally), and immediately purges local files to maintain a strictly bounded disk footprint.

---

## 1. Quick Commands

```bash
# Lint, format, and check (calibrate WaitMsBeforeAsync: 20000 for chained runs)
cargo check --tests; cargo clippy --tests           # Fast test linter loop (<1s)
cargo check --all-targets; cargo clippy --all-targets -- -D warnings; cargo fmt --check

# Test suite (Tiered fast feedback; calibrate WaitMsBeforeAsync: 20000 for suites)
cargo test --lib <filter>                           # Fast internal module unit tests (<1s)
cargo test --test test_cli_smoke                    # CLI & binary startup smoke tests (<1s)
cargo test --test test_channel_lifecycle_registry   # Fast registry unit tests (<1s)
cargo test --test test_engine_orchestrator_registry # Typed engine seam unit tests (<1s)
cargo test --test test_recorder_ffmpeg              # FfmpegSession unit tests (<1s)
cargo test --test test_recorder_watcher             # FFmpeg watcher unit tests (<1s)
cargo test --test test_tui_state                    # TUI state unit tests (<1s)
# Batched consolidation unit suites (<1s each; batched to avoid linking lock overhead)
cargo test --test test_consolidation_staging --test test_consolidation_concat --test test_consolidation_progress --test test_consolidation_remote
cargo test --test test_shutdown_cleanup             # Shutdown & cleanup tests (WaitMsBeforeAsync: 20000)
cargo test --test test_engine_events <filter>       # Heavy async integration tests (20-25s; WaitMsBeforeAsync: 20000)
cargo test -j 2                                     # Full test suite verification gate (bounded jobs; WaitMsBeforeAsync: 20000)
node scripts/test-npm-packages.js                   # Node packaging and CLI launcher suite

# Git commit (runs .githooks/pre-commit: fmt, check, clippy, fast unit tests; WaitMsBeforeAsync: 20000)
git commit -m "feat/fix: ..."

# Build release binary
cargo build --release
```

---

## 2. Architectural Invariants

### 2.1. Video Recording & Remuxing
- **Lossless Stream Copy**: Capture HLS streams directly with `-c copy` using piped stdin and stderr. Leave video uncompressed and un-transcoded.
- **Clean EOF Termination**: Omit `-reconnect*` flags so natural broadcast conclusion causes immediate manifest EOF exit.
- **Direct CDN Bypass**: Decode base64 `cdn_url` in `p2pPath`/`p2pPathUrlEncoding` for direct CDN HLS stream ingestion.
- **Binary Override**: Respect `CHZZK_LOAD_FFMPEG_BIN` override before falling back to `"ffmpeg"` on `PATH`.
- **Numeric Chunk Sealing**: In `SegmentWatcher`, seal chunk $N$ only when chunk $N+1$ exists (>0 bytes) or child process exits. Parse chunk numeric index (`chunk_%04d.ts`) rather than array length to prevent false boundary seals during retries.
- **Concat Demuxer Consolidation**: Consolidate recorded streams into seekable MP4 (`+faststart`) via FFmpeg concat demuxer (`-f concat -safe 0`) with `-avoid_negative_ts make_zero`. In remote mode, stream chunks into memory via an ephemeral read-only HTTP loopback server (`rclone serve http`) while writing seekable output directly to local staging targets before cloud upload. Manage manifest scripts with RAII `ConcatScriptGuard`.
- **Container Structural Integrity**: Prior to promoting staged `live_stream.mp4.part` or resuming staging uploads, validate container integrity (`validate_mp4_container_integrity`): enforce single `moov`, at least one `mdat`, exact file boundary coverage without trailing unindexed bytes, and verify `stco`/`co64` sample chunk offsets point strictly within the `mdat` payload.

### 2.2. Live Chat Archiving
- **Batched Memory I/O**: Serialize live chat messages directly to in-memory `Vec<u8>` buffers in `ChatWriter`. Flush on dual triggers (500 messages or 64 KB) or periodic timer ticks.
- **Chunk Rotation**: Rotate chat chunks aligned with `chunk_duration_seconds` into `chat_%04d.jsonl`, enqueue completed paths to the upload queue, and unlink locally upon confirmed upload.
- **WebSocket Protocol**: Connect to `wss://kr-ss{n}.chat.naver.com/chat`. Complete handshake (`cmd: 100`), reply to ping (`cmd: 0` $\to$ pong `cmd: 10000`), maintain 20s heartbeat, and deserialize messages (`cmd: 93101`, `93102`).

### 2.3. Upload Pipeline & Disk Management
- **Universal Cloud & Local Sync**: Use `RcloneBackend` for remote transfers (`rclone copyto ... --progress`). Unify all transfers (video segments, chat logs, `live_metadata.jsonl`) under `UploadBackend::upload_file`. Respect `CHZZK_LOAD_RCLONE_BIN` override before `settings.rclone.rclone_bin` and `"rclone"` on `PATH`. When `remote_path` is empty (`""`), retain files locally.
- **Non-Blocking DLQ & Retention Policy**: Process uploads through a non-blocking queue with cross-channel concurrency (default: 3). Route failures immediately to a Dead-Letter Queue (DLQ) with exponential backoff (2s initial, 5m cap, infinite retries, 20 tasks/channel in RAM). Tasks carry explicit `delete_on_success` retention policies: `true` for chunks, stream teardown metadata, and crash reconciliation; `false` for intermediate live metadata snapshots. Under disk pressure, DLQ evicts oldest chunk pairs (`.ts` and `.jsonl`) while preserving metadata tasks.
- **Strictly Bounded Footprint**: Purge local video and chat chunks immediately upon confirmed upload (maintaining 1–2 video segments and at most 1 chat segment on disk per active stream).
- **Strict Directory Emptiness & Quiescence**: Drive local file deletion exclusively from `UploadWorker` upon upload confirmation when `delete_on_success` is true, signaling `drain_notify`. `SessionCustodian` enforces strict directory emptiness (exactly 0 entries) before purging session folders once all in-flight uploads and unlinks reach quiescence.
- **Disk Circuit Breaker**: Periodically check free disk space against `min_free_disk_gb`. If breached, gracefully terminate FFmpeg (`"q\n"`), seal/upload final chunks, and block new sessions until disk space recovers.
- **Crash Recovery & Reconciliation**: Detect orphaned chunks and `live_metadata.jsonl` on startup, validate contiguity, enqueue pending files to `upload_tx` (metadata with `delete_on_success: true`), and quarantine partial tail chunks.
- **Ordered Shutdown Barrier**: Follow strict teardown order: (1) terminate FFmpeg gracefully, (2) seal lingering chunks (`is_stream_finished = true`), (3) await all session tasks, (4) drop `upload_tx`, (5) drain and await `UploadWorker`, and (6) purge empty local session directories.
- **Windows File Lock Resilience**: Apply bounded exponential backoff retries when removing files post-upload (`worker.rs`) or purging directories (`custodian.rs`) to absorb transient Windows sharing violations (32), access denied errors (5), and asynchronous unlink latency (145).
- **Canonical Post-Recording Consolidation**: Merge video and chat into `.part` staging files using the two-stage atomic staging pattern to produce the canonical Live Artifact Triad (`live_stream.mp4`, `live_chat.jsonl`, `live_metadata.jsonl`). Upon orchestrator success, upload staged artifacts and purge source chunks via `Concurrent Chunk Purge`.

### 2.4. Lifecycle Orchestration & Metadata
- **Atomic State Transitions**: Coordinate state transitions across channel states (Idle, Recording, Cooldown, Restricted) exclusively through `ChannelLifecycleRegistry` under a short-lived synchronous mutex. Complete all async I/O outside registry locks.
- **Anti-Race Cooldown**: Deduplicate CDN cache TTL (10–30s) using finished `live_id`s and post-recording cooldown.
- **Stream Metadata Tracking**: Dual-write state changes to `live_metadata.jsonl` with monotonic `stream_offset_ms` and enqueue non-blocking `UploadTask::metadata` snapshots to `upload_tx`. Exclude high-frequency viewer telemetry.

### 2.5. Terminal & Subprocess Safety
- **Structured Terminal Event Logging**: Route all logging through `AppEvent::Log(...)`. Unredirected stdout/stderr writes are prohibited while the TUI is active.
- **Panic Hook**: Maintain a panic hook restoring terminal raw mode and leaving the alternate screen before invoking the default panic handler.
- **Non-Blocking Telemetry**: Telemetry sent to the TUI must use bounded channels with `try_send` or non-blocking forwarders; recording and WebSocket loops must never block on terminal rendering.
- **Headless Daemon Detection**: Auto-detect non-TTY environments or `--headless`/`--no-tui` flags to run cleanly as a background daemon.

### 2.6. Subprocess Hermeticity & Fast Test Seams
- **Tiered Test Discipline**: Run fast unit and smoke tests first (<1s; see Section 1) during development loops. Reserve heavy async integration suites (`test_engine_events`, taking 20–25s) and full `cargo test -j 2` for the final verification gate before commit.
- **Subprocess Mock Isolation**: Orchestrator, lifecycle, chat, and uploader integration tests must use shared mock fixtures (`tests/common/mock_ffmpeg.rs`, `tests/common/mock_rclone.rs`), registering temporary build directories with `register_mock_temp_dir` in `tests/common/mod.rs`. Assert domain behavior through public APIs (`run_consolidation`, `upload_file`).
- **State Machine Invariant Coverage**: Every channel state transition guard, restriction condition, and cancellation behavior in `ChannelLifecycleRegistry` must have a dedicated zero-overhead unit test in `tests/test_channel_lifecycle_registry.rs`.
- **Ephemeral Isolation**: Tests must bind ephemeral ports (`127.0.0.1:0`) and isolate all filesystem activity within `std::env::temp_dir()`.

### 2.7. Windows Execution & Async Guardrails
- **Shell Compatibility**: Write command snippets using semicolon statement separators `;` or separate lines to ensure compatibility with Windows PowerShell and POSIX shells.
- **Async Execution Calibration**: When executing test suites, chained cargo commands, or git pre-commit hooks via `run_command`, calibrate with `WaitMsBeforeAsync: 20000` to prevent premature backgrounding. Stop calling tools and yield the turn immediately after launching background commands or when a command transitions to a task ID.
- **Network Probes**: When inspecting live endpoints via shell commands, invoke `curl.exe` with a browser User-Agent (`-A "Mozilla/5.0 ..."`) and a hard timeout (`--max-time 10`).

---

## 3. Context Pointers

### Rust Tasks
Writing, refactoring, or optimizing Rust code, managing Cargo dependencies, or resolving async/lifetime invariants: see [rust-pro](file:///.agents/skills/rust-pro/SKILL.md).

### Coding Standards
Reviewing pull requests or checking test seams, lifecycle rules, ANSI budgets, or Windows filesystem safety: see [CODING_STANDARDS.md](file:///CODING_STANDARDS.md).

### Domain Glossary
Naming domain models, validating stream lifecycle terms, or discovering architectural concepts: see [GLOSSARY.md](file:///GLOSSARY.md).

### Domain Docs & ADRs
Exploring codebase architecture, evaluating subsystem trade-offs, or surfacing ADR conflicts: see [domain.md](file:///docs/agents/domain.md).

### Issue Tracker
Creating, viewing, filtering, or resolving GitHub issues, child tickets, and wayfinding maps: see [issue-tracker.md](file:///docs/agents/issue-tracker.md).

### Triage Labels
Applying or mapping canonical issue triage roles (`needs-triage`, `ready-for-agent`, etc.): see [triage-labels.md](file:///docs/agents/triage-labels.md).
