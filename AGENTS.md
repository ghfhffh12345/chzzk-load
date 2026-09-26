# Developer & AI Agent Guide (`AGENTS.md`)

Welcome to `chzzk-load`. This document serves as the primary technical specification, operational guide, and architectural reference for AI agents and human developers maintaining or extending this codebase.

---

## 1. Project Overview

`chzzk-load` is a high-performance, standalone Rust application equipped with a modern Ratatui Terminal User Interface (TUI). It monitors Naver Chzzk live broadcasts, losslessly segments live video into MPEG-TS chunks via stream-copied FFmpeg (`-c copy`), concurrently archives live chat via WebSocket into structured JSON Lines (`chat.jsonl`), concurrently uploads completed chunks and logs to Google Drive using resumable chunked uploads, and immediately deletes local files upon confirmed upload to maintain a strictly bounded disk footprint.

### Key System Characteristics
- **Standalone Binary**: Compiles directly into an independent executable (`chzzk-load.exe`) runnable without Cargo or external runtime environments (FFmpeg must be installed and available on `PATH`, or configured via `CHZZK_LOAD_FFMPEG_BIN`).
- **Zero CPU Transcoding**: Uses FFmpeg stream-copy (`-c copy`) to segment raw HLS video streams into `.ts` files with near-zero CPU and RAM overhead.
- **Real-Time Live Chat Archiving**: Concurrently connects to Chzzk chat WebSockets, capturing structured JSON Lines logs (`chat.jsonl`) with message timestamps, user badges, donation details, and raw payload.
- **Flash-Friendly Batched I/O (SBC Optimized)**: Minimizes write cycles to protect microSD and flash memory longevity on Single Board Computers (Raspberry Pi/ARM64) using an in-memory buffer (`ChatWriter`) with dual-trigger flushing (500 messages / 64 KB capacity, or periodic timer interval).
- **Strictly Bounded Disk Footprint**: Only 1–2 video segments reside on disk simultaneously per active stream. Chunks and completed chat logs are deleted immediately upon receiving an HTTP 200/201 upload confirmation.
- **N+1 Segment Boundary Safety**: Chunk $N$ is only sealed and queued for upload after chunk $N+1$ exists on disk with file size $> 0$ bytes (or upon final stream termination), guaranteeing no partial chunks are uploaded.
- **Dynamic Title Tracking & Folder Sync**: Detects stream title changes during broadcasts, records them to `title_history.txt`, and automatically synchronizes Google Drive folder names in real time.
- **Anti-Race Cache Deduplication**: Protects against Chzzk CDN cache TTL delays (10–30s) by tracking finished broadcast `live_id`s and enforcing a post-recording cooldown to prevent duplicate sessions.

---

## 2. Repository Structure

```
chzzk-load/
├── .github/
│   └── workflows/
│       ├── ci.yml            # CI validation (fmt, clippy, multi-OS tests with setup-ffmpeg, npm tests)
│       └── release.yml       # Release pipeline (multi-platform builds, GitHub Release, npm publish)
├── Cargo.toml                # Dependencies and binary target definitions
├── README.md                 # Primary documentation (English)
├── README.ko.md              # Documentation (Korean)
├── settings.json             # Dedicated configuration file (portable)
├── npm/
│   └── chzzk-load/           # Root npm CLI wrapper package
│       ├── bin/
│       │   └── chzzk-load.js # Platform resolution & execution launcher script
│       ├── package.json      # Wrapper package definition with optionalDependencies
│       └── README.md
├── scripts/
│   ├── prepare-npm.js        # Platform package generator & binary bundler
│   └── test-npm-packages.js  # Automated mock packaging & execution test suite
├── src/
│   ├── main.rs               # CLI entrypoint, signals, panic hooks, TUI event loop
│   ├── lib.rs                # Module root and library exports
│   ├── app_path.rs           # Portable executable-relative path resolution
│   ├── config.rs             # Settings structs, defaults, and serde loaders (record_chat, chat_flush_interval_seconds)
│   ├── chzzk/                # Chzzk API integration
│   │   ├── mod.rs
│   │   ├── client.rs         # ChzzkClient: live detail polling, HLS URL extraction, chat access token API
│   │   ├── chat.rs           # ChzzkChatClient: WebSocket handshake (cmd: 100), ping-pong, auto-reconnect backoff
│   │   ├── models.rs         # Data structures: LiveDetailContent, LiveStreamInfo, etc.
│   │   └── models_chat.rs    # Chat models: ChatAccessTokenResponse, RecordedChatMessage, WebSocket packet envelopes
│   ├── recorder/             # FFmpeg process management, watcher & chat writer
│   │   ├── mod.rs
│   │   ├── ffmpeg.rs         # Command builder (-extension_picky 0, piped stderr, CHZZK_LOAD_FFMPEG_BIN)
│   │   ├── watcher.rs        # SegmentWatcher, N+1 chunk sealing logic
│   │   └── chat_writer.rs    # ChatWriter: in-memory batched writer with dual-trigger flush
│   ├── drive/                # Google Drive API v3 client & OAuth2
│   │   ├── mod.rs
│   │   ├── auth.rs           # DriveAuth: PKCE authorization flow, token refresh
│   │   └── client.rs         # DriveClient: folder search/creation, resumable uploads, rename
│   ├── uploader/             # Upload pipeline
│   │   ├── mod.rs            # UploadTask, UploadWorker (upload-and-delete pipeline)
│   ├── engine/               # Central orchestrator
│   │   └── mod.rs            # EngineOrchestrator: channel polling, sessions, chat task, title history, upload consumer
│   └── tui/                  # Ratatui Dashboard
│       ├── mod.rs
│       ├── app.rs            # App state, key event handling, channel list state, chat_count telemetry
│       ├── event.rs          # Central AppEvent enum (ChatStats, LogEntry::chat)
│       └── ui.rs             # draw_ui: Layout constraints, strictly bounded logs view, chat badges
└── tests/                    # Integration and smoke tests
    ├── test_app_path.rs
    ├── test_chat_client.rs
    ├── test_chat_writer.rs
    ├── test_chzzk_client.rs
    ├── test_cli_smoke.rs
    ├── test_config.rs
    ├── test_drive_auth.rs
    ├── test_drive_uploader.rs
    ├── test_engine_chat.rs
    ├── test_engine_events.rs
    ├── test_recorder_watcher.rs
    ├── test_tui_console.rs
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
  - Binary resolution: checks `CHZZK_LOAD_FFMPEG_BIN` environment variable before defaulting to `"ffmpeg"`.
  - `-extension_picky 0`: Required for modern FFmpeg builds to demux Naver CDN `.m4v` video segments containing query tokens.
  - `-c copy`: Zero re-encoding overhead.
  - `stdin(Stdio::piped())`: Enables graceful termination via `"q\n"`.
  - `stdout(Stdio::null())` & `stderr(Stdio::piped())`: Prevents raw child process output from leaking into the terminal and corrupting the TUI raw mode buffer. Stderr lines are asynchronously drained into `AppEvent::Log("[FFMPEG] ...")`.
- `SegmentWatcher` polls the session folder every second:
  - Finds `.ts` files sorted lexicographically (`chunk_0000.ts`, `chunk_0001.ts`, ...).
  - Emits chunk $N$ as sealed only when chunk $N+1$ exists with size $> 0$.
  - When the child process exits (`is_stream_finished = true`), seals the final lingering chunk.

### 3.3. Real-Time Chat Recording & Flash Longevity Buffer (`src/chzzk/chat.rs` & `src/recorder/chat_writer.rs`)
- When `settings.general.record_chat` is enabled and `info.chat_channel_id` is present:
  1. Requests chat access token from `https://comm-api.game.naver.com/nng_main/v1/chats/access-token`.
  2. Spawns `ChzzkChatClient` connecting via WebSocket to `wss://kr-ss{n}.chat.naver.com/chat`.
  3. Sends `cmd: 100` (`CONNECT`) handshake with 10-second read timeout. Automatically handles server ping (`cmd: 0` $\to$ pong `cmd: 10000`) and 20-second client heartbeat pings.
  4. Parses chat messages (`cmd: 93101`, `93102`), extracting timestamp, user info, message text, donations (cheese), and raw payload.
  5. Telemetry counts are relayed non-blockingly (`try_send`) to `AppEvent::ChatStats`.
  6. **Flash Longevity Buffer (`ChatWriter`)**: Messages are queued in memory and written to `<session_dir>/chat.jsonl` using dual-trigger flushing (500 messages or 64 KB capacity, or periodic timer interval `chat_flush_interval_seconds`). No per-message `fsync` is performed during streaming.
  7. On session cancellation or stream termination, flushes all remaining records, guaranteeing zero lost messages.

### 3.4. Google Drive Upload Pipeline & Title History Sync (`src/drive/` & `src/uploader/`)
- Sealed chunks are sent over an `mpsc::Sender<UploadTask>` channel to `spawn_upload_consumer_with_concurrency`.
- **Per-Channel Serialization & Cross-Channel Concurrency**: To prevent uplink bandwidth contention, disk accumulation, and Google Drive segment ordering disruption, chunks belonging to the same channel are strictly serialized in FIFO order. Independent channels upload concurrently up to `concurrency` (default: 3) using fair round-robin scheduling.
- **High-Throughput Streaming Buffer**: Resumable file streaming uses an 8 MiB buffer (`RESUMABLE_UPLOAD_BUFFER_SIZE`, 32 * 256 KiB) via `FramedRead`, eliminating thread-pool switching overhead and saturating uplink bandwidth on high-speed networks.
- Drive subfolders are created lazily by `process_sealed_chunk` only when the first valid chunk is confirmed sealed.
- **Stream Title History**: If the broadcast title changes during a session, the engine appends the timestamped change to `title_history.txt`, and asynchronously renames the Google Drive folder via `drive.rename_folder`.
- Resumable upload initiates with a `POST /upload/drive/v3/files?uploadType=resumable` metadata request, obtaining a session URI.
- The chunk byte stream is transmitted with progress tracking callbacks updating `AppEvent::UploadProgress`.
- On HTTP 200/201 response, `tokio::fs::remove_file(&chunk_path)` executes immediately.
- Upon recording session end, `chat.jsonl` is uploaded to the broadcast's Google Drive folder and deleted locally. If Drive is disabled, `chat.jsonl` is preserved on local disk.

### 3.5. Ratatui TUI Dashboard (`src/tui/`)
- Uses strict vertical layout constraints:
  - Header & Divider: `Constraint::Length(1)` each
  - Body: `Constraint::Length(10)` (or `Constraint::Fill(1)` when logs are hidden via 'l' key; horizontal split: Monitored Channels on left, Cloud Upload progress/status on right)
  - Logs Title Divider: `Constraint::Length(1)` & Logs Content: `Constraint::Fill(1)` (omitted when logs are toggled off via 'l' key)
  - Footer Divider & Keybind Footer: `Constraint::Length(1)` each
- **Channel Row Telemetry**: Active recordings display segment count, elapsed duration, and live chat message count (`CHAT: {count}`).
- **Log Slicing & Clipping**: Logs are flattened by `\n` to support multi-line error traces, and sliced to exactly `inner_height = log_area.height`. Lines exceeding `inner_width` are horizontally truncated with `…` to guarantee zero word-wrap overflow or terminal buffer scrolling. `[CHAT]` logs are rendered with a distinct cyan badge.
- **Log Scrolling**: Tail auto-scroll is maintained by default (`log_scroll = 0`). Users can navigate history using `PageUp`, `PageDown`, `Home`, and `End`.
- **View Scrolling**: Channels and Cloud Upload scroll in synchronized lockstep using `Up`, `Down`, `k`, and `j`.

---

## 4. CI/CD & Multi-Platform Distribution Architecture

### 4.1. CI/CD Workflow Architecture (`.github/workflows/`)
The repository uses GitHub Actions for continuous integration and automated multi-platform release publishing:

1. **Continuous Integration (`.github/workflows/ci.yml`)**:
   - Triggers automatically on pushes and pull requests targeting the `main` branch.
   - **`lint` job**: Runs `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` on `ubuntu-latest`.
   - **`test` job**: Matrix build running `cargo test --all-targets` across `ubuntu-latest` and `windows-latest`. Installs FFmpeg via `FedericoCarboni/setup-ffmpeg@v3` with `github-token: ${{ secrets.GITHUB_TOKEN }}` to guarantee FFmpeg availability across all runner OSes.
   - **`npm-test` job**: Sets up Node.js 20 on `ubuntu-latest` and executes `node scripts/test-npm-packages.js` to verify npm package generation, platform resolution, and launcher mechanics.

2. **Automated Multi-Platform Release Pipeline (`.github/workflows/release.yml`)**:
   - Triggers on tag pushes matching `v*` (e.g., `v0.1.0` or `v0.2.0-beta.1`) or manual trigger via `workflow_dispatch`.
   - **`get-version`**: Resolves semver version from git tag or falls back to `Cargo.toml`. Automatically detects pre-releases (via SemVer hyphen e.g. `0.2.0-beta.1` or workflow inputs) and determines the npm distribution tag (e.g. `beta`, `rc`, `alpha`, or fallback to `next`, defaulting to `latest` for stable releases).
   - **`build-linux`**: Runs on `ubuntu-latest`. Uses Zig and `cargo-zigbuild` to compile static musl binaries for `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`.
   - **`build-macos`**: Runs on `macos-14` (Apple Silicon runner). Compiles native binaries for `x86_64-apple-darwin` and `aarch64-apple-darwin`.
   - **`build-windows`**: Runs on `windows-latest`. Compiles native 64-bit binary for `x86_64-pc-windows-msvc`.
   - **`github-release`**: Consolidates SHA256 checksums into `SHA256SUMS.txt`, collects archives (`.zip` for Windows, `.tar.gz` for Linux and macOS), and publishes a GitHub Release using `softprops/action-gh-release@v2`. Correctly marks pre-releases (`prerelease: true`, `make_latest: false`).
   - **`publish-npm`**: Downloads raw binaries from all platform builds, executes `node scripts/prepare-npm.js` to generate platform packages and configure `optionalDependencies`, and publishes all platform packages and the root wrapper package to the npm registry with provenance under the resolved distribution tag (`--tag <dist-tag>`).

### 4.2. Linux Cross-Compilation with `cargo-zigbuild`
Instead of heavy Docker containers or slow QEMU system emulation for building ARM64 Linux binaries, the CI pipeline uses `cargo-zigbuild`:
- **Lightweight Zig Toolchain**: `mlugg/setup-zig` installs Zig 0.13.0, which acts as a zero-dependency C/C++ cross-compiler and linker.
- **Static Musl Binaries**: Binaries are compiled against `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`, producing fully self-contained static executables with zero glibc runtime dependencies. This guarantees maximum portability across Linux distributions (Alpine, Ubuntu, Debian, CentOS, etc.) and ARM architectures (e.g., Raspberry Pi, AWS Graviton).
- **Fast Build Times**: Eliminates Docker container startup latency and QEMU CPU emulation overhead.

### 4.3. npm Multi-Package Distribution Structure
`chzzk-load` is distributed on npm using the modern multi-package pattern (similar to `esbuild` and `@swc/core`):
- **Root Wrapper Package (`npm/chzzk-load`)**:
  - Exposes the CLI executable via `bin: { "chzzk-load": "bin/chzzk-load.js" }`.
  - Declares 5 platform-specific binary packages as `optionalDependencies`:
    - `chzzk-load-windows-x64` (`x86_64-pc-windows-msvc`)
    - `chzzk-load-linux-x64` (`x86_64-unknown-linux-musl`)
    - `chzzk-load-linux-arm64` (`aarch64-unknown-linux-musl`)
    - `chzzk-load-darwin-x64` (`x86_64-apple-darwin`)
    - `chzzk-load-darwin-arm64` (`aarch64-apple-darwin`)
  - When a user runs `npx chzzk-load` or `npm install -g chzzk-load`, the npm package manager automatically downloads only the platform package matching their OS and CPU architecture.
- **Binary Launcher (`npm/chzzk-load/bin/chzzk-load.js`)**:
  - Inspects `process.platform` and `process.arch` to determine the target package name.
  - Resolves the binary path from `node_modules`, checking environment variable override `CHZZK_LOAD_BIN`, root `bin/` fallback, and system `PATH`.
  - Ensures execute permissions (`chmod 0o755`) on POSIX environments.
  - Spawns the native binary with inherited `stdio`, passing through CLI arguments, forwarding exit codes, and relaying termination signals (`SIGINT`, `SIGTERM`, `SIGHUP`).
- **Preparation Script (`scripts/prepare-npm.js`)**:
  - Dynamically constructs platform packages under `npm/platforms/` with appropriate `os`, `cpu`, and `libc` fields in their `package.json`.
  - Synchronizes versions across root and platform packages from CLI argument or `Cargo.toml`.
  - Copies native binary files into their respective platform packages.
- **npm OIDC Trusted Publisher Authentication**:
  - Uses GitHub Actions OpenID Connect (OIDC) Trusted Publisher authentication. Long-lived `NPM_TOKEN` secrets are completely eliminated.
  - To configure: In package settings on [npmjs.com](https://www.npmjs.com) under **Trusted Publishers**, register GitHub Actions with Repository: `ghfhffh12345/chzzk-load` and Workflow: `release.yml`.
  - The workflow automatically requests short-lived cryptographic tokens from the npm registry and publishes packages with verifiable supply-chain `--provenance`.

---

## 5. Development & Testing Workflow

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

# Run specific test suites
cargo test --test test_engine_chat
cargo test --test test_chat_client
cargo test --test test_chat_writer
cargo test --test test_engine_events
cargo test --test test_engine_events test_engine_orchestrator_prevents_duplicate_session_race_condition

# Run linter (must pass with 0 warnings)
cargo clippy --all-targets -- -D warnings

# Format code
cargo fmt --check

# Run npm package packaging and launcher test suite
node scripts/test-npm-packages.js

# Compile optimized standalone binary
cargo build --release

# Smoke test release binary
target/release/chzzk-load.exe --help
```

---

## 6. Architectural Invariants for Agents

When implementing changes, AI agents must strictly preserve the following rules:

1. **No Direct Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs while the TUI is active. All diagnostic output must be routed through `AppEvent::Log(...)`.
2. **Terminal Panic Recovery**: Maintain the panic hook in `src/main.rs` that calls `disable_raw_mode()` and `execute!(stdout, LeaveAlternateScreen, Show)` before invoking the default panic handler.
3. **MPEG-TS Stream Copy**: Never introduce re-encoding flags (`-c:v libx264`, etc.) into `build_ffmpeg_command`. Recording must remain strictly lossless stream-copy (`-c copy`).
4. **Resilient HTTP & WebSocket Mocking**: In unit tests, avoid binding fixed ports or connecting to external network endpoints. Use `tiny_http::Server::http("127.0.0.1:0")` or ephemeral `tokio::net::TcpListener::bind("127.0.0.1:0")` to allocate dynamic local test ports.
5. **Safe File Operations**: All tests performing filesystem mutations must operate strictly within `std::env::temp_dir()`.
6. **Path Portability & Resolution**: Always resolve relative file and directory paths using `app_path::resolve_path(...)`. Resolution prioritizes the current working directory (`CWD`), falls back to the executable directory when present in portable non-npm deployments, and avoids writing/resolving configuration inside `node_modules` when installed globally via npm.
7. **Flash Memory & Disk Wear Longevity**: Chat messages MUST NOT be synchronously flushed or fsynced to disk per message. All streaming chat writes must pass through `ChatWriter` with batching thresholds (500 msgs or 64 KB capacity, or periodic timer interval).
8. **Non-Blocking Telemetry Backpressure**: Never block internal WebSocket reading or recording loops on TUI event channels (`try_send` should always be used for telemetry and stats reporting).
9. **FFmpeg Binary Resolution**: `build_ffmpeg_command` must respect the `CHZZK_LOAD_FFMPEG_BIN` environment variable override before defaulting to `"ffmpeg"`.
