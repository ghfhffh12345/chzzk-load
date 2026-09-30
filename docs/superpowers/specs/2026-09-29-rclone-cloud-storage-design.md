# Technical Specification: Rclone Cloud Storage Architecture

- **Date**: 2026-09-29
- **Status**: Approved Design
- **Target Version**: `chzzk-load` v0.6.0
- **Scope**: Architectural Refactoring of Cloud Upload Subsystem

---

## 1. Executive Summary & Objective

`chzzk-load` currently uploads live stream video segments (`.ts`), chat logs (`chat.jsonl`), and stream title histories (`title_history.txt`) exclusively to Google Drive via direct Google Drive API v3 calls and an interactive OAuth2 PKCE authorization flow.

This design refactors the cloud upload subsystem to use **rclone**, completely replacing the built-in Google Drive API and OAuth implementation. By delegating storage transfers to `rclone`, users can upload to any of rclone's 40+ supported cloud and network storage providers (Google Drive, Amazon S3, Microsoft OneDrive, Dropbox, Backblaze B2, WebDAV, SFTP, etc.) with standard rclone configurations.

The design adheres to **Rust 2024 edition** standards, introduces an asynchronous trait `UploadBackend` for zero-cost abstraction and deterministic testability, captures real-time upload progress via rclone's JSON log stream, maintains a strictly bounded local disk footprint, and cleanly preserves per-channel FIFO chunk serialization with multi-channel upload concurrency.

---

## 2. Architectural Invariants & Constraints

1. **Strictly Bounded Disk Footprint**: Video chunks (`.ts`) and completed chat logs (`chat.jsonl`) are deleted locally *only* after receiving explicit confirmation (exit code 0) from the upload backend. No files are deleted on transfer failure.
2. **N+1 Segment Boundary Safety**: A video chunk is sealed and enqueued for upload only when the subsequent chunk exists on disk with size $> 0$, or upon natural stream termination.
3. **Per-Channel FIFO Serialization with Cross-Channel Concurrency**: To prevent segment ordering disruption and uplink contention, chunks from the same live channel are uploaded strictly sequentially in FIFO order. Separate channels upload concurrently up to `upload_concurrency` (default: 3).
4. **Terminal & TUI Buffer Isolation**: Spawned rclone subprocesses must never leak raw stdout or stderr into the terminal, preventing corruption of the Ratatui alternate screen buffer. Stderr is parsed asynchronously for progress data or error logs.
5. **Deterministic Testing**: Engine lifecycle, concurrency, and failure recovery tests must run deterministically in CI environments without requiring an external `rclone` binary or live cloud credentials.

---

## 3. Configuration & Settings Schema

### 3.1. `RcloneConfig` (`src/config.rs`)
Replace `GoogleDriveConfig` with `RcloneConfig`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RcloneConfig {
    /// Remote destination path in rclone format (e.g. "remote:chzzk", "s3:my-bucket/recordings").
    /// If empty, chzzk-load runs in local-only recording mode.
    #[serde(default = "default_remote_path")]
    pub remote_path: String,

    /// Number of concurrent channel uploads (default: 3).
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,

    /// Executable name or absolute path for the rclone binary (default: "rclone").
    #[serde(default = "default_rclone_bin")]
    pub rclone_bin: String,

    /// Optional extra command-line arguments passed to rclone invocations.
    #[serde(default)]
    pub extra_args: Vec<String>,
}

fn default_remote_path() -> String {
    "remote:chzzk".to_string()
}
fn default_upload_concurrency() -> usize {
    3
}
fn default_rclone_bin() -> String {
    "rclone".to_string()
}

impl Default for RcloneConfig {
    fn default() -> Self {
        Self {
            remote_path: default_remote_path(),
            upload_concurrency: default_upload_concurrency(),
            rclone_bin: default_rclone_bin(),
            extra_args: Vec::new(),
        }
    }
}
```

### 3.2. Settings Root (`Settings`)
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Settings {
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub rclone: RcloneConfig,
    #[serde(default)]
    pub chzzk: ChzzkConfig,
    #[serde(default = "default_channels")]
    pub channels: Vec<ChannelConfig>,
}
```

### 3.3. Binary Resolution Precedence
1. Environment variable: `CHZZK_LOAD_RCLONE_BIN` (highest priority)
2. Configuration file: `settings.rclone.rclone_bin`
3. Fallback: `"rclone"` (resolved via system `PATH`)

### 3.4. Local-Only Recording Mode
When `settings.rclone.remote_path` is empty (`""`), cloud uploads are disabled. `chzzk-load` logs:
`"[CLOUD] 'remote_path' is empty; running in local-only recording mode"`
All recordings, chat logs, and title histories remain preserved on local disk.

---

## 4. Storage Abstraction: The `UploadBackend` Trait

### 4.1. Trait Definition (`src/uploader/backend.rs`)
An asynchronous, object-safe trait providing storage operations:

```rust
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type ProgressCallback = Box<dyn Fn(u64, u64, f64) + Send + Sync + 'static>;

pub trait UploadBackend: Send + Sync {
    /// Uploads the local file to `<remote_path>/<remote_dir>/<file_name>` and deletes the
    /// local file ONLY after receiving exit code 0 confirmation from the backend.
    /// Returns the number of bytes transferred.
    fn upload_file_and_delete<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;

    /// Uploads text metadata (e.g. title_history.txt) to `<remote_path>/<remote_dir>/<file_name>`.
    fn upload_text<'a>(
        &'a self,
        remote_dir: &'a str,
        file_name: &'a str,
        content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>>;

    /// Validates remote reachability and credentials on application startup.
    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>>;
}
```

---

## 5. Rclone CLI Backend Implementation (`src/uploader/rclone.rs`)

### 5.1. Remote Destination Formatting
Given:
- `remote_path`: `"remote:chzzk"` (or `"s3:my-bucket/subpath"`)
- `remote_dir`: `"[2026-09-29_140000] SampleStreamer - Live Broadcast"`
- `file_name`: `"chunk_0000.ts"`

The full remote destination is computed as:
```rust
pub fn format_destination(remote_path: &str, remote_dir: &str, file_name: &str) -> String {
    let trimmed_base = remote_path.trim_end_matches('/');
    if remote_dir.is_empty() {
        format!("{trimmed_base}/{file_name}")
    } else {
        format!("{trimmed_base}/{remote_dir}/{file_name}")
    }
}
```

### 5.2. Video Chunk & File Upload (`rclone copyto`)
To upload a file and guarantee bounded local disk usage:
1. Inspect local file metadata to retrieve total byte size.
2. Build command:
   ```bash
   rclone copyto <local_path> <remote_destination> \
       --use-json-log \
       --stats 250ms \
       --stats-log-level NOTICE \
       <extra_args...>
   ```
3. Configure subprocess stdio:
   - `stdin(Stdio::null())`
   - `stdout(Stdio::null())`
   - `stderr(Stdio::piped())`
4. Asynchronously read `stderr` line-by-line using `tokio::io::BufReader`.
5. Parse JSON log events:
   ```rust
   #[derive(Deserialize)]
   struct RcloneLogEntry {
       stats: Option<RcloneStats>,
       msg: Option<String>,
       level: Option<String>,
   }

   #[derive(Deserialize)]
   struct RcloneStats {
       bytes: u64,
       #[serde(rename = "totalBytes")]
       total_bytes: u64,
       speed: f64,
   }
   ```
   When `stats` is present, invoke `on_progress(stats.bytes, stats.total_bytes, stats.speed / 1_048_576.0)`.
6. Await process exit status.
   - If `status.success()`: invoke `tokio::fs::remove_file(local_path).await` and return `Ok(file_size)`.
   - If non-zero exit code: format and return `anyhow::anyhow!("rclone failed with status {status}")`. The local file is preserved.

### 5.3. Text Upload (`rclone rcat`)
Small text metadata files (such as `title_history.txt`) are uploaded without generating temporary disk files:
1. Command:
   ```bash
   rclone rcat <remote_destination> <extra_args...>
   ```
2. Set `stdin(Stdio::piped())`, `stdout(Stdio::null())`, `stderr(Stdio::piped())`.
3. Asynchronously write `content.as_bytes()` to child stdin and close stdin.
4. Await process completion and verify `status.success()`.

### 5.4. Remote Verification (`check_connection`)
Validates that the specified remote is accessible:
1. Command:
   ```bash
   rclone lsf --max-depth 1 <remote_path> <extra_args...>
   ```
2. If exit code is 0, connection is verified.
3. If it fails, capture `stderr` output and return an error detailing the misconfiguration (e.g. invalid credentials or nonexistent remote).

---

## 6. Engine Orchestrator & Session Lifecycle

### 6.1. Simplified Session State (`ActiveSessionState`)
Because rclone manages remote directory creation implicitly on upload, opaque folder IDs are eliminated:

```rust
#[derive(Debug, Clone)]
pub struct ActiveSessionState {
    pub start_timestamp: String,
    pub streamer_name: String,
    pub initial_title: String,
    pub current_title: String,
    pub title_history: Vec<(String, String)>,
}

impl ActiveSessionState {
    pub fn new(start_timestamp: String, streamer_name: String, title: String) -> Self {
        let initial_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        Self {
            start_timestamp,
            streamer_name,
            initial_title: title.clone(),
            current_title: title.clone(),
            title_history: vec![(initial_time, title)],
        }
    }

    /// Stable directory name based on initial broadcast title.
    pub fn folder_name(&self) -> String {
        let streamer = sanitize_filename(&self.streamer_name);
        let title = sanitize_filename(&self.initial_title);
        let timestamp = &self.start_timestamp;
        format!("[{timestamp}] {streamer} - {title}")
    }

    pub fn record_title_change(&mut self, new_title: String, timestamp: String) {
        self.current_title = new_title.clone();
        self.title_history.push((timestamp, new_title));
    }

    pub fn format_title_history(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        for (timestamp, title) in &self.title_history {
            let _ = writeln!(out, "[{timestamp}] {title}");
        }
        out
    }
}
```

### 6.2. Stream Title Changes
When a broadcast title change is polled:
1. Update `ActiveSessionState`: append `(timestamp, new_title)`.
2. Asynchronously upload the updated history text:
   ```rust
   if let Some(ref backend) = self.backend {
       let history_text = session.format_title_history();
       let remote_dir = session.folder_name();
       let _ = backend.upload_text(&remote_dir, "title_history.txt", &history_text).await;
   }
   ```
3. Remote directory name remains unchanged (`initial_title`), preventing expensive multi-GB re-copies or key-renames on S3, Backblaze B2, and other object storage backends.

### 6.3. Upload Task Pipeline
1. `UploadTask`:
   ```rust
   #[derive(Debug, Clone)]
   pub struct UploadTask {
       pub channel_id: String,
       pub remote_dir: String,
       pub chunk_path: PathBuf,
       pub chunk_name: String,
       pub streamer_name: String,
   }
   ```
2. The upload consumer receives `Option<Arc<dyn UploadBackend>>`.
3. Dispatches tasks per channel in FIFO order while executing up to `upload_concurrency` across independent channels.
4. Real-time progress updates are dispatched as `AppEvent::UploadProgress`.
5. Upon upload completion, emits `AppEvent::UploadCompleted`, logs reclaimed space, and removes empty local session directories.

---

## 7. TUI Telemetry & Badges

- **Log Kind**: Update `LogKind::Drive` to `LogKind::Cloud` with badge `" CLOUD  "` (styled with bold blue/cyan).
- **Log Constructor**: Replace `LogEntry::drive(msg)` with `LogEntry::cloud(msg)`.
- **TUI Dashboard**: Cloud upload status panel displays real-time chunk name, channel/streamer name, transfer speed (MB/s), progress bar (0–100%), and aggregate reclaimed MB.

---

## 8. Testing & Verification Architecture

### 8.1. `MockUploadBackend`
An in-memory mock implementation for deterministic unit and integration tests:
```rust
#[derive(Default, Clone)]
pub struct MockUploadBackend {
    pub uploads: Arc<tokio::sync::Mutex<Vec<(PathBuf, String)>>>,
    pub texts: Arc<tokio::sync::Mutex<Vec<(String, String, String)>>>,
    pub should_fail: Arc<std::sync::atomic::AtomicBool>,
}
```
- Simulates progress reporting and local file deletion upon completion.
- Replaces Google Drive `tiny_http` mock servers across all engine integration tests (`tests/test_engine_events.rs`).

### 8.2. `RcloneBackend` Unit Tests (`tests/test_rclone_backend.rs`)
- **CLI Argument Generation**: Verifies command construction, subcommands, arguments, and `extra_args`.
- **JSON Log Stream Parser**: Validates accurate parsing of progress events, speed, byte counts, and error messages from simulated rclone stderr streams.
- **Path Formatting**: Tests handling of trailing slashes, bucket names, and special characters.

### 8.3. CI Compatibility
- No external `rclone` binary or cloud authentication required for `cargo test --all-targets`.
- Validates cleanly on both `ubuntu-latest` and `windows-latest`.

---

## 9. Codebase Clean-up & Migration

1. **Delete**:
   - `src/drive.rs`
   - `src/drive/auth.rs`
   - `src/drive/client.rs`
   - `tests/test_drive_auth.rs`
   - `tests/test_drive_uploader.rs`
2. **Remove Dependencies** (`Cargo.toml`):
   - `tiny_http`
3. **Update**:
   - `src/config.rs` (`GoogleDriveConfig` $\to$ `RcloneConfig`)
   - `src/uploader.rs` (re-exports `UploadBackend`, `RcloneBackend`)
   - `src/engine.rs` (`drive` $\to$ `backend`)
   - `src/main.rs` (initialize `RcloneBackend` and check connection)
   - `src/tui/event.rs` & `src/tui/ui.rs` (`LogKind::Drive` $\to$ `LogKind::Cloud`)
   - `tests/test_config.rs` & `tests/test_engine_events.rs`
   - `README.md` & `AGENTS.md`
