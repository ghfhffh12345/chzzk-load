# chzzk-load Design Specification

- **Date**: 2026-09-20
- **Status**: Approved Design
- **Target Platform**: Windows (x86_64 standalone binary), Linux/macOS compatible
- **Language & Runtime**: Rust (Edition 2024 / rustc 1.85+), Tokio async runtime

---

## 1. System Overview & Objectives

`chzzk-load` is a high-performance, standalone terminal application written in Rust designed to continuously monitor live streams on Chzzk (Naver's live streaming platform), record live broadcasts in real-time, segment them on the fly into lossless MPEG-TS chunks, stream-upload completed chunks directly to Google Drive using Google Drive API v3, and immediately delete the local chunks upon verified upload.

### Key Objectives
1. **Bounded Disk Footprint**: Strictly constrain local disk consumption to at most 1–2 active segments (~400MB–800MB total) regardless of broadcast duration (e.g. 12+ hour marathon streams).
2. **Lossless, Zero-CPU Recording**: Leverage FFmpeg's stream-copy (`-c copy`) without re-encoding to preserve original broadcast quality (up to 1080p60) with negligible CPU and memory usage.
3. **Crash & Cut Resilience**: Utilize MPEG-TS (`.ts`) container format so partial segments or unexpected drops do not corrupt video files or require finalizing metadata headers (avoiding MP4 `moov` atom corruption).
4. **Resumable Cloud Sync**: Employ Google Drive v3 Resumable Upload protocol with automatic token refresh, chunk progress tracking, and exponential backoff retry.
5. **Modern, Responsive TUI**: Deliver a dashboard UI powered by `ratatui` and `crossterm` providing real-time status of monitored channels, active recording metrics, live upload speed/progress, disk consumption gauges, and a scrollable event log.
6. **Zero-Dependency Standalone Binary**: Compiles into a single standalone executable (`chzzk-load.exe`). Requires no `cargo` or Rust environment at runtime, reads portable configuration from a dedicated `settings.json` file, and resolves paths relative to the executable location.

---

## 2. Architecture & Component Decomposition

```
┌────────────────────────────────────────────────────────────────────────┐
│                        chzzk-load TUI Layer                            │
│                 (Ratatui + Crossterm Event Loop)                       │
└───────────────────▲────────────────────────────────▲───────────────────┘
                    │ AppEvent::*                    │ AppEvent::*
                    │ (tokio::sync::mpsc)            │ (tokio::sync::mpsc)
┌───────────────────┴────────────────┐   ┌───────────┴───────────────────┐
│        Recording Engine            │   │         Upload Engine         │
│  - Chzzk Poller (reqwest)          │   │  - OAuth2 Token Provider      │
│  - HLS Stream Resolver             │   │  - Folder Hierarchy Manager   │
│  - FFmpeg Process Lifecycle        │   │  - Resumable Upload Worker    │
│  - N+1 Segment Detector            │   │  - Immediate Deletion Worker  │
└───────────────────▲────────────────┘   └───────────▲───────────────────┘
                    │ Subprocess                     │ REST API
┌───────────────────┴────────────────┐   ┌───────────┴───────────────────┐
│        FFmpeg Subprocess           │   │      Google Drive API         │
│   (Lossless HLS -> MPEG-TS chunks) │   │    (Resumable Chunk Sync)     │
└────────────────────────────────────┘   └───────────────────────────────┘
```

The system is decomposed into four isolated, asynchronous layers:

1. **Config & Environment Layer (`config.rs`, `app_path.rs`)**:
   - Locates and loads `settings.json`.
   - Resolves portable base directories relative to `std::env::current_exe()`.
   - Validates existence of `ffmpeg` on `PATH` or local folder.
2. **Recording Engine (`chzzk/`, `recorder/`)**:
   - `ChzzkClient`: Polls channel live status, extracts playback JSON, picks highest available resolution HLS `.m3u8` URL.
   - `FfmpegRecorder`: Spawns and supervises `ffmpeg` subprocess with standard segment muxer parameters.
   - `SegmentWatcher`: Detects completed chunks using the deterministic N+1 chunk boundary rule.
3. **Upload Engine (`drive/`, `uploader/`)**:
   - `DriveAuth`: Handles OAuth 2.0 loopback redirect authorization flow, token acquisition, local caching (`token.json`), and silent background refresh.
   - `DriveClient`: Implements Google Drive v3 REST API (folder creation/lookup, resumable upload initialization, byte-stream chunk transfer with progress tracking).
   - `UploadWorker`: Consumes sealed chunk tasks from a FIFO channel, uploads to Drive, verifies 200/201 response, and deletes local chunk file immediately.
4. **TUI Presentation Layer (`tui/`, `ui/`)**:
   - Consumes typed `AppEvent` items from Tokio MPSC channels.
   - Updates reactive `AppState` and renders Ratatui widgets (channel table, active recording card, upload gauge, activity log) at a 30 FPS tick rate.

---

## 3. Chzzk Polling & Stream Resolution

### 3.1 Polling Endpoint
- **URL**: `https://api.chzzk.naver.com/service/v2/channels/{channel_id}/live-detail`
- **Request Headers**:
  - `User-Agent`: Modern Chrome Windows UA (e.g. `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...`)
  - Optional `Cookie`: `NID_AUT=...; NID_SES=...` (for 19+ age-restricted broadcasts)

### 3.2 Response Deserialization
```json
{
  "code": 200,
  "content": {
    "status": "OPEN",
    "liveTitle": "Broadcast Title",
    "liveCategoryValue": "Category / Game",
    "channel": {
      "channelId": "4c3b44869c9b1399723ec28ec236f736",
      "channelName": "StreamerNickname"
    },
    "livePlaybackJson": "{\"media\": [{\"mediaId\": \"HLS\", \"path\": \"https://.../master.m3u8\", \"encodingTrack\": [{\"encodingTrackId\": \"1080p\", \"path\": \"...\"}]}]}"
  }
}
```
* If `content.status == "OPEN"`, the channel is live.
* The string inside `content.livePlaybackJson` is parsed as a nested JSON object.
* The engine selects the media entry with `mediaId == "HLS"` and extracts the highest resolution stream URL (prioritizing 1080p -> 720p -> master).

---

## 4. Real-Time Segmentation & N+1 Chunk Detection

### 4.1 FFmpeg Execution
When a stream transitions to `OPEN`, a unique session directory is created:
`{recordings_dir}/{channel_id}_{timestamp}/`

FFmpeg is spawned asynchronously via `tokio::process::Command`:
```bash
ffmpeg -hide_banner -loglevel warning -y \
  -headers "User-Agent: Mozilla/5.0 ...\r\nCookie: ...\r\n" \
  -i "<m3u8_url>" \
  -c copy \
  -f segment \
  -segment_time <chunk_duration_seconds> \
  -segment_format mpegts \
  -reset_timestamps 1 \
  -strftime 0 \
  "{recordings_dir}/{channel_id}_{timestamp}/chunk_%04d.ts"
```

### 4.2 Deterministic N+1 Chunk Completion Rule
* FFmpeg's `segment` muxer writes to `chunk_0000.ts`. When `chunk_duration_seconds` elapses or a keyframe boundary is hit, FFmpeg closes `chunk_0000.ts` and opens `chunk_0001.ts`.
* The `SegmentWatcher` scans the session directory every 1 second:
  - If file `chunk_{N+1}.ts` exists and its length is > 0 bytes:
    - `chunk_{N}.ts` is guaranteed to be completely written, flushed, and closed by the OS.
    - If `chunk_{N}.ts` has not yet been enqueued, `SegmentWatcher` pushes an `UploadTask` for `chunk_{N}.ts` to the upload channel.
* When the live stream concludes and FFmpeg exits:
  - The final active chunk `chunk_{latest}.ts` is verified for size > 0 and pushed to the upload queue.
  - The session is marked closed.

---

## 5. Google Drive Resumable Upload & Zero-Leak Cleanup

### 5.1 OAuth 2.0 Authorization & Token Provider
* **Configuration**: `credentials_path` (points to Google Cloud OAuth 2.0 Client credentials JSON) and `token_path` (stored authorized token).
* **First-Time Setup**:
  - The app spins up a local loopback listener on `http://127.0.0.1:8085/oauth2callback`.
  - Generates PKCE code verifier and challenge.
  - Prompts authorization URL via default browser.
  - Receives redirect code, exchanges for `access_token` and `refresh_token`, and writes `token.json`.
* **Silent Token Refresh**:
  - Prior to initiating an upload, `DriveAuth` verifies token validity. If within 5 minutes of expiration, it issues a refresh request to Google's OAuth endpoint and updates `token.json`.

### 5.2 Folder Management
1. Root Folder: Checks for an existing Drive folder with name specified in `root_folder_name` (default: `Chzzk_Recordings`). If absent, creates it via `POST https://www.googleapis.com/drive/v3/files`.
2. Session Subfolder: When recording starts, creates:
   `[YYYY-MM-DD_HHmm] {StreamerName} - {StreamTitle}`
   (Sanitizing invalid filename characters like `\ / : * ? " < > |`).
   Folder ID is retained in memory for that recording session.

### 5.3 Resumable Upload Workflow
1. **Initialize Upload**:
   - `POST https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable`
   - Headers:
     - `Authorization: Bearer <token>`
     - `Content-Type: application/json; charset=UTF-8`
     - `X-Upload-Content-Type: video/mp2t`
     - `X-Upload-Content-Length: <file_size>`
   - Body: `{"name": "chunk_0000.ts", "parents": ["<session_folder_id>"]}`
   - Google returns `200 OK` with session URL in `Location` header.
2. **Stream Chunk**:
   - Uses `reqwest::Body::wrap_stream` to stream file contents from disk.
   - A wrapper stream monitors bytes read to calculate upload speed (MB/s) and sends `AppEvent::UploadProgress` events to the TUI.
3. **Verify & Immediate Cleanup**:
   - When Google responds with `200 OK` / `201 Created` with the created file metadata:
     - The local chunk file is deleted immediately via `tokio::fs::remove_file`.
     - An `AppEvent::UploadCompleted` event is dispatched to update the TUI disk reclaimed counter.
   - If the upload fails due to network disruption:
     - Local file is **retained**.
     - Exponential backoff is triggered, querying status via `Content-Range: bytes */<total_size>` to resume where it left off.
4. **Session Teardown**:
   - Once the stream finishes and all chunks are confirmed uploaded, the empty session folder on disk is removed.

---

## 6. Standalone Binary & Configuration Schema

### 6.1 Standalone Binary Requirements
* Target binary: `chzzk-load.exe` (on Windows).
* Fully self-contained: No dependence on `cargo run`.
* Path Resolution Strategy:
  - Base directory defaults to the parent folder of `std::env::current_exe()`.
  - Allows running `chzzk-load` from any working directory, desktop shortcut, or scheduled task without broken relative paths.
  - Accepts CLI argument: `chzzk-load.exe --config <path_to_settings.json>`.

### 6.2 Dedicated Configuration File (`settings.json`)
The configuration will reside in a dedicated `settings.json` file. If the file does not exist on launch, the application creates a default template:

```json
{
  "general": {
    "chunk_duration_seconds": 600,
    "poll_interval_seconds": 20,
    "recordings_dir": "recordings",
    "min_free_disk_gb": 2.0
  },
  "google_drive": {
    "credentials_path": "credentials.json",
    "token_path": "token.json",
    "root_folder_name": "Chzzk_Recordings"
  },
  "chzzk": {
    "nid_aut": "",
    "nid_ses": ""
  },
  "channels": [
    {
      "id": "4c3b44869c9b1399723ec28ec236f736",
      "name": "TargetStreamer1"
    }
  ]
}
```

---

## 7. TUI Architecture & Dashboard Layout

### 7.1 Visual Layout (Ratatui)
```
┌ chzzk-load v0.1.0 ─────────────────────────────────────────── [Drive: Connected ●] ─ [Disk: 420 MB Used] ┐
│ Monitored Channels (2)               │ Active Recording & Upload Pipeline                                  │
│┌────────────────────────────────────┐│ Channel: StreamerXYZ (Live - 1080p60)                                │
││ ● StreamerXYZ             [ LIVE ] ││ Title:   Playing Elden Ring DLC!                                    │
││   Elden Ring DLC                   ││ Session: [2026-09-20 20:30:00] (Elapsed: 00:37:12)                  │
││   Chunk #4 recording...            ││                                                                     │
││                                    ││ Current Segment: chunk_0003.ts                                      │
││ ○ StreamerABC          [ OFFLINE ] ││ [████████████████████████████████░░░░░░░░] 07:15 / 10:00 (72%)       │
││   Last live: 4 hours ago           ││                                                                     │
││                                    ││ Active Upload: chunk_0002.ts (480.2 MB)                             │
││                                    ││ [████████████████████░░░░░░░░░░░░░░░░░░░░] 240.1 MB / 480.2 MB (50%) │
││                                    ││ Speed: 14.2 MB/s | ETA: 17s | Upload Queue: 0 pending               │
││                                    ││ Summary: 2 chunks uploaded (960 MB) • 2 deleted locally             │
│└────────────────────────────────────┘└─────────────────────────────────────────────────────────────────────┘
│ Live Activity Logs                                                                                         │
│ [20:30:00] [INFO]  Monitoring 2 configured channels...                                                     │
│ [20:30:04] [LIVE]  StreamerXYZ is OPEN: "Playing Elden Ring DLC!"                                          │
│ [20:30:05] [REC]   Spawned FFmpeg segmenter (10m TS chunks) -> ./recordings/xyz_20260920/                  │
│ [20:40:05] [REC]   chunk_0000.ts sealed (482 MB). Pushed to Drive upload queue.                            │
│ [20:40:06] [DRIVE] Resumable upload started: chunk_0000.ts to 'Chzzk_Recordings/[2026-09-20] StreamerXYZ' │
│ [20:40:35] [DRIVE] chunk_0000.ts upload confirmed (ID: 1x9Fk...).                                         │
│ [20:40:35] [CLEAN] Deleted local chunk_0000.ts. Reclaimed 482 MB disk space.                              │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 [q] Quit  [Tab] Switch Focus  [a] Add Channel  [r] Refresh Now  [↑/↓] Scroll Logs                           
```

### 7.2 Event-Driven State Machine
* **AppEvent Enum**:
  ```rust
  pub enum AppEvent {
      Tick,
      Key(crossterm::event::KeyEvent),
      ChannelStatusUpdate {
          channel_id: String,
          is_live: bool,
          title: String,
          streamer_name: String,
      },
      SegmentStarted {
          channel_id: String,
          chunk_name: String,
      },
      SegmentSealed {
          channel_id: String,
          chunk_name: String,
          size_bytes: u64,
          file_path: PathBuf,
      },
      UploadStarted {
          chunk_name: String,
          total_bytes: u64,
      },
      UploadProgress {
          chunk_name: String,
          uploaded_bytes: u64,
          total_bytes: u64,
          speed_bps: f64,
      },
      UploadCompleted {
          chunk_name: String,
          file_id: String,
          reclaimed_bytes: u64,
      },
      UploadFailed {
          chunk_name: String,
          error: String,
      },
      Log {
          level: LogLevel,
          message: String,
      },
  }
  ```
* **Thread Safety**: UI renders in the main thread with zero locks held across async await points. All background jobs produce events onto the MPSC channel.

---

## 8. Error Handling & Edge Cases

| Scenario | Behavior / Mitigation |
| :--- | :--- |
| **Stream stutter / momentary disconnect** | FFmpeg exits. Monitor detects status is still `OPEN` in Chzzk API and relaunches FFmpeg into the active session folder with resumed chunk indexing. |
| **Stream ends cleanly** | Chzzk status flips to `CLOSE`. Final chunk is sealed and queued for upload. Session folder is removed once all uploads finish. |
| **Google Drive 429 / 5xx error** | Exponential backoff (2s, 4s, 8s, up to 60s). Local chunk is **never deleted** until Google Drive confirms receipt with HTTP 200/201. |
| **Network connection down** | Uploads pause and retry. Sealed chunks remain safely on disk. TUI displays offline/reconnecting indicator. |
| **Low disk space (< 2 GB)** | Disk guardrail halts new recording sessions and flags a prominent warning in the TUI to protect the host system. |
| **Application exit (`q` / `Ctrl+C`)** | Graceful shutdown signal sent to FFmpeg (`SIGINT` / `CTRL_BREAK_EVENT`), flushing the active chunk. In-flight uploads complete before process termination. |

---

## 9. Verification & Testing Strategy

1. **Unit Tests**:
   - `chzzk::test_parse_live_detail`: Verify JSON deserialization of open/closed streams and playback JSON extraction.
   - `recorder::test_n_plus_one_detection`: Test segment watcher with synthetic files to ensure chunk N is enqueued only after chunk N+1 is written.
   - `drive::test_resumable_upload_ranges`: Validate byte range calculations and header constructions for resumable upload requests.
2. **Mock Integration Tests**:
   - Mock HTTP server for OAuth2 loopback exchange and Drive resumable upload session simulation.
3. **End-to-End Build & Execution**:
   - `cargo build --release` producing optimized standalone `target/release/chzzk-load.exe`.
   - Launch binary without `cargo` to verify automatic `settings.json` generation and path resolution.
   - Live smoke test against an active public Chzzk stream: monitor detection, FFmpeg segmentation, Drive upload, and immediate disk cleanup.
