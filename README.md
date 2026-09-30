# chzzk-load

**English** | [한국어](README.ko.md)

[![npm version](https://img.shields.io/npm/v/chzzk-load.svg?logo=npm)](https://www.npmjs.com/package/chzzk-load)
[![GitHub Release](https://img.shields.io/github/v/release/ghfhffh12345/chzzk-load?logo=github)](https://github.com/ghfhffh12345/chzzk-load/releases)
[![CI](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml/badge.svg)](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A high-performance, standalone tool for automated Naver Chzzk live stream recording, real-time live chat archiving, and cloud storage syncing (via [rclone](https://rclone.org/)), featuring an interactive Terminal User Interface (TUI) powered by [Ratatui](https://github.com/ratatui/ratatui).

`chzzk-load` monitors live broadcasts, losslessly segments video streams into MPEG-TS chunks via FFmpeg stream-copy (`-c copy`), concurrently archives live chat via WebSocket into structured JSON Lines (`chat_%04d.jsonl`), concurrently uploads completed chunks and logs to cloud storage via rclone (or keeps them locally in local-only mode), and immediately deletes local files upon confirmed upload to maintain a strictly bounded disk footprint.

![chzzk-load TUI Dashboard](assets/tui-preview.png)

---

## Key Features

- ⚡ **Lossless Stream-Copy & Clean EOF (`-c copy`)**: Segments live HLS video streams into `.ts` chunks with zero CPU transcoding overhead. Omits reconnect flags to eliminate infinite HLS manifest EOF retry loops upon natural stream completion.
- 🌐 **Direct CDN Stream Extraction (P2P/Grid Bypass)**: Automatically decodes base64-encoded `cdn_url` parameters from Chzzk `p2pPath` playlists, pulling direct 1080p/720p CDN HLS streams without requiring P2P or grid software.
- 💬 **Real-Time Live Chat Recording (`chat_%04d.jsonl`)**: Simultaneously captures live chat via WebSocket into structured JSON Lines format segmented into time-aligned chunks, preserving timestamps, user nicknames, badges, donations/cheeses, and message text.
- 💽 **Flash-Friendly Batched I/O (SBC Optimized)**: Minimizes write cycles to protect microSD card and flash storage longevity on Single Board Computers (Raspberry Pi, ARM64) using in-memory byte buffering with dual-trigger flushing (500 messages / 64 KB capacity, or periodic timer interval).
- 🏷️ **Dynamic Title Tracking & History Sync**: Detects stream title changes during broadcasts, records them to `title_history.txt`, and automatically updates cloud storage via rclone in real-time.
- 💾 **Strictly Bounded Disk Footprint**: Only 1–2 video segments and at most 1 chat chunk reside on disk simultaneously per active stream. Chunks are permanently deleted immediately upon verified cloud upload.
- 🛡️ **N+1 Segment Boundary Safety**: Chunk $N$ is sealed and uploaded only when chunk $N+1$ exists on disk with size $> 0$, preventing partial or corrupted uploads.
- ☁️ **Universal Cloud Storage Sync via Rclone**: Seamless cloud synchronization powered by [rclone](https://rclone.org/), supporting 70+ storage providers including Google Drive, OneDrive, Amazon S3, Dropbox, WebDAV, SFTP, and local paths. Runs in **local-only recording mode** when cloud sync is disabled (`remote_path: ""`).
- 🔀 **Intra-Channel FIFO Serialization & Multi-Stream Concurrency**: Guarantees segments belonging to the same stream upload strictly in sequential order while uploading across different channels concurrently (up to `upload_concurrency`, default: 3).
- 🖥️ **Event-Driven Terminal Dashboard**: Powered by `crossterm::event::EventStream` with zero-allocation rendering, real-time channel states, live stream titles, chat message counters, upload progress gauges, transfer speed metrics, header statistics (active recordings, total duration, archived size), collapsible activity logs (`l` key), and native Windows UTF-8 console support.
- 🔄 **Anti-Race Cache Protection**: Enforces post-recording cooldown and tracks broadcast session IDs to prevent duplicate recording triggers caused by CDN cache TTL delays.

---

## Prerequisites

- **FFmpeg**: Must be installed and accessible on your system's `PATH` (or configured via the `CHZZK_LOAD_FFMPEG_BIN` environment variable).
- **Rclone**: (Optional for local-only mode, required for cloud upload) Must be installed and accessible on your system's `PATH` (or configured via `settings.toml` `rclone.rclone_bin` or the `CHZZK_LOAD_RCLONE_BIN` environment variable).

```bash
ffmpeg -version
rclone version
```

---

## Installation & Quick Start

Install globally via npm:

```bash
npm install -g chzzk-load
```

Start the application:

```bash
# Run with default settings (automatically creates settings.toml if missing)
chzzk-load

# Or specify a custom configuration file
chzzk-load --config /path/to/my-settings.toml
```

On first startup, `chzzk-load` generates a default `settings.toml` template in the current working directory if one does not exist.

---

## Configuration (`settings.toml`)

```toml
# chzzk-load configuration

[general]
chunk_duration_seconds = 600
poll_interval_seconds = 20
stream_cooldown_seconds = 60
recordings_dir = "recordings"
min_free_disk_gb = 2.0
record_chat = true
chat_flush_interval_seconds = 30

[rclone]
remote_path = "remote:chzzk"
upload_concurrency = 3
rclone_bin = "rclone"
extra_args = []

[chzzk]
nid_aut = ""
nid_ses = ""

# Channels can be defined with an optional custom alias:
[[channels]]
id = "4c3b44869c9b1399723ec28ec236f736"
alias = "SampleStreamer"

# Or using shorthand string syntax (official channel name is resolved automatically):
# channels = ["4c3b44869c9b1399723ec28ec236f736"]
```

### Key Settings

| Field | Default | Description |
| :--- | :--- | :--- |
| `general.chunk_duration_seconds` | `600` (10m) | Duration in seconds for each video chunk. |
| `general.poll_interval_seconds` | `20` | Interval in seconds between live broadcast status checks. |
| `general.stream_cooldown_seconds` | `60` | Post-stream cooldown to avoid duplicate sessions from CDN caching. |
| `general.recordings_dir` | `"recordings"` | Local folder for temporary video segments and chat logs. |
| `general.min_free_disk_gb` | `2.0` | Minimum required free disk space in GB to continue recording. |
| `general.record_chat` | `true` | Enable concurrent real-time live chat recording into time-aligned `chat_%04d.jsonl` chunks. |
| `general.chat_flush_interval_seconds` | `30` | Periodic timer interval in seconds to flush buffered chat messages to disk. |
| `rclone.remote_path` | `"remote:chzzk"` | Destination remote and folder path in rclone format (`<remote>:<path>`). Set to `""` for **local-only recording mode**. |
| `rclone.upload_concurrency` | `3` | Maximum number of concurrent channel upload streams (intra-channel uploads remain strictly serialized). |
| `rclone.rclone_bin` | `"rclone"` | Path or command name for the rclone executable. |
| `rclone.extra_args` | `[]` | Optional extra CLI flags passed to rclone invocations (e.g. `["--drive-chunk-size=64M"]`). |
| `chzzk.nid_aut` / `nid_ses` | `""` | Optional Naver session cookies for adult/subscriber-only streams. |
| `channels` | - | Monitored Chzzk channels. Specify `[[channels]]` with `id` and optional `alias`, or use shorthand string syntax `channels = ["<id>"]` (official streamer name is resolved automatically from the API). |

### Recommended Configuration for SBCs (Raspberry Pi, etc.)

For Single Board Computers (such as a Raspberry Pi or ARM64 board running Linux from a microSD card), it is strongly recommended to set `recordings_dir` to a RAM disk (e.g. `/dev/shm/chzzk-load`), shorten `chunk_duration_seconds` to `120`, and limit `upload_concurrency` to `2`.

Because `chzzk-load` maintains only 1–2 video segments and active chat chunks locally and deletes them immediately upon confirmed cloud upload, using `/dev/shm` buffers temporary chunks in memory and uploads them directly to cloud storage, completely eliminating flash storage wear and protecting microSD card longevity:

```toml
[general]
chunk_duration_seconds = 120
poll_interval_seconds = 20
stream_cooldown_seconds = 0
recordings_dir = "/dev/shm/chzzk-load"
min_free_disk_gb = 2.0
record_chat = true
chat_flush_interval_seconds = 30

[rclone]
remote_path = "remote:chzzk"
upload_concurrency = 2
rclone_bin = "rclone"
extra_args = []

[chzzk]
nid_aut = ""
nid_ses = ""

[[channels]]
id = "4c3b44869c9b1399723ec28ec236f736"
alias = "SampleStreamer"
```

- **`recordings_dir: "/dev/shm/chzzk-load"`**: Points to Linux shared memory (RAM disk / tmpfs). Video chunks and chat logs are buffered in RAM and deleted immediately upon verified upload, resulting in zero disk writes to your microSD card.
- **`chunk_duration_seconds: 120`**: Shorter 2-minute segments keep in-memory chunk sizes small (~50–100 MB at 1080p60), safely fitting within limited SBC RAM.
- **`stream_cooldown_seconds: 0`**: Bypasses extra cooldown wait times between sessions.
- **`upload_concurrency: 2`**: Bounded upload concurrency prevents network and CPU contention on resource-constrained devices.
- **`chat_flush_interval_seconds: 30`**: Batches real-time chat messages in memory and flushes periodically, reducing I/O operations.

---

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `CHZZK_LOAD_FFMPEG_BIN` | Custom path to the FFmpeg executable (defaults to `ffmpeg` on `PATH`). |
| `CHZZK_LOAD_RCLONE_BIN` | Custom path to the rclone executable (overrides `rclone.rclone_bin` and system `PATH`). |
| `CHZZK_LOAD_BIN` | Path override for the native `chzzk-load` binary when running via the npm launcher. |

---

## Cloud Storage Setup (rclone)

If `remote_path` is left empty (`""`), `chzzk-load` automatically runs in **local-only recording mode** and preserves `.ts` files and `chat_%04d.jsonl` chunks in `recordings_dir`.

To enable automatic cloud storage upload:
1. Install [rclone](https://rclone.org/downloads/) on your system:
   - **Windows**: `winget install Rclone.Rclone` or `choco install rclone`
   - **macOS**: `brew install rclone`
   - **Linux**: `sudo apt install rclone` or `curl https://rclone.org/install.sh | sudo bash`
2. Run `rclone config` in your terminal to configure your desired cloud storage remote (e.g. `gdrive` for Google Drive, `onedrive` for Microsoft OneDrive, `s3` for AWS S3, etc.). Follow the interactive prompts provided by rclone.
3. Test your remote connection:
   ```bash
   rclone lsd gdrive:
   ```
4. Set `remote_path` in `settings.toml` to your target remote and destination folder (e.g. `remote_path = "remote:chzzk"` or `remote_path = "onedrive:Recordings"`).
5. Run `chzzk-load`. The application will verify the rclone remote connection on startup and stream completed segments to your cloud storage.

---

## Keyboard Shortcuts

| Key | Action |
| :--- | :--- |
| `q` | **Quit**: Initiates graceful shutdown (stops active recordings, flushes chat buffer, and completes pending uploads). Press `q` or `Ctrl+C` again to force exit immediately. |
| `l` | **Toggle Logs**: Show or hide the activity log section (expands channels and cloud upload views when hidden). |
| `r` | **Refresh**: Immediately polls monitored channels. |
| `↑` / `k` | **Scroll Up**: Scroll visible channels and cloud uploads upward. |
| `↓` / `j` | **Scroll Down**: Scroll visible channels and cloud uploads downward. |
| `PageUp` / `PageDown` | **Scroll Logs**: Move activity logs up/down by 5 lines. |
| `Home` / `End` | **Log Navigation**: Jump to top (oldest) or bottom (latest, re-enables auto-tail). |

---

## License

Distributed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
