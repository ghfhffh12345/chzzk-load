# chzzk-load

**English** | [한국어](README.ko.md)

[![npm version](https://img.shields.io/npm/v/chzzk-load.svg?logo=npm)](https://www.npmjs.com/package/chzzk-load)
[![GitHub Release](https://img.shields.io/github/v/release/ghfhffh12345/chzzk-load?logo=github)](https://github.com/ghfhffh12345/chzzk-load/releases)
[![CI](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml/badge.svg)](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A high-performance, standalone tool for automated Naver Chzzk live stream recording, real-time live chat archiving, post-recording media consolidation, and cloud storage syncing (via [rclone](https://rclone.org/)), featuring an interactive Terminal User Interface (TUI) powered by [Ratatui](https://github.com/ratatui/ratatui).

`chzzk-load` monitors live broadcasts, losslessly segments video streams into MPEG-TS chunks via FFmpeg stream-copy (`-c copy`), concurrently archives live chat via WebSocket into structured JSON Lines (`chat_%04d.jsonl`), concurrently uploads completed chunks and logs to cloud storage via rclone (or keeps them locally in local-only mode), and immediately deletes local files upon confirmed upload to maintain a strictly bounded disk footprint. It also includes an offline post-recording consolidation pipeline (`chzzk-load consolidate`) that losslessly remuxes recorded chunks into normalized, seekable MP4 files (`livestream.mp4`) with centralized seek tables (`+faststart`) and deduplicates chat logs (`livechat.jsonl`).

![chzzk-load TUI Dashboard](assets/tui-preview.png)

---

## Key Features

- ⚡ **Lossless Stream-Copy & Clean EOF (`-c copy`)**: Segments live HLS video streams into `.ts` chunks with zero CPU transcoding overhead. Omits reconnect flags to eliminate infinite HLS manifest EOF retry loops upon natural stream completion.
- 🌐 **Direct CDN Stream Extraction (P2P/Grid Bypass)**: Automatically decodes base64-encoded `cdn_url` parameters from Chzzk `p2pPath` playlists, pulling direct 1080p/720p CDN HLS streams without requiring P2P or grid software.
- 💬 **Real-Time Live Chat Recording (`chat_%04d.jsonl`)**: Simultaneously captures live chat via WebSocket into structured JSON Lines format segmented into time-aligned chunks, preserving timestamps, user nicknames, badges, donations/cheeses, and message text.
- 💽 **Flash-Friendly Batched I/O (SBC Optimized)**: Minimizes write cycles to protect microSD card and flash storage longevity on Single Board Computers (Raspberry Pi, ARM64) using in-memory byte buffering with dual-trigger flushing (500 messages / 64 KB capacity, or periodic timer interval).
- 📊 **Stream Metadata Event Tracking (`live_metadata.jsonl`)**: Tracks essential broadcast state transitions (title, category, tags, access tier, and playback/chat policy flags) using a lean JSON Lines snapshot format (v2) with millisecond-accurate video synchronization, synchronized in real time via the unified non-blocking upload pipeline.
- 💾 **Strictly Bounded Disk Footprint & Disk Space Guarding**: Only 1–2 video segments and at most 1 chat chunk reside on disk simultaneously per active stream. Chunks are permanently deleted immediately upon verified cloud upload. An active cross-platform circuit breaker (`min_free_disk_gb`) gracefully pauses recordings and actively evicts the oldest failed chunks in the Dead-Letter Queue (DLQ) while strictly safeguarding metadata timelines if disk space falls below safe limits.
- 🛡️ **N+1 Segment Boundary Safety**: Explicit numeric sequence parsing (`chunk_%04d.ts`) ensures chunk $N$ is sealed and uploaded only when chunk $N+1$ exists on disk with size $> 0$, preventing partial or corrupted uploads.
- 🎬 **Post-Recording Consolidation (`chzzk-load consolidate`)**: Losslessly merges and remuxes fragmented video segments (`chunk_%04d.ts`) into a seekable, timeline-normalized MP4 (`livestream.mp4` with `+faststart`) and deduplicates chat logs into `livechat.jsonl`. Supports both local recording directories and cloud storage remotes (`remote:path`) with a strictly bounded 1× local disk footprint by streaming remote chunks on demand through an ephemeral read-only HTTP loopback server (`rclone serve http`).
- 🚀 **High-Throughput Concurrent Cloud Upload & Resilient Staging**: Concurrently uploads consolidated `livestream.mp4` and `livechat.jsonl` using two-stage atomic staging (`.part` files renamed via `rclone moveto`), tuned with large-file transfer optimizations (`--drive-chunk-size 128M`, multi-threading). Features sibling process abort on failure, container structural sanity probing (`moov` atom validation) for instant upload resumption, and pre-flight disk capacity checks ($1.15\times$ required space).
- 📊 **Responsive Consolidation Telemetry**: Real-time dual-track ANSI progress bars (`VID`, `CHAT`, `UP V`, `UP C`, `DEL`) with a 10 Hz dirty render loop for remuxing, cloud uploading, and concurrent chunk purging, automatically falling back to 20% milestone boundaries and 30-second heartbeats in non-interactive/headless environments.
- 📬 **Non-Blocking Dead-Letter Queue (DLQ)**: Upload failures never block subsequent chunks or metadata snapshots. Failed tasks transfer to a background DLQ with exponential backoff (initial 2s, capped at 5m, infinite retries, capped at 20 tasks/channel in RAM), maintaining uplink progress and disk reclamation while preserving stream metadata.
- 🔄 **Startup Crash Reconciliation & Tail Chunk Quarantine**: Reconciles orphaned session folders from prior crashes or system reboots, validating contiguous sealed chunks and lingering `live_metadata.jsonl` files, re-injecting them into the Dead-Letter Queue (DLQ) for cloud upload while safely quarantining unfinalized tail segments (`.quarantine`).
- 🤖 **Headless Console Mode (`--headless` / `--no-tui`)**: Auto-detects non-TTY environments (or manual flags) to bypass TUI raw mode, streaming colorized logs to stdout for systemd services and Docker containers.
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

# Or bypass remote connection check at startup
chzzk-load --skip-rclone-check

# Run in headless console mode (ideal for background systemd/Docker services)
chzzk-load --headless

# Consolidate a local recording session
chzzk-load consolidate recordings/[2026-09-30_140000]\ [StreamerAlias]\ StreamerName\ -\ Live\ Stream\ Title

# Consolidate a remote recording session stored in cloud storage
chzzk-load consolidate remote:chzzk/[2026-09-30_140000]\ [StreamerAlias]\ StreamerName\ -\ Live\ Stream\ Title
```

On first startup, `chzzk-load` generates a default `settings.toml` template in the current working directory if one does not exist.

---

## Post-Recording Consolidation (`chzzk-load consolidate`)

Following a recording session, stream archives consist of fragmented video chunks (`chunk_%04d.ts`), chunked chat logs (`chat_%04d.jsonl`), and stream metadata (`live_metadata.jsonl`). The `consolidate` subcommand losslessly merges and remuxes these fragments into normalized, seekable container files (`livestream.mp4` and `livechat.jsonl`) with a strictly bounded local disk footprint.

```bash
# Consolidate a local recording session directory
chzzk-load consolidate recordings/[2026-09-30_140000]\ [StreamerAlias]\ StreamerName\ -\ Live\ Stream\ Title

# Consolidate a remote recording session stored in cloud storage
chzzk-load consolidate remote:chzzk/[2026-09-30_140000]\ [StreamerAlias]\ StreamerName\ -\ Live\ Stream\ Title

# Retain original video and chat chunks (skips automatic post-consolidation chunk purge)
chzzk-load consolidate recordings/... --keep-original

# Overwrite existing livestream.mp4 or livechat.jsonl if already present
chzzk-load consolidate recordings/... --overwrite

# Abort if sequence gaps or malformed chunk entries are detected
chzzk-load consolidate recordings/... --strict

# Custom staging directory on another drive (useful when system drive has limited space)
chzzk-load consolidate remote:chzzk/... --temp-dir /mnt/fast-nvme/staging

# Custom deletion concurrency when purging original chunks
chzzk-load consolidate remote:chzzk/... --delete-concurrency 32

# Run consolidation in headless mode (milestone logging for scripts/cron)
chzzk-load --headless consolidate recordings/...
```

### Consolidation CLI Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `<PATH>` | *(Required)* | Local directory path or remote rclone path (`<remote>:<path>`) to the recording session. |
| `--keep-original` | `false` | Retain original video and chat chunks after successful consolidation. When omitted, original chunks are automatically purged after verified consolidation. |
| `--overwrite` | `false` | Overwrite existing `livestream.mp4` or `livechat.jsonl` files in the target directory. |
| `--strict` | `false` | Abort consolidation if chunk sequence gaps (e.g. missing `chunk_0002.ts`) or malformed entries are encountered. |
| `--delete-concurrency <N>` | `16` | Maximum concurrent deletion tasks when purging original chunks. |
| `--temp-dir <PATH>` | System temp | Custom staging directory for temporary consolidation files (`livestream.mp4.part`, `livechat.jsonl.part`). |
| `-c`, `--config <PATH>` | Auto-detect | Path to dedicated `settings.toml` configuration file (used to resolve rclone binary, remote path, and `consolidate_extra_args`). |

### Architecture & Safety Highlights

- **Lossless Concat Demuxer Remuxing (`-c copy`)**: Uses FFmpeg's native Concat Demuxer (`-f concat -safe 0`) rather than raw byte piping, respecting transport stream packet boundaries without re-encoding video or dropping boundary frames.
- **Timestamp Normalization & Monotonic DTS**: Applies `-avoid_negative_ts make_zero` so playback starts strictly at 0.0s, eliminating initial playback freezes and audio/video desync. Monotonic bitstream filters (`VIDEO_MONOTONIC_DTS_BSF`, `AUDIO_MONOTONIC_DTS_BSF` / `aac_adtstoasc`) resolve chunk seam DTS collisions without video transcoding.
- **Desktop-Friendly Faststart Seek Table (`+faststart`)**: Places the `moov` atom header at the beginning of `livestream.mp4`, enabling instant duration indexing and smooth scrubbing in desktop media players (VLC, QuickTime, Windows Media Player) and browsers.
- **Bounded 1× Disk Footprint (Zero Local Chunk Download)**: In remote consolidation, chunks stream on demand into memory through an ephemeral read-only HTTP loopback server (`rclone serve http`) while writing seekable output directly to a local staging file (`livestream.mp4.part`), bounding local disk consumption strictly to 1× the stream size.
- **Pre-flight Capacity Verification**: Checks staging disk space prior to remuxing, requiring at least $1.15\times$ total video bytes. Fails fast with actionable guidance if capacity is insufficient.
- **Container Structural Integrity Validation**: Probes MP4 container structural integrity (`validate_mp4_container_integrity`) before promoting staged `.part` files or resuming uploads: verifies a single `moov` atom, at least one `mdat` atom, complete file coverage without trailing unindexed garbage, and ensures sample chunk offsets (`stco`/`co64`) point strictly within `mdat`.
- **Concurrent Cloud Upload & Sibling Abort**: Concurrently uploads `livestream.mp4` and `livechat.jsonl` with tuned large-file flags (`--drive-chunk-size 128M`, multi-threading). If any upload fails, sibling transfers are immediately aborted and remote `.part` files are removed while local staging files are preserved for instant resume.
- **Sliding-Window Chat Deduplication**: Deduplicates chat records concurrently with video remuxing into `livechat.jsonl`, preserving chronological message order with $O(1)$ memory consumption.
- **Concurrent Chunk Purge**: Rapidly purges original `.ts` and `.jsonl` files in parallel (`--delete-concurrency 16`) after verified consolidation and upload, keeping storage clean.

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
skip_connection_check = false
extra_args = []
consolidate_extra_args = []

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
| `rclone.skip_connection_check` | `false` | Skip remote connection check at startup (runs in background with 10s timeout by default). |
| `rclone.extra_args` | `[]` | Optional extra CLI flags passed to rclone invocations (e.g. `["--drive-chunk-size=64M"]`). |
| `rclone.consolidate_extra_args` | `[]` | Optional extra CLI flags passed to consolidation rclone upload invocations (e.g. `["--drive-chunk-size=128M"]`), separated from live recording chunk uploads. |
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

## Data Formats & File Specifications

Each recording session generates a dedicated directory structured as `[{timestamp}] [{alias}] {streamer_name} - {title}`. During active capture, the directory contains fragmented video segments, time-aligned chat logs, and broadcast metadata events:

```
recordings/
└── [2026-09-30_140000] [StreamerAlias] StreamerName - Live Stream Title/
    ├── chunk_0000.ts          # Video segment (lossless MPEG-TS)
    ├── chunk_0001.ts
    ├── chat_0000.jsonl        # Chat log segment (JSON Lines)
    ├── chat_0001.jsonl
    └── live_metadata.jsonl    # Broadcast state transitions & sync timeline
```

After running `chzzk-load consolidate`, original chunks are purged (unless `--keep-original` is specified) and replaced with consolidated, seekable files:

```
recordings/
└── [2026-09-30_140000] [StreamerAlias] StreamerName - Live Stream Title/
    ├── livestream.mp4         # Consolidated seekable MP4 video (normalized timestamps, +faststart moov atom)
    ├── livechat.jsonl         # Consolidated & deduplicated chat log (JSON Lines)
    └── live_metadata.jsonl    # Broadcast state transitions & sync timeline
```

---

### 1. Live Chat Format (`chat_%04d.jsonl`)

Live chat messages captured via WebSocket are serialized as structured JSON Lines into progressively numbered chunks (`chat_%04d.jsonl`), time-aligned with video segments (`chunk_duration_seconds`).

#### Field Schema

| Field | Type | Description |
| :--- | :--- | :--- |
| `time_ms` | `number` | Unix timestamp in milliseconds when the message was sent on Chzzk. |
| `datetime` | `string` | Local timestamp formatted as `YYYY-MM-DD HH:mm:ss`. |
| `msg_type` | `string` | Message category: `"TEXT"`, `"DONATION"`, `"SUBSCRIPTION"`, `"SYSTEM_MESSAGE"`, or `"TYPE_{code}"`. |
| `nickname` | `string` | Display nickname of the message sender. |
| `user_id_hash` | `string \| null` | Anonymized user ID hash provided by Chzzk API. |
| `content` | `string` | Raw text content of the message. |
| `donation_amount` | `number \| null` | Donated cheese amount (present for `"DONATION"` messages, `null` otherwise). |
| `extras` | `object \| null` | Parsed JSON object containing user badges, subscription tier, emojis, and pay metadata. |
| `raw` | `object` | Complete raw JSON envelope as delivered by the Chzzk WebSocket server. |

#### Example Record (Standard Chat)

```json
{
  "time_ms": 1790757912345,
  "datetime": "2026-09-30 14:05:12",
  "msg_type": "TEXT",
  "nickname": "ChzzkViewer",
  "user_id_hash": "a1b2c3d4e5f6789012345678abcdef01",
  "content": "GG! Great play!",
  "donation_amount": null,
  "extras": {
    "chatType": "STREAMING",
    "emojis": {},
    "osType": "PC",
    "streamingChannelId": "4c3b44869c9b1399723ec28ec236f736",
    "userRoleCode": "common_user"
  },
  "raw": { "cmd": 93101, "bdy": [], "tid": "1" }
}
```

#### Example Record (Donation)

```json
{
  "time_ms": 1790757920123,
  "datetime": "2026-09-30 14:05:20",
  "msg_type": "DONATION",
  "nickname": "CheeseLover",
  "user_id_hash": "b2c3d4e5f6a1789012345678abcdef02",
  "content": "Cheering you on! Here is 1,000 cheese!",
  "donation_amount": 1000,
  "extras": {
    "donationType": "CHAT",
    "payAmount": 1000,
    "payType": "CURRENCY"
  },
  "raw": { "cmd": 93102, "bdy": [], "tid": "2" }
}
```

---

### 2. Stream Metadata & Timeline Format (`live_metadata.jsonl`)

`live_metadata.jsonl` tracks essential broadcast state transitions across the lifetime of the stream with millisecond-accurate video timeline synchronization (`stream_offset_ms`) using a lean JSON Lines snapshot schema (v2). It is appended locally and synchronized to cloud storage in real time via the unified upload pipeline (`rclone copyto`). Live snapshots are retained locally during active capture (`delete_on_success: false`), while the final concluding snapshot triggers automatic local deletion (`delete_on_success: true`) once uploaded, ensuring complete cloud synchronization and strict directory emptiness.

#### Event Types

- **`INITIAL_STATE`**: Written once at stream startup (`stream_offset_ms: 0`), capturing the initial broadcast state snapshot.
- **`METADATA_CHANGED`**: Emitted whenever any tracked broadcast property changes (e.g. title update, category switch, watch party started, access tier or policy flags modified). Contains the complete updated `state` snapshot.

#### Envelope Schema

| Field | Type | Description |
| :--- | :--- | :--- |
| `version` | `number` | Schema version (`2`). |
| `event` | `string` | Event discriminator: `"INITIAL_STATE"` or `"METADATA_CHANGED"`. |
| `timestamp` | `string` | RFC 3339 / ISO 8601 UTC timestamp (`YYYY-MM-DDTHH:mm:ssZ`). |
| `stream_offset_ms`| `number` | Milliseconds elapsed since the recording session started (`0` at initial start). Syncs directly with video timestamps. |
| `state` | `object` | Complete snapshot of the lean broadcast state after the event occurred. |

> [!NOTE]
> In schema version 2, redundant local timestamps (`time_local`) and delta diff objects (`changes`) are omitted in favor of unified, full lean snapshots (`state`).

> [!NOTE]
> **Unified Upload Pipeline & DLQ Protection**: Metadata snapshots are uploaded through the primary upload queue and Dead-Letter Queue (DLQ) using `rclone copyto ... --progress`, sharing global cross-channel concurrency bounds (`upload_concurrency`). In the event of network disruption or low disk space, metadata uploads retry with exponential backoff and are strictly immune from DLQ eviction, ensuring broadcast history is never lost.

#### State Snapshot Schema (`state`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `live_id` | `number \| null` | Unique numeric broadcast session ID (omitted when null). |
| `open_date` | `string \| null` | Broadcast start timestamp from Chzzk API (`YYYY-MM-DD HH:mm:ss`, omitted when null). |
| `close_date` | `string \| null` | Broadcast end timestamp (populated upon stream completion, omitted when null). |
| `channel_id` | `string` | Monitored Chzzk channel alphanumeric ID. |
| `channel_name` | `string` | Streamer channel display name. |
| `live_title` | `string` | Broadcast title. |
| `category_type` | `string \| null` | Broad category classification (`"GAME"`, `"TALK"`, `"SPORTS"`, `"ETC"`, etc., omitted when null). |
| `live_category` | `string \| null` | Category slug identifier (e.g. `"game"`, `"talk"`, omitted when null). |
| `live_category_value`| `string \| null` | Display category/game title (e.g. `"Valorant"`, `"League of Legends"`, omitted when null). |
| `tags` | `string[]` | List of broadcast tags configured by the streamer. |
| `access_tier` | `string` | Mutually exclusive access gating tier: `"PUBLIC"`, `"ADULT_ONLY"`, `"CHEAT_KEY"`, `"NAVER_PLUS"`, `"CHANNEL_SUBSCRIPTION"`, or `"PAY_PER_VIEW"`. |
| `is_kr_only` | `boolean` | Whether stream viewing is restricted to South Korea (`kr_only_viewing`). |
| `is_chat_active` | `boolean` | Whether live chat is active for the broadcast. |
| `is_watch_party` | `boolean` | Whether broadcast is an active watch party. |
| `paid_promotion` | `boolean` | Whether paid sponsorship/advertisement is declared. |
| `drops_campaign_no`| `string \| null` | Identifier of active Drops campaign, if any (omitted when null). |

> [!TIP]
> **Lean Snapshot Design & Anti-Churn**: Schema v2 captures essential stream lifecycles and classification state while intentionally omitting volatile viewer telemetry (`concurrent_user_count`, `accumulate_count`), static CDN thumbnail URLs, and complex nested objects. This avoids disk and cloud write churn while enabling fast, zero-overhead direct state equality comparisons during polling.

#### Example Record (`METADATA_CHANGED`)

```json
{
  "version": 2,
  "event": "METADATA_CHANGED",
  "timestamp": "2026-10-03T14:35:10Z",
  "stream_offset_ms": 2110450,
  "state": {
    "live_id": 3829140,
    "open_date": "2026-10-03 23:00:00",
    "channel_id": "4c3b44869c9b1399723ec28ec236f736",
    "channel_name": "SampleStreamer",
    "live_title": "Switching to Valorant with viewers!",
    "category_type": "GAME",
    "live_category": "game",
    "live_category_value": "Valorant",
    "tags": [
      "Valorant",
      "FPS",
      "Viewers"
    ],
    "access_tier": "PUBLIC",
    "is_kr_only": false,
    "is_chat_active": true,
    "is_watch_party": false,
    "paid_promotion": false
  }
}
```

---

### 3. Consolidated Media Formats (`livestream.mp4`, `livechat.jsonl`)

Running `chzzk-load consolidate` merges fragmented stream archives into two standardized, production-ready artifacts:

#### `livestream.mp4` (Lossless Consolidated Video)
- **Container**: MPEG-4 Part 14 (`.mp4`) with faststart seek table (`-movflags +faststart`). The `moov` atom header is relocated to the start of the file, providing instant seek table indexing and smooth scrubbing in desktop players (VLC, QuickTime, Windows Media Player) and web browsers.
- **Encoding**: Lossless stream copy (`-c copy`) preserving the original H.264 video and AAC audio bitstreams from the Chzzk CDN without generational quality loss or CPU transcoding overhead.
- **Timeline Normalization**: Presentation Timestamps (PTS) and start offsets are reset to `0.0s` (`-avoid_negative_ts make_zero`), resolving initial playback freezing and audio/video desynchronization.
- **Bitstream Filtering**: Monotonic DTS bitstream filters (`VIDEO_MONOTONIC_DTS_BSF` and `AUDIO_MONOTONIC_DTS_BSF` / `aac_adtstoasc`) reconcile duplicate packets and timestamp collisions across chunk transitions while preserving B-frame display order without video transcoding.
- **Stream Mapping**: Enforces explicit single-stream mapping (`-map 0:v:0 -map 0:a:0?`), ensuring video and audio tracks are captured cleanly without ancillary metadata track errors.
- **Container Structural Integrity**: Prior to promotion or upload resume, the file is structurally validated (`validate_mp4_container_integrity`): enforces a single `moov` atom, at least one `mdat` atom, contiguous file boundary coverage without trailing unindexed garbage, and confirms sample chunk offsets (`stco`/`co64`) point strictly within the `mdat` media payload.

#### `livechat.jsonl` (Consolidated Live Chat)
- **Format**: JSON Lines (`.jsonl`), preserving the individual chat record schema (`time_ms`, `datetime`, `msg_type`, `nickname`, `user_id_hash`, `content`, `donation_amount`, `extras`, `raw`).
- **Deduplication**: Processed through sliding-window deduplication over consecutive chat chunks, eliminating duplicate messages delivered across chunk boundary transitions while strictly preserving chronological timestamp order and $O(1)$ memory consumption.

---

## Environment Variables

| Variable | Description |
| :--- | :--- |
| `CHZZK_LOAD_FFMPEG_BIN` | Custom path to the FFmpeg executable (defaults to `ffmpeg` on `PATH`). |
| `CHZZK_LOAD_RCLONE_BIN` | Custom path to the rclone executable (overrides `rclone.rclone_bin` and system `PATH`). |
| `CHZZK_LOAD_BIN` | Path override for the native `chzzk-load` binary when running via the npm launcher. |

---

## Cloud Storage Setup (rclone)

If `remote_path` is left empty (`""`), `chzzk-load` automatically runs in **local-only recording mode** and preserves `.ts` files, `chat_%04d.jsonl` chunks, and `live_metadata.jsonl` in `recordings_dir`.

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
