# chzzk-load

[![npm version](https://img.shields.io/npm/v/chzzk-load.svg?logo=npm)](https://www.npmjs.com/package/chzzk-load)
[![GitHub Release](https://img.shields.io/github/v/release/ghfhffh12345/chzzk-load?logo=github)](https://github.com/ghfhffh12345/chzzk-load/releases)
[![CI](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml/badge.svg)](https://github.com/ghfhffh12345/chzzk-load/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A high-performance, standalone tool for automated Naver Chzzk live stream recording and real-time Google Drive syncing, featuring an interactive Terminal User Interface (TUI) powered by [Ratatui](https://github.com/ratatui/ratatui).

`chzzk-load` monitors live broadcasts, losslessly segments video streams into MPEG-TS chunks via FFmpeg stream-copy (`-c copy`), concurrently uploads completed chunks to Google Drive, and immediately deletes local files upon confirmed upload to maintain a strictly bounded disk footprint.

![chzzk-load TUI Dashboard](assets/tui-preview.png)

---

## Key Features

- ⚡ **Lossless Stream-Copy (`-c copy`)**: Segments live HLS video streams into `.ts` chunks with zero CPU transcoding overhead.
- 💾 **Strictly Bounded Disk Footprint**: Only 1–2 video segments reside on disk per active stream. Chunks are permanently deleted immediately upon verified cloud upload.
- 🛡️ **N+1 Segment Boundary Safety**: Chunk $N$ is sealed and uploaded only when chunk $N+1$ exists on disk with size $> 0$, preventing partial or corrupted uploads.
- ☁️ **Resumable Google Drive Sync & Local Fallback**: Direct cloud upload via Google Drive API v3 with automatic PKCE OAuth2 authorization. Runs in local-only recording mode if Google Drive credentials are omitted.
- 🖥️ **Interactive Terminal Dashboard**: Real-time channel states, live stream titles, upload progress gauges, transfer speed metrics, disk space reclaimed counters, and live activity logs.
- 🔄 **Anti-Race Cache Protection**: Enforces post-recording cooldown and tracks broadcast session IDs to prevent duplicate recording triggers caused by CDN cache TTL delays.

---

## Prerequisites

- **FFmpeg**: Must be installed and accessible on your system's `PATH`.

```bash
ffmpeg -version
```

---

## Installation & Quick Start

Install globally via npm:

```bash
npm install -g chzzk-load
```

Start the application:

```bash
# Run with default settings (automatically creates settings.json if missing)
chzzk-load

# Or specify a custom configuration file
chzzk-load --config /path/to/my-settings.json
```

On first startup, `chzzk-load` generates a default `settings.json` template in the current working directory if one does not exist.

---

## Configuration (`settings.json`)

```json
{
  "general": {
    "chunk_duration_seconds": 600,
    "poll_interval_seconds": 20,
    "stream_cooldown_seconds": 60,
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
      "id": "1a1dd9ce56fb61a37ffb6f69f6d5b978",
      "name": "강퀴"
    }
  ]
}
```

### Key Settings

| Field | Default | Description |
| :--- | :--- | :--- |
| `general.chunk_duration_seconds` | `600` (10m) | Duration in seconds for each video chunk. |
| `general.poll_interval_seconds` | `20` | Interval in seconds between live broadcast status checks. |
| `general.stream_cooldown_seconds` | `60` | Post-stream cooldown to avoid duplicate sessions from CDN caching. |
| `general.recordings_dir` | `"recordings"` | Local folder for temporary video segments. |
| `google_drive.credentials_path` | `"credentials.json"` | Path to Google OAuth2 Desktop client secrets file. |
| `google_drive.root_folder_name` | `"Chzzk_Recordings"` | Destination folder name created in Google Drive. |
| `chzzk.nid_aut` / `nid_ses` | `""` | Optional Naver session cookies for adult/subscriber-only streams. |
| `channels` | - | List of monitored Chzzk channels (`id` from channel URL, `name` for display). |

---

## Google Drive Setup

If Google Drive credentials are not provided, `chzzk-load` automatically runs in **local-only recording mode** and preserves `.ts` files in `recordings_dir`.

To enable automatic Google Drive upload:
1. In the [Google Cloud Console](https://console.cloud.google.com/), create a project and enable the **Google Drive API**.
2. Under **Credentials** $\to$ **Create Credentials** $\to$ **OAuth Client ID**, select **Desktop App**.
3. Download the client secrets JSON, rename it to `credentials.json`, and place it in the same directory as `settings.json`.
4. Run `chzzk-load`. A browser window will open for one-time OAuth2 authorization. Tokens will be automatically saved to `token.json` and refreshed in future runs.

---

## Keyboard Shortcuts

| Key | Action |
| :--- | :--- |
| `q` | **Quit**: Initiates graceful shutdown (stops active recordings and flushes pending uploads). |
| `r` | **Refresh**: Immediately polls monitored channels. |
| `↑` / `k` | **Navigate Up**: Select previous channel in the list. |
| `↓` / `j` | **Navigate Down**: Select next channel in the list. |
| `PageUp` / `PageDown` | **Scroll Logs**: Move activity logs up/down by 5 lines. |
| `Home` / `End` | **Log Navigation**: Jump to top (oldest) or bottom (latest, re-enables auto-tail). |

---

## License

Distributed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
