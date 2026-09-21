# chzzk-load

High-performance standalone TUI application for monitoring, recording, and uploading Naver Chzzk livestreams directly to Google Drive in real-time.

Written in Rust for near-zero CPU and memory overhead, using FFmpeg stream-copy (`-c copy`) and chunked resumable Google Drive uploads.

---

## Quick Start

### Run immediately with `npx`
```bash
npx chzzk-load
```

### Install globally
```bash
npm install -g chzzk-load
chzzk-load
```

---

## Prerequisites

- **FFmpeg**: Must be installed and available on your system `PATH`.
  - **Windows**: `winget install Gyan.FFmpeg` or `choco install ffmpeg`
  - **macOS**: `brew install ffmpeg`
  - **Ubuntu / Debian**: `sudo apt install ffmpeg`
  - Verify with: `ffmpeg -version`
- **Node.js**: Version 16.0.0 or later (for this npm runner).

---

## Supported Platforms

`chzzk-load` automatically detects your operating system and CPU architecture, downloading the corresponding native pre-compiled binary via optional dependencies:

| Platform | Architecture | Package |
| :--- | :--- | :--- |
| **Windows** | x86_64 | `chzzk-load-windows-x64` |
| **Linux** | x86_64 | `chzzk-load-linux-x64` |
| **Linux** | ARM64 | `chzzk-load-linux-arm64` |
| **macOS** | x86_64 (Intel) | `chzzk-load-darwin-x64` |
| **macOS** | ARM64 (Apple Silicon) | `chzzk-load-darwin-arm64` |

---

## Environment Variables

- `CHZZK_LOAD_BIN`: Path to a custom `chzzk-load` executable. If set, the runner executes this binary directly instead of looking for platform-specific packages.
  ```bash
  # Linux / macOS
  export CHZZK_LOAD_BIN="/usr/local/bin/chzzk-load"

  # Windows (PowerShell)
  $env:CHZZK_LOAD_BIN = "C:\Tools\chzzk-load.exe"
  ```

---

## Manual Binary Download

If optional dependencies cannot be installed in your environment (e.g., `--no-optional` was passed), standalone executables are available on the [GitHub Releases](https://github.com/ghfhffh12345/chzzk-load/releases) page.

---

## Documentation & Repository

For configuration details, TUI keyboard shortcuts, and architecture documentation, visit the [GitHub Repository](https://github.com/ghfhffh12345/chzzk-load).

## License

[Apache-2.0](https://github.com/ghfhffh12345/chzzk-load/blob/main/LICENSE)
