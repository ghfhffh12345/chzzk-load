# 0012 - Canonical Consolidation Naming, High-Throughput Transfer, and Upload Telemetry

**Status**: Accepted (supersedes ADR 0011 naming conventions and upload execution)

Post-recording consolidation previously produced output artifacts named `consolidated.mp4` and `consolidated.jsonl` (staged as `consolidated.mp4.part` and `consolidated.jsonl.part`). During remote cloud synchronization, uploads executed sequentially via unadorned `rclone copyto` processes without live progress reporting or large-file performance optimizations. For broadcast streams exceeding tens of gigabytes, default rclone configurations (such as Google Drive's 8MB chunk size and single-threaded transfers) led to degraded transfer throughput and an unresponsive terminal experience.

We establish canonical media naming conventions (`stream.mp4` and `chat.jsonl`), introduce concurrent two-stage cloud uploads with interactive dual-track ANSI progress telemetry in `ConsolidationProgressCoordinator`, and enforce high-throughput transfer optimizations backed by dedicated configuration controls.

## Decision

1. **Canonical Output Naming**:
   - Rename consolidated media outputs from `consolidated.mp4` to `stream.mp4` and `consolidated.jsonl` to `chat.jsonl`.
   - Update temporary staging and remote in-flight targets to `stream.mp4.part` and `chat.jsonl.part`.
   - Apply clean-break validation: overwrite checks, manifest conflict detection, and staging invalidation exclusively monitor `stream.mp4` and `chat.jsonl`.

2. **Concurrent Dual-Track Cloud Upload**:
   - Replace sequential upload execution with concurrent parallel transfers of `stream.mp4` and `chat.jsonl`.
   - Implement fail-fast error handling: if any upload fails or encounters cooperative cancellation, immediately abort sibling child processes, remove remote `.part` files via `rclone deletefile`, and preserve finalized local staging files in `temp_dir` to facilitate instant upload resumption without re-running FFmpeg.

3. **Consolidation Upload Progress Telemetry**:
   - Extend `ConsolidationProgressCoordinator` with an active upload session (`start_upload`).
   - In interactive TUI mode, render responsive TrueColor ANSI progress bars matching existing `VID` and `CHAT` telemetry:
     - ` UP V ` (`theme::GREEN`): Tracks `stream.mp4` uploaded/total bytes, transfer percentage, and MB/s speed.
     - ` UP C ` (`theme::BLUE`): Tracks `chat.jsonl` uploaded/total bytes, transfer percentage, and MB/s speed.
   - In non-interactive mode, log 20% milestone boundaries and 30-second heartbeat proof-of-liveness entries.
   - Stream rclone stderr line-by-line using `--use-json-log --stats 250ms --stats-log-level NOTICE`, parsing metrics via `parse_rclone_log_line`.

4. **High-Throughput Large-File Transfer Optimizations**:
   - Apply tuned rclone defaults optimized for tens-of-gigabytes payloads to consolidation uploads:
     - `--drive-chunk-size 128M` (reduces Google Drive API request overhead)
     - `--s3-chunk-size 64M` and `--s3-upload-concurrency 4`
     - `--buffer-size 64M`, `--multi-thread-streams 4`, and `--multi-thread-cutoff 250M`
   - Introduce `consolidate_extra_args` in `[rclone]` of `settings.toml` to separate batch consolidation flags from live chunk upload flags (`extra_args`).
   - Automatically resolve `settings.toml` (or `--config <path>`) in `chzzk-load consolidate`, falling back cleanly to built-in high-performance defaults when executed in unconfigured environments.

## Considered Options

- **Sequential Upload with a Single Transitioning Bar**: Uploading video first and chat second reduces peak upload bandwidth competition but extends total run time and leaves chat progress unmonitored until multi-gigabyte video finishes. Rejected in favor of concurrent parallel transfers with dual progress bars.
- **Unified `extra_args` for Both Live and Consolidation Uploads**: Reusing `extra_args` risks applying large 128MB chunk buffers and multi-threading parameters to lightweight live segment uploads (60s `.ts` chunks), potentially inflating memory usage across numerous concurrent live streams. Rejected in favor of dedicated `consolidate_extra_args`.
- **Retaining `consolidated.*` Legacy Resume Aliases**: Detecting both `consolidated.*` and `stream.*` in staging increases pipeline surface area and risks leaving mismatched artifacts across remotes. A clean break enforces a uniform directory layout.

## Consequences

- Consistent, intuitive file naming (`stream.mp4` and `chat.jsonl`) aligned across local and remote targets.
- Real-time terminal progress visibility for multi-gigabyte cloud transfers with accurate byte counts, percentages, and MB/s transfer speeds.
- Dramatic throughput improvements on large cloud uploads without impacting lightweight live recording chunk uploads.
