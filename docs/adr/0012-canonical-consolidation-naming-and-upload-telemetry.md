# 0012 - Canonical Live Artifact Triad Naming, High-Throughput Transfer, and Upload Telemetry

**Status**: Accepted (supersedes ADR 0011 naming conventions and upload execution; amended for canonical Live Artifact Triad)

Post-recording consolidation previously produced output artifacts named `consolidated.mp4` and `consolidated.jsonl` (or compound `livestream.mp4` and `livechat.jsonl`), alongside live stream metadata recorded as `livemetadata.jsonl`. Meanwhile, the VOD Archival Subsystem (ADR 0013) produces its outputs using standardized snake_case identifiers (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`). This divergence created cross-subsystem naming friction, non-uniform word segmentation, and complex regex matching for downstream indexers and media processors. Additionally, during remote cloud synchronization, uploads executed sequentially via unadorned `rclone copyto` processes without live progress reporting or large-file performance optimizations. For broadcast streams exceeding tens of gigabytes, default rclone configurations (such as Google Drive's 8MB chunk size and single-threaded transfers) led to degraded transfer throughput and an unresponsive terminal experience.

We establish the canonical snake-case **Live Artifact Triad** (`live_stream.mp4`, `live_chat.jsonl`, and `live_metadata.jsonl`), introduce concurrent two-stage cloud uploads with interactive dual-track ANSI progress telemetry in `ConsolidationProgressCoordinator`, and enforce high-throughput transfer optimizations backed by dedicated configuration controls.

## Decision

1. **Canonical Live Artifact Triad Naming**:
   - Standardize post-recording consolidation outputs and live stream recording metadata on the canonical snake-case **Live Artifact Triad**:
     - **Consolidated Video**: `live_stream.mp4` (staged locally and remotely as `live_stream.mp4.part`).
     - **Consolidated Chat**: `live_chat.jsonl` (staged locally and remotely as `live_chat.jsonl.part`).
     - **Stream Metadata**: `live_metadata.jsonl` (produced during capture and consolidated alongside media).
   - Mirror the structural pattern of the VOD Artifact Triad (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`), establishing uniform `<type>_<category>.<ext>` conventions across both real-time capture and VOD archival pipelines.
   - Apply clean-break validation: overwrite checks, manifest conflict detection, and staging invalidation exclusively monitor and cite `live_stream.mp4` and `live_chat.jsonl`.
   - Maintain dual-compatible discovery: pre-flight manifest discovery recognizes both canonical `live_metadata.jsonl` and legacy `livemetadata.jsonl` when determining metadata presence (`has_metadata`), and startup crash reconciliation scans for both files.
   - Align recording engine lifecycle: `RecordingSession` initializes and uploads `live_metadata.jsonl` snapshots, `SessionCustodian` tracks `live_metadata.jsonl` quiescence for strict directory emptiness, and the DLQ preserves `live_metadata.jsonl` tasks immune from deletion under low disk pressure.

2. **Concurrent Dual-Track Cloud Upload**:
   - Replace sequential upload execution with concurrent parallel transfers of `live_stream.mp4` and `live_chat.jsonl`.
   - Implement fail-fast error handling: if any upload fails or encounters cooperative cancellation, immediately abort sibling child processes, remove remote `.part` files (`live_stream.mp4.part`, `live_chat.jsonl.part`) via `rclone deletefile`, and preserve finalized local staging files in `temp_dir` to facilitate instant upload resumption without re-running FFmpeg.

3. **Consolidation Upload Progress Telemetry**:
   - Extend `ConsolidationProgressCoordinator` with an active upload session (`start_upload`).
   - In interactive TUI mode, render responsive TrueColor ANSI progress bars matching existing `VID` and `CHAT` telemetry:
     - ` UP V ` (`theme::GREEN`): Tracks `live_stream.mp4` uploaded/total bytes, transfer percentage, and MB/s speed.
     - ` UP C ` (`theme::BLUE`): Tracks `live_chat.jsonl` uploaded/total bytes, transfer percentage, and MB/s speed.
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
- **Compound or Generic Naming (`livestream.mp4` / `stream.mp4`)**: Using compound words (`livestream.mp4`, `livechat.jsonl`, `livemetadata.jsonl`) breaks stylistic consistency with VOD artifacts (`vod_chat.jsonl`, `vod_metadata.jsonl`), whereas overly generic names (`stream.mp4`, `chat.jsonl`) cause collision ambiguity in shared media buckets. Standardizing on the snake-case Live Artifact Triad (`live_stream.mp4`, `live_chat.jsonl`, `live_metadata.jsonl`) provides uniform tokenization and predictable regex discovery across all archival outputs.
- **Retaining Legacy Staging Aliases**: Detecting legacy `consolidated.*` or compound `.part` names in staging increases pipeline surface area and risks leaving mismatched artifacts across remotes. A clean break enforces uniform directory layouts while preserving read-only backward compatibility for metadata detection during migration.

## Consequences

- Consistent, intuitive snake-case file naming (`live_stream.mp4`, `live_chat.jsonl`, `live_metadata.jsonl`) aligned across local recording, consolidation staging, and remote targets.
- Complete stylistic and architectural symmetry between the Live Artifact Triad and the VOD Artifact Triad (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`).
- Real-time terminal progress visibility for multi-gigabyte cloud transfers with accurate byte counts, percentages, and MB/s transfer speeds.
- Dramatic throughput improvements on large cloud uploads without impacting lightweight live recording chunk uploads.
