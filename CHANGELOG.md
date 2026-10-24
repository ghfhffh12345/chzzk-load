# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-10-24

### Added
- **Post-Recording Consolidation Subcommand (`chzzk-load consolidate`)**: Introduced an offline and cloud-capable consolidation command to losslessly remux fragmented video chunks (`chunk_%04d.ts`) and deduplicate live chat segments (`chat_%04d.jsonl`) into unified artifacts (`stream.mp4` and `chat.jsonl`) (#21, #22, ADR 0010, ADR 0011, ADR 0012).
- **Locally-Staged Seekable Remuxing & Ephemeral HTTP Loopback**: Implemented FFmpeg Concat Demuxer (`-f concat -safe 0`) remuxing with start offset normalization (`-avoid_negative_ts make_zero`) and faststart seek table (`-movflags +faststart`). For cloud storage sessions, chunks stream on demand into memory via an ephemeral read-only HTTP loopback server (`rclone serve http`) while writing seekable output directly to a local staging file (`stream.mp4.part`), bounding peak local disk consumption strictly to 1× the stream size while eliminating playback freezes and desktop scrubbing issues (#58, #59, #70, ADR 0010, ADR 0011).
- **Monotonic DTS Bitstream Filtering & Stream Mapping**: Configured container-aware bitstream filters (`VIDEO_MONOTONIC_DTS_BSF` and `AUDIO_MONOTONIC_DTS_BSF` / `aac_adtstoasc`) to resolve DTS collisions and micro-discontinuities across chunk boundaries without video re-encoding, paired with explicit single-stream mapping (`-map 0:v:0 -map 0:a:0?`) to prevent extraneous stream metadata conflicts (#66).
- **Pre-Flight Capacity Verification & Resilient Upload Resume**: Added staging workspace capacity checks ensuring at least $1.15\times$ total video bytes are available before remuxing begins, with support for custom workspace paths (`--temp-dir <PATH>`). Interrupted cloud uploads inspect existing staged files via `moov` atom container probing and resume transfers immediately without re-remuxing (#69, #72, ADR 0011).
- **Container Structural Integrity Validation**: Added MP4 container structural integrity verification (`validate_mp4_container_integrity`) prior to promoting or resuming staged files: enforces a single `moov` atom, at least one `mdat` atom, complete file coverage without trailing unindexed garbage, and verifies that `stco`/`co64` sample chunk offsets point strictly within the `mdat` media payload (d1b8501).
- **Sliding-Window Chat Deduplication**: Implemented a streaming sliding-window deduplication algorithm that merges chunked chat records into `chat.jsonl.part` in parallel with video remuxing, eliminating duplicate boundary messages while preserving $O(1)$ memory consumption (#23, #71).
- **Concurrent Cloud Upload & Large-File Transfer Optimizations**: Concurrently uploads `stream.mp4` and `chat.jsonl` using two-stage atomic staging (`.part` files renamed via `rclone moveto`) with sibling process abort on failure. Applied large-file transfer optimizations (`--drive-chunk-size 128M`, `--s3-chunk-size 64M`, multi-threading) configurable via `consolidate_extra_args` in `[rclone]` settings (#76, #77, ADR 0012).
- **Dual-Track Consolidation Progress Telemetry**: Responsive ANSI progress tracking with 10 Hz dirty render loop for media remuxing (`VID`, `CHAT`), cloud uploading (`UP V`, `UP C`), and chunk purging (`DEL`), with automatic fallback to 20% milestone boundaries and 30-second heartbeats in non-interactive/headless environments (#55, #62, #63, #78, ADR 0012).
- **Concurrent Chunk Purge**: Added bounded asynchronous purge loop (`--delete-concurrency`, default: 16) for deleting original chunks after verified consolidation, featuring robust error aggregation and silent handling of nonexistent remote targets (#54).
- **Session Custodian Directory Lifecycle Consolidation**: Consolidated all recording session directory tracking, strict 0-entry unlinking verification, and unmanaged empty directory sweeps under `SessionCustodian` (#48, #49, #50, #51).

### Changed
- **Canonical Media Output Naming**: Standardized consolidation output filenames on `stream.mp4` and `chat.jsonl` (staged as `.part`), replacing the previous `consolidated.mp4` and `consolidated.jsonl` convention (#75, ADR 0012).
- **Windows Concat Manifest Path Sanitization**: Canonicalized directory paths and sanitized extended-length verbatim UNC prefixes (`\\?\`) that caused FFmpeg concat demuxer failures on Windows (#65).
- **Shared Sync File Unlink Retries**: Centralized file unlinking retry logic across workers with exponential backoff to absorb transient Windows file sharing locks (#44, #90).

### Removed
- **Legacy Fragmented MP4 Pure-Streaming Pipeline**: Deprecated and completely removed stdout pipe remuxing into `rclone rcat` and fragmented MP4 container configurations (`frag_keyframe+empty_moov`), standardizing seekable MP4 containers across both local and remote operations (#73, ADR 0011).

## [1.0.0] - 2026-10-04

### Added
- **Unified Upload Pipeline & Task Retention Policy (`UploadTask`)**: Introduced explicit `delete_on_success: bool` retention policies to `UploadTask` alongside typed constructors (`UploadTask::chunk`, `UploadTask::metadata`). Video chunks and chat archives unlink locally upon confirmed upload (`delete_on_success: true`), intermediate live metadata snapshots persist locally across updates (`delete_on_success: false`), and concluding stream teardown metadata triggers clean final deletion (`delete_on_success: true`) (#44, ADR 0007).
- **Worker-Driven File Deletion & Windows Lock Resilience**: Centralized all local file unlinking exclusively into `UploadWorker` upon successful remote upload confirmation, decoupling storage backends from local path mutations. Unlinking features bounded exponential backoff retries (20ms to 200ms, max 10 attempts) to absorb transient Windows sharing violations (OS error 32, access denied 5) and asynchronous unlink latency (OS error 145) (#44, ADR 0007).
- **DLQ Disk-Pressure Eviction Immunity for Metadata**: In `UploadWorker::evict_oldest_chunk_pair_if_disk_pressure`, tasks representing `metadata.jsonl` and tasks with `delete_on_success: false` are strictly shielded from eviction. Under low disk pressure, large video and chat chunks are dropped to reclaim gigabytes of disk space while strictly preserving lightweight stream classification and title timelines (#44, ADR 0007).
- **Startup Crash Reconciliation for Metadata**: In `ReconciliationCoordinator`, orphaned recording sessions containing `metadata.jsonl` now enqueue the metadata file as an upload task (`delete_on_success: true`) following any orphaned video segments, ensuring stream state is synchronized to the cloud before session directory removal (#46, ADR 0007).
- **Strict Directory Emptiness Invariant**: Replaced heuristic file-name sniffing (`has_metadata`) in `cleanup_session_dir_if_empty` and `SessionCustodian` with a strict emptiness invariant (`read_dir().count() == 0`). Concluded session folders are purged only when all files have been confirmed uploaded and unlinked by `UploadWorker` (#44, ADR 0007).

### Changed
- **Eliminated `rclone rcat` Text Streaming**: Streamlined `UploadBackend` to a single transport contract (`upload_file`), replacing ad-hoc `upload_text` and `rclone rcat` child processes with standard `rclone copyto ... --progress` executions governed by the primary queue, cross-channel concurrency limits, and DLQ retry semantics (#45, ADR 0007).
- **Non-Blocking Metadata Engine Ingress**: In `EngineOrchestrator` poll loop and `RecordingSession`, metadata state changes append to local disk and non-blockingly enqueue upload tasks to `upload_tx` without spawning child processes or stalling the engine poll loop (#46, ADR 0007).
- **Differentiated Telemetry Logging**: Upload telemetry now distinguishes retained metadata syncs (`[{streamer}] Uploaded metadata.jsonl (synced)`) from unlinked chunk files (`[{streamer}] Uploaded & deleted {chunk_name} (reclaimed {mb:.1} MB)`) (#44, ADR 0007).

### Removed
- **`UploadBackend::upload_text` & Pipe Scaffolding**: Purged `upload_text` from `UploadBackend`, `RcloneBackend`, and `MockUploadBackend`, along with stdin pipe streaming child process handling (#45, ADR 0007).
- **File-Name Sniffing in Directory Cleanup**: Purged `has_metadata` heuristic directory inspection flags from `src/engine/cleanup.rs` and `src/engine/custodian.rs` (#44, ADR 0007).

## [1.0.0-rc.4] - 2026-10-04

### Added
- **Live Stream Intake Seam (`LiveStreamSource`)**: Introduced an object-safe asynchronous stream intake seam (`LiveStreamSource`) decoupling channel polling, stream access tier validation, and chat token retrieval from HTTP networking. Provides direct production implementation on `ChzzkClient` and deterministic in-memory `MockLiveStreamSource` fake adapter with sticky state configurations, scripted transitions, and API error simulation (#30, #31, ADR 0004).
- **Session Custodian Lifecycle Directory Coordinator**: Introduced `SessionCustodian` (`src/engine/custodian.rs`) to track recording session directories across discrete lifecycle states (`Active`, `Draining`). Strictly protects active recording directories against premature deletion while guaranteeing clean, quiescence-based purging when concluded sessions finish in-flight uploads (#39, #41, ADR 0006).
- **Immediate Quiescence Wakeup via Drain Notification**: Introduced a shared `drain_notify` notification hook (`Arc<tokio::sync::Notify>`) signaled whenever `UploadWorker` finishes an upload and unlinks the local chunk file. Wired `drain_notify` into `EngineOrchestrator::run()` to immediately purge drained session directories without waiting for poll intervals (#40, #41).
- **Recording Session Parameter Bundling**: Bundled recording session configuration dependencies into `RecordingSessionParams`, eliminating positional argument bloat across session spawning (#35, ADR 0005).
- **Subprocess Hermeticity & Mock FFmpeg Fallback**: Automatically configured `CHZZK_LOAD_FFMPEG_BIN` environment fallback in the engine orchestrator and test harnesses, preventing real child FFmpeg processes from leaking or racing with directory cleanup during tests (#33).

### Changed
- **Consolidated Chunk Dispatching**: Consolidated sealed chunk handling, telemetry logging, and upload task creation into an internal `SessionChunkDispatcher` helper within `src/engine/recording.rs`, capturing ambient session state and streamlining chunk ingress across segment watcher loops, exit drains, and live chat archiving (#35, #36, ADR 0005).
- **Decoupled Upload Worker**: Stripped all parent directory inspection and session folder naming heuristics from `UploadWorker`, restricting it strictly to chunk uploading, post-upload local unlinking, and telemetry (#40, ADR 0006).
- **In-Memory Integration Test Migration**: Migrated engine events, chat, and registry integration test suites (`test_engine_events.rs`, `test_engine_chat.rs`, `test_engine_orchestrator_registry.rs`) from ephemeral HTTP loopback servers to in-memory `MockLiveStreamSource`, eliminating network socket flakiness and significantly accelerating test execution (#32, #33).
- **Engine Orchestrator Builder Dependency Injection**: Replaced ad-hoc test shims on `EngineOrchestrator` with builder-based dependency injection (`with_chzzk`, `with_backend`, `with_custodian`, `with_ffmpeg_bin`, `with_poll_interval`, `with_drain_notify`, `with_cancel_token`), standardizing testing seams.
- **Immediate Zero-Chunk Session Purge**: Concluded sessions that produce no video segments (such as streams ending before the first segment boundary) are now purged immediately upon stream exit rather than lingering on disk (#41).

### Removed
- **Shallow Dispatcher Module & Pass-Through Orchestrator Shims**: Removed `src/engine/dispatcher.rs` and deprecated static pass-through shims on `EngineOrchestrator` (`process_sealed_chunk`, `seal_and_enqueue_chunks`), co-locating chunk dispatch unit tests directly in `recording.rs` (#37, ADR 0005).

## [1.0.0-rc.3] - 2026-10-03

### Fixed
- **Windows File Lock & Asynchronous Unlink Resilience**: Enhanced `RcloneBackend` and session directory cleanup with bounded exponential backoff retries against transient Windows sharing violations (OS error 32, access denied 5) and asynchronous directory unlinking latency (OS error 145), treating missing files as successful unlinks.
- **Graceful Shutdown Parallelization**: Parallelized metadata upload and live chat teardown via `tokio::join!` during session finalization, eliminating shutdown stalls and preventing residual files on disk.

### Added
- **Shutdown Cleanup Integration Tests**: Added integration tests (`tests/test_shutdown_cleanup.rs`) verifying graceful exit directory cleanup and transient file lock recovery.

## [1.0.0-rc.2] - 2026-10-03

### Added
- **Lean Metadata Snapshot Schema (v2)**: Implemented flat, lightweight stream metadata state snapshots in `metadata.jsonl` (schema version 2), capturing essential broadcast lifecycles, classifications, and flattened policy flags (`is_kr_only`, `is_chat_active`, `is_watch_party`, `paid_promotion`, `drops_campaign_no`) (#26).
- **Targeted Unit & Seam Tests**: Added comprehensive test coverage for lean metadata wire serialization/deserialization, API client parsing, channel lifecycle registry title transitions, and typed engine orchestrator seams (#26, #27).

### Changed
- **Direct Session State Equality Tracking**: Stream metadata change detection in active recording sessions now evaluates direct struct equality (`state == self.last_state`) on lean snapshots rather than computing field-by-field delta diffs, minimizing memory allocations and preventing write churn (#27).
- **Dedicated Title Change Dispatching**: Separated broadcast title change notifications from metadata persistence in `PollAction::RecordingMetadataChanged`, ensuring channel title updates are dispatched to the TUI without redundant event broadcasts (#27).
- **Optimized JSON Lines Serialization**: Utilized `MetadataEvent::to_json_line()` for direct serialization and newline appending to session metadata files (#27).
- Updated `README.md`, `README.ko.md`, and `GLOSSARY.md` to document the v2 lean metadata snapshot specification (#28).

### Removed
- **Legacy v1 Delta Models & Volatile Fields**: Purged legacy delta calculation models (`MetadataDelta`, `ValueDiff`), nested structures (`policies`, `watch_party`, `chat_rules`), redundant local timestamps (`time_local`), and volatile viewer telemetry (`concurrent_user_count`, `accumulate_count`, thumbnail URLs) from `metadata.jsonl` (#28).

## [1.0.0-rc.1] - 2026-10-02

### Added
- **Global disk-aware eviction strategy in DLQ**: The `min_free_disk_gb` circuit breaker now actively evicts the oldest failed chunks (and their paired chat logs) from the DLQ when disk space falls below safe limits (#18).
- **Startup Crash Reconciliation Re-injection**: Orphaned, contiguous sealed chunks detected on startup are now explicitly re-injected into the Dead-Letter Queue (DLQ) for uploading instead of just being validated (#19).
- **Infinite Retry Loop with Backoff Cap for DLQ**: Upload failures now transfer to a background DLQ with an infinite retry loop (capped at 5m backoff) instead of a 3-retry limit. Tasks are capped at 20 per channel in memory (#17).
- **End-to-End DLQ Integration Tests**: Added rigorous integration tests with a real `rclone` binary (#20).

### Changed
- Updated `README.md` and `README.ko.md` to reflect the new DLQ active eviction and crash reconciliation behaviors.
- Refined DLQ integration tests with paired chat envelopes.
