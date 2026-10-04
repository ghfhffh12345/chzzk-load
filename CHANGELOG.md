# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
