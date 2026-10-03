# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
