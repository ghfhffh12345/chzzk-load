# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-rc.1] - 2026-10-02

### Added
- **Global disk-aware eviction strategy in DLQ**: The `min_free_disk_gb` circuit breaker now actively evicts the oldest failed chunks (and their paired chat logs) from the DLQ when disk space falls below safe limits (#18).
- **Startup Crash Reconciliation Re-injection**: Orphaned, contiguous sealed chunks detected on startup are now explicitly re-injected into the Dead-Letter Queue (DLQ) for uploading instead of just being validated (#19).
- **Infinite Retry Loop with Backoff Cap for DLQ**: Upload failures now transfer to a background DLQ with an infinite retry loop (capped at 5m backoff) instead of a 3-retry limit. Tasks are capped at 20 per channel in memory (#17).
- **End-to-End DLQ Integration Tests**: Added rigorous integration tests with a real `rclone` binary (#20).

### Changed
- Updated `README.md` and `README.ko.md` to reflect the new DLQ active eviction and crash reconciliation behaviors.
- Refined DLQ integration tests with paired chat envelopes.
