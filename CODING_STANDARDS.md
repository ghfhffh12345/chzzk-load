# Coding Standards & Review Guidelines (`CODING_STANDARDS.md`)

This document is the authoritative standard for code review under the `/code-review` skill. Review subagents consult these rules to distinguish documented repo violations from baseline judgement calls.

---

## 1. Test Architecture & Seam Discipline

- **Sub-Second Unit Test Seams**: Unit suites (`test_channel_lifecycle_registry`, `test_engine_orchestrator_registry`, `test_recorder_watcher`, `test_recorder_ffmpeg`, `test_tui_state`, `test_cli_smoke`) must complete execution in under 1 second.
  - *Hard Violation*: Adding blocking sleep ticks, network roundtrips, or heavy multi-step loops to unit test files.
  - *Resolution*: Heavy multi-step integration scenarios belong strictly in `tests/test_engine_events.rs` (20–25s budget).
- **Virtual Time Testing**: Test async timeouts, grace periods, and debounces using Tokio's virtual time (`tokio::time::pause()`) rather than wall-clock thread sleeps (`std::thread::sleep` or unpaused `tokio::time::sleep`).
  - *Hard Violation*: Adding wall-clock sleeps to simulate timeout expiration or debounce delays in unit tests.
  - *Resolution*: Initialize unit tests with `#[tokio::test(start_paused = true)]` or invoke `tokio::time::pause()` to advance time deterministically via `tokio::time::advance()`.
- **Shared Subprocess Mocking**: Never invoke `rustc` or recompile dummy executables inline inside individual test bodies.
  - *Hard Violation*: Inline dummy process compilation or duplicate C-ABI `atexit` temporary directory handlers.
  - *Resolution*: Import and reuse the shared mock fixture via `mod common; use common::mock_ffmpeg::get_mock_ffmpeg_bin;`.
- **Ephemeral Port & Path Isolation**: Tests must never bind hardcoded network ports (`127.0.0.1:0` only) and all filesystem mutations must operate strictly within `std::env::temp_dir()`.
- **No Test-Convenience Forwarding Shims**: Top-level orchestrators (`EngineOrchestrator`) must expose only real public caller interfaces. Never add public static or instance forwarding shims (*Middle Man* smell) solely to facilitate tests of internal subsystems from external integration tests.
  - *Hard Violation*: Adding public static or instance pass-through shims on `EngineOrchestrator` (e.g. `process_sealed_chunk`, `seal_and_enqueue_chunks`) to test internal subsystem logic from outer integration tests.
  - *Resolution*: The interface is the test surface. Internal subsystem behavior (segment dispatching, WebSocket frames, upload queue tasks) must be tested at its own co-located module seam (e.g., `RecordingSession`, `ChatWriter`, `RcloneBackend`), using unit tests co-located under `#[cfg(test)] mod tests` or dedicated module unit test suites.

---

## 2. Process & Stream Lifecycle

- **Autonomous Video Segmenter**: FFmpeg child processes, pipe I/O, stderr log parsing, and AES key error detection are encapsulated entirely within `FfmpegSession`.
  - *Hard Violation*: Direct subprocess spawning (`Command::new("ffmpeg")`), manual pipe reading, or polling atomic flags for key errors outside the `recorder` crate.
- **Unified Graceful Teardown**: Post-recording shutdown (natural manifest EOF, session cancellation, or disk space circuit breaker) must funnel through a single call to `session.stop_graceful(timeout)` and final segment sealing outside the main select loop.
  - *Code Smell (Duplicated Code)*: Triplicating `stop_graceful`, log emission, and chunk sealing across individual `tokio::select!` break branches.
- **Reactive Event Consumption**: React to stream events (`FfmpegEvent::KeyForbidden`, `FfmpegEvent::Exited`) directly as they arrive from `recv_event()`.
  - *Code Smell*: Polling `session.is_key_forbidden()` or `child.try_wait()` inside periodic timer loops when event streams already provide notifications.
- **Safe Multiplexing in `tokio::select!`**: Branch future expressions evaluate eagerly on every loop tick before guards (`if <cond> =>`) are evaluated.
  - *Hard Violation*: Calling `.unwrap()` or executing fallible logic in a `tokio::select!` branch future expression (e.g. `_ = sleep_until(deadline.unwrap()), if deadline.is_some() =>`).
  - *Resolution*: Defer evaluation inside an `async` block that yields `std::future::pending().await` when inactive (e.g., `async { match deadline { Some(d) => sleep_until(d).await, None => std::future::pending().await } }`).

---

## 3. TUI Decoupling & Terminal Safety

- **Zero Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs in the engine, recorder, or uploader layers.
- **Non-Blocking Telemetry**: Telemetry sent to the TUI must use bounded channels with `try_send` or non-blocking forwarders; recording and WebSocket loops must never block on terminal rendering.
- **Typed Semantic Outcomes**: Use typed enums (`FfmpegExit::Clean`, `FfmpegExit::Killed`, `RestrictionReason`) rather than unstructured error strings for state transitions and logging.

---

## 4. Domain Model Hygiene & Schema Transitions

- **Contract Phase Canonicalization**: When completing the contract or purge phase of a schema refactoring, internal engine, recorder, and client modules must directly consume canonical type names (`StreamMetadataState`, `MetadataEvent`).
  - *Code Smell (Middle Man / Speculative Generality)*: Retaining transitional version aliases (`*V2`, `*Old`) across internal callers after legacy models have been purged.
  - *Resolution*: Migrate internal imports and usages directly to canonical types; preserve aliases only when required for external library consumers.

---

## 5. Filesystem Invariants & Windows OS Safety

- **Windows Transient Lock & Unlink Resilience**: All filesystem deletion routines operating on child process outputs (`upload_file_and_delete`) or directory unlinks (`cleanup_session_dir_if_empty`) must implement bounded exponential backoff retries handling `ERROR_SHARING_VIOLATION` (32), `ERROR_ACCESS_DENIED` (5), and `ERROR_DIR_NOT_EMPTY` (145), and treat `ErrorKind::NotFound` as success.
  - *Hard Violation*: Single-attempt `tokio::fs::remove_file` or `remove_dir` immediately following child process termination or file deletion.
  - *Resolution*: Retry up to 5–10 attempts with exponential backoff (20ms–200ms) before returning an I/O error, allowing background Windows antivirus and filesystem filter drivers to release locks.
- **External Subprocess Hermeticity**: Integration tests invoking real external binaries (`rclone`, `ffmpeg`) must guard execution behind an availability check (e.g. `ensure_rclone_available()`, `ensure_ffmpeg_available()`) to prevent environment-dependent test failures in lightweight runner environments. External live network streams or credentials must be marked `#[ignore]`.

