# Coding Standards & Review Guidelines (`CODING_STANDARDS.md`)

This document is the authoritative standard for code review under the `/code-review` skill. Review subagents consult these rules to distinguish documented repo violations from baseline judgement calls.

---

## 1. Test Architecture & Seam Discipline

- **Sub-Second Unit Test Seams**: Unit suites (`test_channel_lifecycle_registry`, `test_engine_orchestrator_registry`, `test_recorder_watcher`, `test_recorder_ffmpeg`, `test_tui_state`) must complete execution in under 1 second.
  - *Hard Violation*: Adding blocking sleep ticks, network roundtrips, or heavy multi-step loops to unit test files.
  - *Resolution*: Heavy multi-step integration scenarios belong strictly in `tests/test_engine_events.rs` (20–25s budget).
- **Shared Subprocess Mocking**: Never invoke `rustc` or recompile dummy executables inline inside individual test bodies.
  - *Hard Violation*: Inline dummy process compilation or duplicate C-ABI `atexit` temporary directory handlers.
  - *Resolution*: Import and reuse the shared mock fixture via `mod common; use common::mock_ffmpeg::get_mock_ffmpeg_bin;`.
- **Ephemeral Port & Path Isolation**: Tests must never bind hardcoded network ports (`127.0.0.1:0` only) and all filesystem mutations must operate strictly within `std::env::temp_dir()`.

---

## 2. Process & Stream Lifecycle

- **Autonomous Video Segmenter**: FFmpeg child processes, pipe I/O, stderr log parsing, and AES key error detection are encapsulated entirely within `FfmpegSession`.
  - *Hard Violation*: Direct subprocess spawning (`Command::new("ffmpeg")`), manual pipe reading, or polling atomic flags for key errors outside the `recorder` crate.
- **Unified Graceful Teardown**: Post-recording shutdown (natural manifest EOF, session cancellation, or disk space circuit breaker) must funnel through a single call to `session.stop_graceful(timeout)` and final segment sealing outside the main select loop.
  - *Code Smell (Duplicated Code)*: Triplicating `stop_graceful`, log emission, and chunk sealing across individual `tokio::select!` break branches.
- **Reactive Event Consumption**: React to stream events (`FfmpegEvent::KeyForbidden`, `FfmpegEvent::Exited`) directly as they arrive from `recv_event()`.
  - *Code Smell*: Polling `session.is_key_forbidden()` or `child.try_wait()` inside periodic timer loops when event streams already provide notifications.

---

## 3. TUI Decoupling & Terminal Safety

- **Zero Terminal Pollution**: Never use `println!`, `eprintln!`, or unredirected subprocess outputs in the engine, recorder, or uploader layers.
- **Non-Blocking Telemetry**: Telemetry sent to the TUI must use bounded channels with `try_send` or non-blocking forwarders; recording and WebSocket loops must never block on terminal rendering.
- **Typed Semantic Outcomes**: Use typed enums (`FfmpegExit::Clean`, `FfmpegExit::Killed`, `RestrictionReason`) rather than unstructured error strings for state transitions and logging.
