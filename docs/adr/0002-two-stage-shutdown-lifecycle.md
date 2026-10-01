# Two-Stage Shutdown Lifecycle and Grace Period Escalation

When users initiated application shutdown via TUI keypress ('q') or OS signal (SIGINT/SIGTERM/SIGHUP), active video recording streams or rclone cloud uploads could stall or hang indefinitely, trapping the process in an infinite wait state without returning to the shell prompt. Furthermore, manual force exit risked orphaned child FFmpeg processes and uncleaned empty session directories.

We formalize a two-stage shutdown lifecycle across the application:

1. **Stage 1 (Cooperative Graceful Teardown)**:
   - Triggered by a single 'q' press in TUI or first OS signal.
   - Cancellation token is broadcast to all active recording sessions and the orchestrator.
   - FFmpeg sessions execute graceful stop (`"q\n"`), final video/chat segments seal and queue, and active cloud uploads are given a bounded 15-second grace window to finish.
   - In TUI mode, the dashboard displays shutdown progress and instructions for immediate override.
   - If all recording tasks and uploads complete within the 15-second window, session directory cleanup runs, terminal state is cleanly restored, and the process exits with status code 0.

2. **Stage 2 (Escalation to Immediate Force Exit)**:
   - Triggered either manually (second 'q' keypress / second OS signal) or automatically (expiration of the bounded 15-second grace period while tasks remain active).
   - The application immediately aborts all active orchestrator tasks and spawned recording session join handles (`orchestrator.abort_all()`, `orch_handle.abort()`). Subprocess kill-on-drop ensures active FFmpeg child processes are killed instantly.
   - Rapid bounded directory cleanup (<500ms via `EngineOrchestrator::cleanup_empty_session_dirs_bounded`) purges empty or metadata-only folders without blocking on disk stalls.
   - Terminal raw mode, alternate screen, and original console code page are restored safely.
   - The process terminates with status code 1.

## Considered Options

- **Unbounded Wait for Cloud Uploads**: Rejected because network drops or hung rclone transfers trap the user indefinitely in the terminal.
- **Immediate Process Kill Without Abort or Cleanup**: Rejected because background child tasks can leave orphaned recording sessions, locked files, and empty folder pollution across the filesystem.
- **Detached Background Sleep Timer in Signal Handler**: Rejected because uncoordinated sleep tasks race with the main TUI render loop, skip directory cleanup, and fail to handle in-TUI 'q' keypresses.

## Consequences

- Bounded shutdown guarantee: The application will never block for more than 15.5 seconds under any failure mode.
- Clean vs Escalated exit codes: Clean completions exit with code 0; timeouts and force exits exit with code 1.
- Zero terminal pollution or hang: Both TUI and headless daemon modes reliably restore the terminal and code page before exit.
