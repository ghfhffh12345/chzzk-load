# 0006 - Session Custodian Directory Purge

Session directories were previously cleaned up via ad-hoc parent directory inspections in `UploadWorker` (`src/uploader/worker.rs`) or coarse directory scans in `EngineOrchestrator`. This created significant architectural coupling: the upload worker was burdened with session folder naming heuristics and directory cleanup side effects, upload worker unit tests were forced to manage filesystem folder deletion scaffolding, concluded sessions with in-flight chunk uploads risked leaving phantom empty directories if uploads completed asynchronously, and zero-chunk sessions (such as streams ending before the first segment boundary) were not cleanly purged immediately.

We introduce a dedicated, quiescence-driven `SessionCustodian` coordinator and decouple the upload worker:

1. **Decouple the Upload Worker**:
   - Strip all parent directory inspection, folder naming heuristics, and directory cleanup routines from `UploadWorker`.
   - Restrict `UploadWorker` strictly to uploading individual chunk files, deleting uploaded chunk files from local disk upon confirmation, and emitting telemetry events.
   - Introduce an optional shared `drain_notify: Option<Arc<tokio::sync::Notify>>` hook. Whenever an upload finishes and its chunk file is deleted, `UploadWorker` signals this hook to wake up draining coordinators.

2. **Dedicated `SessionCustodian` Coordinator**:
   - Establish `SessionCustodian` (`src/engine/custodian.rs`) as a thread-safe lifecycle coordinator managing recording session directory states.
   - Monitored session directories transition across discrete lifecycle states:
     - `SessionTrackState::Active`: Actively recording; strictly protected against directory cleanup even if temporarily empty.
     - `SessionTrackState::Draining`: Broadcast has concluded; awaiting quiescence of in-flight chunk uploads, Dead-Letter Queue (DLQ) retries, or retention checks.
   - Expose a focused typed interface: `register_active`, `register_draining`, `mark_concluded`, `try_purge`, `try_purge_drained`, and `active_paths`.

3. **Quiescence-Based Directory Purge**:
   - Concluded directories are inspected for quiescence before removal: if video chunks (`.ts`) or chat chunks (`.jsonl`) remain (due to active uploads, DLQ backoff retries, or local-only retention mode), the directory is preserved.
   - When only `metadata.jsonl` remains (or if the folder is completely empty), `metadata.jsonl` is safely unlinked and the folder is removed using bounded exponential backoff retries for Windows file lock tolerance (sharing violation 32, access denied 5, async unlink latency 145).
   - Upon successful purge, the custodian emits a streamer-attributed clean log (`[{streamer} ({channel_id})] Cleaned up empty session folder '{path}'`) and removes the directory from tracking.

4. **Lifecycle Sequencing & Immediate Quiescence Wakeup**:
   - On broadcast start, `RecordingSession` registers its folder as active with the custodian.
   - On broadcast exit, `RecordingSession` transitions the folder to draining and invokes `try_purge`, purging zero-chunk sessions immediately.
   - In `EngineOrchestrator::run()`, the event loop awaits `drain_notify.notified()`. When `UploadWorker` finishes an in-flight chunk upload and unlinks the local file, the orchestrator immediately triggers `custodian.try_purge_drained()`, deleting the session directory the moment the last chunk clears without waiting for polling intervals.
   - Periodic engine poll loops and the graceful shutdown barrier invoke `try_purge_drained()`, guaranteeing clean directory teardown prior to process termination.

## Considered Options

- **Retaining directory cleanup within `UploadWorker`**: Rejected because inspecting parent directories leaked session folder naming heuristics into the upload pipeline, complicated worker unit tests, and failed to handle zero-chunk sessions where no upload tasks were ever queued.
- **Relying solely on periodic poller scans without drain notification**: Rejected because polling intervals (10–60s) left phantom empty session directories on disk long after chunk uploads finished, degrading disk cleanliness and user feedback.
- **Immediate unconditional deletion on broadcast end**: Rejected because pending in-flight chunk uploads, DLQ backoff retries during network outages, and local-only recording mode require session directories and their media files to remain intact.

## Consequences

- `UploadWorker` presents a cohesive, focused interface with zero knowledge of session folder naming conventions.
- Empty session directories are purged immediately upon reaching quiescence without residual phantom folders across normal broadcast conclusions, zero-chunk streams, and graceful shutdowns.
- Active recording sessions remain strictly protected from premature deletion.
- Fast sub-millisecond in-memory unit tests in `src/engine/custodian.rs` (<10ms) comprehensively verify directory lifecycle state transitions.
