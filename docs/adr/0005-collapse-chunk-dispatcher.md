# 0005 - Collapse Shallow Chunk Dispatcher into Recording Session

Sealed chunk dispatching (video segments and chat chunks) and telemetry event logging were previously split across a shallow external module (`src/engine/dispatcher.rs`) and pass-through static shims on `EngineOrchestrator` (`process_sealed_chunk` and `seal_and_enqueue_chunks`). Because the dispatcher module possessed no state of its own, every call required passing 7 to 8 positional parameters across an artificial module boundary. Furthermore, `RecordingSession::spawn` suffered from parameter bloat with 10 positional arguments.

We consolidate chunk dispatching directly into the recording session module:

1. **Parameter Bundling (`RecordingSessionParams`)**:
   Bundle the 10 dependencies required by recording sessions into a typed `RecordingSessionParams` struct (`channel_id`, `info`, `upload_tx`, `settings`, `backend`, `chzzk`, `event_tx`, `registry`, `cancel_token`, `ffmpeg_bin`).

2. **Internal Ambient Context Helper (`SessionChunkDispatcher`)**:
   Encapsulate chunk sealing, telemetry logging, and upload task creation in an internal `SessionChunkDispatcher` helper within the recording session module, capturing ambient session state (`session_folder`, `channel_id`, `streamer_name`, `upload_tx`, `event_tx`, `backend_active`).

3. **Signature Reduction**:
   - `process_sealed_chunk(&self, chunk_path: &Path)` (reduced from 7 arguments to 1).
   - `seal_and_enqueue(&self, watcher: &mut SegmentWatcher, is_finished: bool)` (reduced from 8 arguments to 2).

4. **Unified Ingress Dispatch**:
   Reuse this internal helper uniformly across the periodic 1-second segment watcher loop, post-recording exit chunk drain, and live chat archiving chunk forwarder.

5. **Shallow Module Contraction & Interface Minimization**:
   Eliminate public static shims on `EngineOrchestrator` and contract the shallow `dispatcher.rs` module. The public interface is the test surface; internal session details are not exposed as public orchestrator helpers.

6. **Co-Located Unit Testing**:
   Co-locate chunk dispatch unit tests in `src/engine/recording.rs` under `#[cfg(test)] mod tests`, running in sub-milliseconds (<10ms) against in-memory channels and isolated temporary directories.

## Considered Options

- **Retaining standalone `src/engine/dispatcher.rs` as a stateless utility module**: Rejected because it failed the deletion test. It did not hide complexity, maintain independent state, or offer reuse outside of recording sessions. It merely created an artificial boundary that inflated method signatures.
- **Retaining pass-through static shims on `EngineOrchestrator`**: Rejected because orchestrator shims existed purely for test convenience, violating seam discipline ("the interface is the test surface"). Exposing internal chunk handling on the orchestrator polluted its public API.
- **Passing individual parameters into private helper functions**: Rejected because it perpetuated data clumping across the three chunk ingress points (watcher loop, stream drain, chat chunk forwarder). Capturing ambient session state in `SessionChunkDispatcher` simplifies all call sites.

## Consequences

- `RecordingSession` configuration and chunk dispatch operate with clean, self-documenting data structures and drastically reduced argument counts.
- Chunk sealing and dispatch unit tests run co-located in sub-milliseconds without requiring orchestrator instantiation or heavy integration scaffolding.
- Eliminates artificial module sprawl in `src/engine/`, keeping the architecture deep, cohesive, and easy to navigate.
