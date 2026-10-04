# 0007 - Unify Stream Metadata Uploads into Dead-Letter Queue (DLQ) Pipeline

Stream metadata snapshots (`metadata.jsonl`) were previously transferred to remote storage via a dedicated, out-of-band text streaming path: `UploadBackend::upload_text` delegating to `rclone rcat`. This specialized mechanism suffered from several structural defects:

1. **Queue & Concurrency Bypass**: Metadata uploads bypassed the non-blocking primary queue and Dead-Letter Queue (DLQ), ignoring global cross-channel upload concurrency bounds (`upload_concurrency`) and contending uncontrollably with high-priority video segments.
2. **Synchronous Engine Ingress & Child Process Latency**: Metadata changes during active capture ran child processes inline or out-of-band without retry semantics, risking thread pool contention and engine poll loop stalls during network degradation.
3. **Storage Backend Bloat**: The `UploadBackend` trait was burdened with two disparate transport abstractions (`upload_file` and `upload_text`), forcing mock implementations and storage adapters to support stdin pipe streaming.
4. **Heuristic Directory Cleanup**: Because intermediate metadata snapshots had to remain on disk during capture while final snapshots had to be removed, `cleanup_session_dir_if_empty` and `SessionCustodian` relied on file-name heuristics (`has_metadata`) to decide whether a folder could be purged and manually unlinked residual metadata files during directory teardown.
5. **Orphaned Crash Exposure**: Orphaned `metadata.jsonl` files from unexpected application crashes were vulnerable to being deleted during startup cleanup before their final state was synchronized to the cloud.

We unify all remote transfers into a single, cohesive file-upload pipeline governed by explicit task retention policies:

1. **Streamlined Storage Backend Contract (`UploadBackend`)**:
   - Eliminate `upload_text` and `rclone rcat` streaming entirely from `UploadBackend` and `RcloneBackend`.
   - Reduce the trait contract to a single transport method: `upload_file`. All remote transfers uniformly leverage robust `rclone copyto ... --progress` subprocess execution.
   - Remove text collection scaffolding from `MockUploadBackend`, reducing interface surface and mocking complexity.

2. **Explicit Task Retention Policy (`UploadTask`)**:
   - Introduce `pub delete_on_success: bool` to `UploadTask`.
   - Provide typed, self-documenting constructors:
     - `UploadTask::chunk`: Sets `delete_on_success: true` for video segments (`chunk_%04d.ts`) and live chat archives (`chat_%04d.jsonl`).
     - `UploadTask::metadata`: Accepts an explicit retention policy (`false` for intermediate live snapshots, `true` for stream conclusion teardown and startup crash reconciliation).

3. **Worker-Driven File Deletion & Windows Lock Resilience**:
   - Transfer file deletion responsibility exclusively to `UploadWorker` upon successful remote confirmation. Storage backend adapters only transport bytes; they never mutate local filesystem paths.
   - When `task.delete_on_success` is `true`, `UploadWorker` deletes the local file using bounded exponential backoff retries (20ms to 200ms, max 10 attempts), absorbing transient Windows sharing violations (`32`), access denied errors (`5`), and asynchronous unlink latency (`145`), treating `NotFound` as success.
   - When `task.delete_on_success` is `false`, the local file remains untouched on disk.
   - Worker signals `drain_notify` upon completing file unlinking to immediately wake draining coordinators.

4. **Differentiated Telemetry Logging**:
   - Synced snapshots emit: `[{streamer}] Uploaded metadata.jsonl (synced)`.
   - Unlinked files emit: `[{streamer}] Uploaded & deleted {chunk_name} (reclaimed {mb:.1} MB)`.

5. **DLQ Disk-Pressure Eviction Immunity**:
   - In `UploadWorker::evict_oldest_chunk_pair_if_disk_pressure`, tasks representing `metadata.jsonl` or tasks with `!delete_on_success` are strictly shielded from eviction. Under low disk pressure, large video and chat segments are dropped to reclaim gigabytes of disk space, preserving lightweight stream classification and title timelines (a few kilobytes).

6. **Non-Blocking Engine Ingress & Session Lifecycle**:
   - In `EngineOrchestrator` poll loop: state transitions append to local `metadata.jsonl` and non-blockingly enqueue `UploadTask::metadata` with `delete_on_success: false` into `upload_tx`. Zero synchronous child processes are spawned in the poll loop.
   - In `RecordingSession`: initial broadcast registration queues `UploadTask::metadata` with `delete_on_success: false`. Stream conclusion queues final `metadata.jsonl` with `delete_on_success: true`. In local-only mode (`backend: None`), empty zero-chunk sessions unlink local metadata immediately.

7. **Strict Directory Emptiness Invariant**:
   - Eliminate all file-name sniffing heuristics (`has_metadata`) from `cleanup_session_dir_if_empty` and `SessionCustodian`.
   - A session directory is purged if and only if reading its entries yields exactly 0 entries. Because the upload worker unlinks the final `metadata.jsonl`, quiescence and directory emptiness naturally coincide without external intervention.

8. **Startup Crash Reconciliation**:
   - In `ReconciliationCoordinator`, orphaned session directories containing `metadata.jsonl` enqueue it as an `UploadTask::metadata` with `delete_on_success: true` after all orphaned video chunks for that session, guaranteeing cloud synchronization before directory removal.

## Considered Options

- **Retaining `rclone rcat` text streaming**: Rejected because streaming child processes bypassed the upload concurrency queue, lacked DLQ backoff resilience, and created disparate backend interfaces.
- **Relying on `SessionCustodian` to unlink `metadata.jsonl` during folder purge**: Rejected because it violated separation of concerns: the custodian should only purge empty folders, while the uploader should manage unlinking files it successfully uploaded. It also caused phantom directory issues and race conditions.
- **Immediate local deletion of `metadata.jsonl` during live recording**: Rejected because intermediate metadata updates must remain appendable on local disk as subsequent broadcast events (title changes, category changes) occur.

## Consequences

- Universal, non-blocking upload pipeline for video, chat, and metadata under global concurrency limits.
- Eventual consistency and automatic retry for metadata through the DLQ, with strict immunity against disk-pressure eviction.
- Clean separation of concerns: `UploadBackend` only handles remote transport (`upload_file`), `UploadWorker` handles file lifecycle and unlinking, and `SessionCustodian` / `cleanup` enforces strict directory emptiness (0 entries).
- Fast in-memory unit testing for all metadata upload scenarios without external network or subprocess calls.
