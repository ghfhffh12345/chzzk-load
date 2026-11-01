# 0014 - Deepen Remote Storage Backend Seam (UploadBackend)

**Status**: Accepted (Amends and fulfills ADR 0007 and ADR 0012)

Cloud storage interactions in `chzzk-load` were previously fragmented across three separate subsystems:

1. **Live Broadcast Recording** (`src/engine/recording.rs`, `src/uploader/worker.rs`): Utilized the `UploadBackend` trait seam and an asynchronous upload queue with a Dead-Letter Queue (DLQ) to transfer video segments (`chunk_%04d.ts`), chat archives (`chat_%04d.jsonl`), and stream metadata (`live_metadata.jsonl`) via `RcloneBackend` or `MockUploadBackend`.
2. **On-Demand VOD Archival** (`src/vod/upload.rs`, `src/vod.rs`): Coupled directly to concrete `RcloneBackend` instances, relying on concrete struct fields (`backend.config.remote_path`) and inherent methods rather than a polymorphic storage interface.
3. **Post-Recording Consolidation** (`src/consolidation.rs`, `src/consolidation/chat.rs`, `src/consolidation/video.rs`): Bypassed the storage abstraction completely by spawning ad-hoc child CLI processes via raw `tokio::process::Command` for `rclone copyto`, `moveto`, and `deletefile`, duplicating JSON log parsing, stderr collection, and cancellation logic.

This fragmentation created substantial technical debt and operational risk:
- **Test Overhead & Brittle Fixtures**: Integration tests for VOD archival and post-recording consolidation were forced to compile and spawn external mock subprocess binaries (`tests/common/mock_rclone.rs`), making test execution slow and vulnerable to transient Windows process and file sharing locks.
- **Duplicated Subprocess & Error Logic**: Child process management, stderr buffering, JSON progress parsing (`parse_rclone_log_line`), and process lifecycle guarantees (`kill_on_drop(true)`) were duplicated across `uploader`, `vod`, and `consolidation`.
- **Inconsistent Cancellation & Cleanup**: When multi-gigabyte consolidation transfers or VOD uploads were cancelled or failed, partial staging files (`.part`) on cloud remotes risked being leaked if cleanup routines diverged.

We deepen the `UploadBackend` trait seam into a unified, polymorphic Remote Storage Backend, centralize all rclone subprocess operations within `RcloneBackend`, provide an in-memory virtual remote store in `MockUploadBackend`, and eliminate raw subprocess spawning from domain logic.

---

## Decision

1. **Deepen the `UploadBackend` Trait Contract**:
   Expand `UploadBackend` (`src/uploader/backend.rs`) from a single-method interface into a comprehensive remote storage abstraction while preserving 100% object safety:
   - `upload_file_to_dest`: Transfers a local file directly to an explicit remote destination path with optional progress tracking (`Option<ProgressCallback>`).
   - `move_file`: Atomically moves or renames a remote file from `remote_src` to `remote_dst`.
   - `delete_file`: Idempotently deletes a remote file at `remote_path`.
   - `delete_files`: Concurrently deletes multiple remote files under a bounded concurrency limit (defaulting to 16 concurrent operations with cooperative error aggregation).
   - `check_connection`: Validates remote storage connectivity and configuration before initiating workflows.
   - `upload_file`: Retained as a default convenience method routing to `upload_file_to_dest`, preserving complete backwards compatibility with `UploadWorker` and existing caller sites.

2. **Centralize Subprocess Execution in `RcloneBackend`**:
   - Promote inherent `RcloneBackend` operations onto the `UploadBackend` trait.
   - Standardize progress tracking across all upload operations using `--use-json-log --stats 250ms --stats-log-level NOTICE`, parsing metrics line-by-line via `parse_rclone_log_line` and buffering trailing non-JSON stderr lines for descriptive diagnostic errors.
   - Enforce `kill_on_drop(true)` on every spawned child process (`copyto`, `moveto`, `deletefile`, connection checks). Dropping an upload future immediately terminates the underlying rclone subprocess, ensuring instantaneous sibling abort under `tokio::try_join!`.
   - Guarantee strict idempotency in `delete_file` by treating rclone exit code 4 ("directory or object not found") as success (`Ok(())`).
   - Support high-throughput transfers for multi-gigabyte consolidation payloads via `RcloneBackend::for_consolidation`, applying `HIGH_THROUGHPUT_UPLOAD_ARGS` and `consolidate_extra_args`.
   - Integrate `check_connection` with `skip_connection_check` and empty `remote_path` handling.

3. **In-Memory Virtual Remote Store in `MockUploadBackend`**:
   - Equip `MockUploadBackend` with an in-memory virtual remote store (`files: Arc<Mutex<HashMap<String, u64>>>`).
   - Simulate atomic promotions (`move_file`), idempotent deletions (`delete_file`, `delete_files`), file transfers (`upload_file_to_dest`), and failure injection (`should_fail: Arc<AtomicBool>`).
   - Maintain `uploads: Arc<Mutex<Vec<(PathBuf, String)>>>` to ensure seamless backwards compatibility with legacy assertions across engine and DLQ test suites.
   - Enable sub-millisecond (<1ms), deterministic unit and integration tests for VOD archival and consolidation workflows without disk I/O, network sockets, or external subprocess overhead.

4. **Retire Raw Subprocess Spawning from VOD Archival and Consolidation**:
   - **VOD Archival** (`src/vod/upload.rs`, `src/vod.rs`):
     - Refactor `validate_remote_connection` and `upload_vod_artifacts` to accept `&dyn UploadBackend`.
     - Introduce `backend: Option<Arc<dyn UploadBackend>>` to `VodOptions`, allowing caller injection of production or mock backends.
   - **Post-Recording Consolidation** (`src/consolidation.rs`, `src/consolidation/chat.rs`, `src/consolidation/video.rs`):
     - Remove raw `Command` helper functions (`build_remote_staged_upload_command`, `upload_remote_staged_file_with_progress`, `upload_remote_staged_file_with_args`, `upload_remote_staged_file`).
     - Refactor `upload_remote_staged_media_with_progress`, `finalize_staged_video`, `finalize_staged_chat`, and `delete_original_chunks_with_progress` to operate against `&dyn UploadBackend`.
     - Forward ANSI upload telemetry channels (`video_tx`, `chat_tx`) into `ProgressCallback` adapters for seamless live status updates.

---

## Considered Options

- **Subprocess Spawning in Each Subsystem**: Retaining raw `tokio::process::Command` calls in consolidation and VOD. Rejected because it duplicates JSON log parsing, stderr buffering, and Windows process lifecycle handling, while requiring slow and brittle external mock binaries in tests.
- **Subsystem-Specific Storage Traits (`LiveUploadBackend`, `ConsolidationBackend`)**: Splitting storage contracts into separate traits for streaming live chunks versus batch operations. Rejected because all subsystems fundamentally perform identical primitives (uploading to destination, atomic renaming, idempotent deletion, and health checking). A single unified trait prevents interface fragmentation and allows a single mock fixture to serve the entire test suite.
- **Direct Cloud SDKs (AWS SDK, Google Cloud Client)**: Embedding native Rust cloud provider SDKs instead of delegating to `rclone`. Rejected to prevent massive binary bloat and compile-time inflation, and to retain `rclone`'s broad support for over 40 cloud storage providers and user configuration files.

---

## Consequences

- **Single Remote Storage Seam**: Live broadcast recording, VOD archival, and post-recording consolidation share an identical storage abstraction (`UploadBackend`), eliminating structural fragmentation.
- **Hermetic Subprocess Boundary**: Raw rclone CLI subprocess invocations (`copyto`, `moveto`, `deletefile`) are strictly isolated within `src/uploader/rclone.rs`. Domain logic contains zero raw CLI process management.
- **Fast, Deterministic In-Memory Testing**: VOD archival and consolidation tests can execute against `MockUploadBackend` in sub-milliseconds without external tools, disk I/O, or Windows process lock flakes.
- **Cooperative Cancellation & Safe Rollback**: Spawning processes with `kill_on_drop(true)` guarantees that dropping a transfer future instantly kills the child process. Both VOD archival and consolidation leverage idempotent `delete_file` calls to cleanly scrub remote `.part` files upon cancellation or failure.
- **Preserved Backwards Compatibility**: Existing recording engine workflows, DLQ retries, and unit tests using `upload_file` and `mock_backend.uploads` continue to function without alteration.
