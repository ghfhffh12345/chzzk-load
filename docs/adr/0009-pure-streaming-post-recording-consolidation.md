# 0009 - Pure-Streaming Post-Recording Consolidation

**Status**: Accepted

Following a recording session, stream archives in remote storage exist as hundreds of fragmented video chunks (`chunk_%04d.ts`) and chat fragments (`chat_%04d.jsonl`). Merging these fragments on low-resource environments (e.g., 512MB/1GB VPS instances) without exhausting local disk capacity or memory requires a specialized consolidation pipeline.

ADR 0007 previously eliminated `rclone rcat` pipe streaming from the live `UploadBackend` to protect the live recording pipeline against concurrency bypasses and thread pool stalls during network degradation. However, post-recording consolidation is an offline batch lifecycle with fundamentally different constraints: it requires zero local disk consumption and strictly bounded $O(1)$ memory utilization when processing multi-hour recordings.

We establish an isolated, pure-streaming batch consolidation architecture (`chzzk-load consolidate <path>`) operating across network pipes without touching local disk or altering the live `UploadBackend` contract.

## Decision

1. **Strict Domain Isolation from ADR 0007**:
   - Preserve `UploadBackend` and `RcloneBackend` unchanged with their single `upload_file` contract for live recording ingestion and Dead-Letter Queue (DLQ) retries.
   - Implement post-recording consolidation in a dedicated module (`src/consolidation/`) with independent subprocess pipe management (`rclone cat`, `ffmpeg`, `rclone rcat`, `rclone moveto`, `rclone delete`).

2. **Network Pipe Streaming for Video Remuxing**:
   - Stream remote `.ts` chunks sequentially via an asynchronous Tokio feeder into a single `ffmpeg` stdin pipe.
   - Configure `ffmpeg` with `-c copy -movflags frag_keyframe+empty_moov -f mp4` to losslessly remux MPEG-TS into fragmented MP4 on the fly without transcoding or disk buffering.
   - Inject `-fflags +genpts+discardcorrupt` to absorb Presentation Timestamp (PTS) gaps and sequence discontinuities caused by missing intermediate chunks.
   - Pipe `ffmpeg` stdout directly into `rclone rcat <remote-path>/consolidated.mp4.part`.

3. **Sliding-Window Chat Deduplication**:
   - Stream remote `chat_%04d.jsonl` files sequentially into an asynchronous line reader.
   - Validate JSON lines and deduplicate messages within a bounded 10-second sliding time window using a composite key `(time_ms, user_id_hash, content)`, preserving $O(1)$ memory usage.
   - Pipe deduplicated lines directly into `rclone rcat <remote-path>/consolidated.jsonl.part`.
   - Skip malformed JSON lines with warnings, failing fast only when `--strict` is enabled.

4. **Atomic Staged Finalization & Failure Isolation**:
   - Stream video and chat outputs into temporary staged remote targets (`consolidated.mp4.part` and `consolidated.jsonl.part`).
   - Run video and chat consolidation concurrently via `tokio::try_join!`.
   - Upon zero-exit completion of all active pipelines, atomically rename `.part` files to `consolidated.mp4` and `consolidated.jsonl` via `rclone moveto`.
   - If either pipeline fails or aborts, attempt removal of the remote `.part` files via `rclone deletefile` and leave all original chunks untouched.

5. **All-or-Nothing Deletion & Metadata Preservation**:
   - Delete original remote `chunk_%04d.ts` and `chat_%04d.jsonl` files only after both video and chat consolidation have successfully completed and finalized.
   - Strictly preserve `metadata.jsonl` (Lean Metadata Snapshot) in remote storage to maintain stream classification, title history, and lifecycle audit logs.

6. **Unified Remote and Local Path Support**:
   - Support both remote rclone paths (`remote:bucket/path`) and local recording directories (`recordings/...`).
   - In local mode, execute native stream copy and in-place chunk deletion with a `--keep-original` safety flag.

## Considered Options

- **Extending `UploadBackend` with streaming methods (`rcat`)**: Rejected because post-recording consolidation is a separate batch post-processing lifecycle. Re-introducing pipe streaming to `UploadBackend` would re-introduce the exact interface bloat and architectural defects eliminated in ADR 0007.
- **Downloading chunks to local staging disk**: Rejected because 12-hour high-bitrate live streams produce 20–50 GB of video, which immediately exhausts disk quotas on low-cost VPS instances.
- **Direct write to destination without `.part` staging**: Rejected because mid-stream network failures or FFmpeg exits would leave partial, corrupt `.mp4` files in cloud storage, violating User Story 8 clean abort invariants.

## Consequences

- Post-recording consolidation guarantees $O(1)$ memory consumption and zero local disk usage regardless of broadcast duration.
- Live intake architecture (`UploadBackend`, `UploadWorker`, `DLQ`) remains completely decoupled from batch consolidation.
- Output MP4 files use fragmented MP4 (`frag_keyframe+empty_moov`), trading initial seek table centralization for zero-disk network streaming capability.
- Remote archives retain a clean, finalized layout consisting of `consolidated.mp4`, `consolidated.jsonl`, and `metadata.jsonl`.
