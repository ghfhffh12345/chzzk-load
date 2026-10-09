# chzzk-load Domain Glossary

Monitors Naver Chzzk live streams, archives lossless video segments and structured chat, and synchronizes files to remote storage with bounded disk footprint.

## Language

**Channel**:
A Naver Chzzk broadcaster stream identified by a unique channel identifier.
_Avoid_: Streamer, user, account

**Recording Session**:
An active, concurrent capture lifecycle for a live broadcast, managing video segments, live chat archiving, and metadata tracking.
_Avoid_: Broadcast, job, stream recorder

**Channel Lifecycle Registry**:
The state machine coordinator that manages channel transitions, cooldown windows, access restrictions, and cancellation tokens behind a single atomic lock.
_Avoid_: Engine state, channel manager, session store

**Session Custodian**:
The lifecycle coordinator that tracks recording session directories across active capture and draining phases, safely purging empty or quiescent session folders once in-flight chunk uploads complete, and sweeping unmanaged empty directories under strict directory emptiness.
_Avoid_: Folder cleaner, directory manager, session cleaner, purge service

**Cooldown Window**:
A grace period following broadcast conclusion that prevents redundant recording sessions during Chzzk CDN API cache expiration delays.
_Avoid_: Sleep, pause, debounce delay

**Restricted Stream**:
A live broadcast requiring authenticated Naver credentials due to pay-per-view access, channel membership, or age restriction.
_Avoid_: Private stream, blocked channel, locked stream

**Video Segmenter**:
The subprocess capture module responsible for lossless stream copy (`-c copy`), live chunk generation (`chunk_%04d.ts`), stderr diagnostic parsing, and graceful process termination.
_Avoid_: FFmpeg wrapper, video capture service, ffmpeg runner

**Grace Period**:
The bounded time window (15s) following a shutdown signal allowing active recording sessions to seal final chunks and upload queues to drain cleanly.
_Avoid_: Timeout, shutdown delay, wait interval

**Force Exit**:
An immediate process termination path triggered by a double `'q'` keystroke or Grace Period timeout expiration, aborting active tasks, executing bounded directory cleanup (<500ms), and exiting with code 1.
_Avoid_: Hard crash, emergency kill, dirty exit

**Dead-Letter Queue (DLQ)**:
The background queue that continuously retries failed file uploads upon network recovery, ensuring eventual consistency.
_Avoid_: Retry loop, failed upload cache

**Disk-Aware Retry Policy**:
The eviction strategy used by the DLQ under extreme disk pressure. If remaining space drops to `min_free_disk_gb`, the DLQ permanently deletes the oldest pending chunks to prevent disk-full crashes, while strictly preserving lean metadata tasks.
_Avoid_: Disk quota, auto-delete, purge strategy

**Task Retention Policy**:
The per-task lifecycle rule (`delete_on_success`) governing whether `UploadWorker` unlinks the local file post-upload (video segments, chat chunks, final stream teardown metadata, orphaned crash reconciliation) or retains it locally (intermediate live stream metadata snapshots).
_Avoid_: File deleter, delete flag, cleanup policy

**Strict Directory Emptiness**:
The invariant enforced exclusively by `SessionCustodian` requiring a session folder to contain exactly zero entries (`read_dir().count() == 0`) before removal, eliminating heuristic file-name sniffing.
_Avoid_: Empty check, heuristic delete, metadata purge

**Post-Recording Consolidation**:
The batch post-processing task that losslessly merges and remuxes fragmented video chunks (`.ts` to `.mp4`) and deduplicates chat logs (`.jsonl`) entirely over network pipes or local files with a strictly bounded memory footprint, circumventing local disk staging.
_Avoid_: Cloud merge, remote stitch, post-processing script

**Timestamp Normalization**:
The batch timeline reconciliation process in Post-Recording Consolidation that resets container start offsets to zero and linearly aligns Presentation Timestamps (PTS) and Decoding Timestamps (DTS) across fragmented chunk boundaries to prevent audio/video desync and playback freezes without video re-encoding.
_Avoid_: Timestamp reset, clock sync, re-mux timing, timeline patch

**Staged Remote Part**:
Temporary upload targets (`consolidated.mp4.part`, `consolidated.jsonl.part`) holding in-flight network consolidation streams until full process exit verification, preventing corrupt partial files upon unexpected failure.
_Avoid_: Temp file, incomplete download, partial upload

**Sliding-Window Chat Deduplication**:
The bounded time-window algorithm that identifies and eliminates duplicate live chat messages across chunk boundaries using monotonic message timestamps and identity tuples while maintaining $O(1)$ memory consumption.
_Avoid_: Chat filter, message dedup cache, hash set

**Concurrent Chunk Purge**:
The post-consolidation cleanup process that deletes original video and chat chunks concurrently under a bounded concurrency limit, rapidly reclaiming storage without exceeding API rate limits or local resource quotas.
_Avoid_: Parallel deleter, batch wipe, chunk cleaner

**Consolidation Progress Telemetry**:
The real-time progress tracking mechanism providing live visibility into simultaneous video remuxing, chat deduplication, and chunk purge states via interactive terminal bars or non-interactive diagnostic milestones.
_Avoid_: Progress monitor, consolidate logger, status bar

**Lean Metadata Snapshot**:
The flat JSON Lines event format (`metadata.jsonl`) recording essential stream lifecycle and classification state with monotonic offsets, synchronized via the unified upload DLQ pipeline and omitting volatile viewer telemetry and static CDN thumbnail URLs.
_Avoid_: Metadata diff, delta log, stream telemetry

**Live Stream Source**:
The interface seam representing live broadcast stream intake, encapsulating broadcast detail polling, chat session authorization token acquisition, and optional chat WebSocket endpoint configuration, decoupling engine orchestration from HTTP network transport.
_Avoid_: Poller, stream client, API wrapper, HTTP helper
