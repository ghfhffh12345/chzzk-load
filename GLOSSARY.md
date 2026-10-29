# chzzk-load Domain Glossary

Monitors Naver Chzzk live streams, archives lossless video segments and structured chat, and synchronizes files to remote storage with bounded disk footprint.

## Language

**Channel**:
A Naver Chzzk broadcaster stream identified by a unique channel identifier.
_Avoid_: Streamer, user, account

**Recording Session**:
An active capture lifecycle for a live broadcast, managing video segments, live chat archiving, and metadata tracking.
_Avoid_: Broadcast, job, stream recorder

**Channel Lifecycle Registry**:
The state machine coordinator that manages channel transitions, cooldown windows, access restrictions, and cancellation tokens behind a single atomic lock.
_Avoid_: Engine state, channel manager, session store

**Session Custodian**:
The lifecycle coordinator that tracks recording session directories across active capture and draining phases, safely purging empty or quiescent session folders once in-flight chunk uploads complete.
_Avoid_: Folder cleaner, directory manager, session cleaner, purge service

**Cooldown Window**:
A grace period following broadcast conclusion that prevents redundant recording sessions during Chzzk CDN API cache expiration delays.
_Avoid_: Sleep, pause, debounce delay

**Restricted Stream**:
A live broadcast requiring authenticated Naver credentials due to pay-per-view access, channel membership, or age restriction.
_Avoid_: Private stream, blocked channel, locked stream

**Video Segmenter**:
The subprocess capture module responsible for lossless stream copy, chunk generation, diagnostic parsing, and graceful process termination.
_Avoid_: FFmpeg wrapper, video capture service, ffmpeg runner

**Grace Period**:
The bounded time window following a shutdown signal allowing active recording sessions to seal final chunks and upload queues to drain cleanly.
_Avoid_: Timeout, shutdown delay, wait interval

**Force Exit**:
An immediate process termination path triggered by a double `'q'` keystroke or Grace Period expiration, aborting active tasks and executing bounded cleanup.
_Avoid_: Hard crash, emergency kill, dirty exit

**Dead-Letter Queue (DLQ)**:
The background queue that continuously retries failed file uploads upon network recovery, ensuring eventual consistency.
_Avoid_: Retry loop, failed upload cache

**Disk-Aware Retry Policy**:
The eviction strategy used by the DLQ under extreme disk pressure, permanently deleting the oldest pending chunks while strictly preserving lean metadata tasks.
_Avoid_: Disk quota, auto-delete, purge strategy

**Task Retention Policy**:
The per-task lifecycle rule (`delete_on_success`) governing whether local files are unlinked post-upload or retained locally.
_Avoid_: File deleter, delete flag, cleanup policy

**Strict Directory Emptiness**:
The invariant requiring a session folder to contain exactly zero filesystem entries before removal, eliminating heuristic file-name sniffing.
_Avoid_: Empty check, heuristic delete, metadata purge

**Post-Recording Consolidation**:
The batch post-processing task that losslessly merges fragmented video chunks and deduplicates chat logs into normalized, seekable containers prior to cloud synchronization.
_Avoid_: Cloud merge, remote stitch, post-processing script, legacy streaming pipeline

**Staged Local Consolidation Target**:
The temporary local files (`live_stream.mp4.part`, `live_chat.jsonl.part`) where normalized seekable media is assembled within a bounded disk footprint prior to upload.
_Avoid_: Temp file, intermediate cache, scratch chunk

**Timestamp Normalization**:
The batch timeline reconciliation process in Post-Recording Consolidation that resets start offsets to zero, linearly aligns Presentation Timestamps, and enforces strictly monotonic Decoding Timestamps without transcoding.
_Avoid_: Timestamp reset, clock sync, re-mux timing, timeline patch

**Staged Remote Part**:
Temporary upload targets (`live_stream.mp4.part`, `live_chat.jsonl.part`) holding in-flight network consolidation streams until full verification, preventing corrupt partial files upon failure.
_Avoid_: Temp file, incomplete download, partial upload

**Sliding-Window Chat Deduplication**:
The bounded time-window algorithm that identifies and eliminates duplicate live chat messages across chunk boundaries using monotonic timestamps and identity tuples.
_Avoid_: Chat filter, message dedup cache, hash set

**Concurrent Chunk Purge**:
The post-consolidation cleanup process that deletes original video and chat chunks concurrently under a bounded limit, rapidly reclaiming storage.
_Avoid_: Parallel deleter, batch wipe, chunk cleaner

**Consolidation Progress Telemetry**:
The real-time progress tracking mechanism providing live visibility into simultaneous video remuxing, chat deduplication, and chunk purge states via interactive terminal bars or milestone logs.
_Avoid_: Progress monitor, consolidate logger, status bar

**Lean Metadata Snapshot**:
The flat JSON Lines event format (`live_metadata.jsonl`) recording essential stream lifecycle and classification state with monotonic offsets, omitting volatile viewer telemetry and thumbnail URLs.
_Avoid_: Metadata diff, delta log, stream telemetry

**Live Stream Source**:
The interface seam representing live broadcast stream intake, encapsulating broadcast detail polling, chat authorization, and WebSocket configuration while decoupling orchestration from HTTP transport.
_Avoid_: Poller, stream client, API wrapper, HTTP helper

**VOD Archival Session**:
An on-demand batch capture lifecycle for an archived broadcast VOD, coordinating stream ingestion, timeline-synchronized chat pagination, and container finalization into seekable media.
_Avoid_: VOD rip, download job, stream grabber

**Progressive Stream Ingestion**:
The high-throughput download strategy for VODs that streams a monolithic progressive MP4 directly using multi-socket byte-range chunking, bypassing segment-level remuxing.
_Avoid_: Direct download, single-file rip, fast download

**Segmented Stream Fallback**:
The secondary ingestion strategy for VODs lacking progressive streams, downloading individual HLS segments via parallel workers and remuxing them through the FFmpeg concat demuxer.
_Avoid_: Chunk fallback, HLS rip, legacy segmenter

**Live Artifact Triad**:
The canonical three-file output specification (`live_stream.mp4`, `live_chat.jsonl`, `live_metadata.jsonl`) generated during live broadcast recording and post-recording consolidation.
_Avoid_: Consolidation bundle, live dump, stream files

**VOD Artifact Triad**:
The canonical three-file output specification (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`) generated during a VOD Archival Session, mirroring live recording structure.
_Avoid_: Download bundle, VOD package, media dump

**Broadcast Start Anchor**:
The authoritative origin timestamp of a VOD's original live broadcast, utilized to chronologically align session directories and relative chat playback offsets.
_Avoid_: VOD timestamp, download date, publish time

**Cursor-Resumable Staging**:
The partial-state retention strategy that preserves in-flight download progress to resume interrupted VOD downloads without refetching preceding data.
_Avoid_: Download resume, chunk cache, partial retry

**Dual-Event Boundary Model**:
The metadata synthesis pattern for archived VOD metadata (`vod_metadata.jsonl`), recording an initial `start` snapshot at offset zero and a terminal `finish` snapshot at the final duration.
_Avoid_: Metadata diff, start-stop tags, bookends, event wrappers
