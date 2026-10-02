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
The eviction strategy used by the DLQ under extreme disk pressure. If remaining space drops to `min_free_disk_gb`, the DLQ permanently deletes the oldest pending chunks to prevent disk-full crashes.
_Avoid_: Disk quota, auto-delete, purge strategy

**Post-Recording Consolidation**:
The post-processing task that losslessly merges and remuxes remote video chunks (`.ts` to `.mp4`) and deduplicates chat logs (`.jsonl`) entirely over the network with a strictly bounded memory footprint, circumventing local disk usage.
_Avoid_: Cloud merge, remote stitch, post-processing script
