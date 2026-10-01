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
