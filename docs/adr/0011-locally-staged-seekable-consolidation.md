# 0011 - Locally-Staged Seekable Post-Recording Consolidation

**Status**: Accepted (supersedes ADR 0009 and ADR 0010 streaming output)

Post-recording consolidation (`chzzk-load consolidate <path>`) previously utilized pure network pipe streaming into `rclone rcat` (ADR 0009, ADR 0010) to support zero-disk VPS instances. However, streaming writes over stdout force fragmented MP4 container flags (`frag_keyframe+empty_moov`), omitting the centralized `moov` atom seek table (`+faststart`) and causing initial playback freezes, broken scrubbing, and timeline irregularities in desktop players and video editing software.

We deprecate the legacy pure-streaming pipeline in favor of **Locally-Staged Seekable Post-Recording Consolidation**, bounding peak local disk consumption to strictly 1× the stream size by streaming remote chunks into memory while writing a normalized, seekable MP4 container directly to local disk prior to cloud upload.

## Decision

1. **Strictly Bounded 1× Local Disk Staging**:
   - Decouple input and output storage mechanisms.
   - Remote `.ts` chunks stream on demand from cloud storage via `EphemeralLoopbackServer` (`rclone serve http`) without saving chunk files to local disk (0× chunk storage).
   - FFmpeg writes directly to a seekable local staging file (`temp_dir/consolidated.mp4.part`), bounding local disk consumption strictly to 1× the stream size.

2. **Container Normalization with Faststart Seek Table**:
   - Remux video using FFmpeg's Concat Demuxer with `-avoid_negative_ts make_zero` and monotonic bitstream filters (`VIDEO_MONOTONIC_DTS_BSF`, `AUDIO_MONOTONIC_DTS_BSF`).
   - Configure `-movflags +faststart` to relocate the `moov` atom header to the beginning of the file, providing instant seek table indexing and smooth scrubbing in desktop players.

3. **Symmetric Local Chat Staging**:
   - Deduplicate live chat concurrently with video remuxing into `temp_dir/consolidated.jsonl.part`.
   - Verify zero-exit completion of both video and chat locally before initiating any cloud uploads.

4. **Pre-flight Capacity Verification & Staging Workspace**:
   - Calculate total video bytes from manifest discovery.
   - Verify that free disk space at the staging location (default: `std::env::temp_dir() / chzzk-load-consolidation-<hash>`) is at least $1.15\times$ total video bytes.
   - If capacity is insufficient, fail fast with an error detailing required vs. available space and suggest `--temp-dir <path>` to specify a drive with adequate storage.

5. **Two-Stage Atomic Upload & Verified Purge**:
   - Upload local files to `remote:path/consolidated.mp4.part` and `remote:path/consolidated.jsonl.part` via `rclone copyto`.
   - Atomically rename remote `.part` files via `rclone moveto`.
   - Upon confirmed success, execute `Concurrent Chunk Purge` on remote `.ts` and `.jsonl` files (unless `--keep-original` is set) and unlink the local temporary staging directory.

6. **Resilient Upload Resume**:
   - If an upload is interrupted, retain the finalized local files in the staging directory.
   - Subsequent invocations inspect existing staging files: if a valid container (`moov` atom) is present, log a resume notice and proceed directly to upload without re-remuxing, unless `--overwrite` is explicitly specified.

7. **Full Deprecation of Legacy Pure-Streaming**:
   - Eliminate `rclone rcat` stdout pipe streaming, removing fragmented MP4 container configurations entirely from the consolidation pipeline.

## Considered Options

- **Full Local Staging (2× Disk Footprint)**: Downloading all `.ts` chunks locally before remuxing requires $2\times$ disk space ($1\times$ chunks + $1\times$ MP4), needlessly exhausting disk quotas when remote chunks can be streamed via HTTP loopback.
- **Sliding-Window Progressive Chunk Deletion**: Downloading chunks in a rolling queue and unlinking each chunk after demuxing risks Windows sharing violations (error 32) due to open file handles in FFmpeg's concat demuxer.
- **Retaining Fragmented MP4 Streaming as an Opt-in Fallback**: Rejected because fragmented MP4 files suffer from fragmented timetables and playback freezes, failing user quality expectations.

## Consequences

- 100% seekable, standard MP4 files with centralized seek tables (`+faststart`) and normalized timelines starting at 0.0s.
- Local disk usage is strictly capped at 1× stream size instead of 2×.
- Eliminates Windows file-locking hazards by keeping chunk transport in-memory via loopback HTTP.
- Environments running `chzzk-load consolidate` on cloud remotes must have sufficient temporary disk space for 1× the stream size (or mount external storage via `--temp-dir`).
