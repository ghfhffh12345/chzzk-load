# 0010 - Concat Demuxer Timestamp Normalization for Post-Recording Consolidation

**Status**: Accepted

Following a recording session, stream archives consist of fragmented video chunks (`chunk_%04d.ts`) and chat fragments (`chat_%04d.jsonl`). Under ADR 0009, post-recording consolidation (`chzzk-load consolidate <path>`) merged video chunks by sequentially streaming raw byte streams of consecutive MPEG-TS files into FFmpeg's `stdin` (`pipe:0`) and losslessly remuxing to MP4.

While raw byte pipe streaming satisfied ADR 0009's requirement for $O(1)$ memory consumption and zero local disk usage in cloud environments, empirical analysis and user reports revealed critical container-level defects:
1. **Boundary Packet Drops & Discontinuities**: Feeding independent MPEG-TS chunks over a continuous raw byte stream caused FFmpeg's MPEG-TS input demuxer to encounter unexpected Program Association Table (PAT) / Program Map Table (PMT) resets and discontinuity indicators at chunk boundaries. This triggered non-monotonic DTS warnings, audio/video packet drop warnings, and occasional dropped boundary frames across chunk seams.
2. **Unaligned Start Offsets & Playback Freezing**: Live broadcasts recorded with non-zero initial Presentation Timestamps (PTS) or micro-gaps retained their non-zero start offsets when remuxed without timeline normalization. This caused initial playback freezes (e.g. VLC, QuickTime, or web players freezing for several seconds before playback started) and negative Composition Time Offset (CTS) errors.
3. **Missing Desktop Seek Tables in Local Mode**: ADR 0009 forced fragmented MP4 output (`-movflags frag_keyframe+empty_moov`) across both local and remote consolidation. While necessary for network streaming writes over stdout (`rclone rcat`), fragmented MP4 lacks a centralized `moov` atom seek table placed at the front of the file (`+faststart`). Local desktop players (VLC, QuickTime, Windows Media Player) could not determine total duration or perform instant scrubbing without reading the entire file.

We establish container-aware **Concat Demuxer Timestamp Normalization** across both local and remote consolidation targets, eliminating raw stdin pipe feeding while strictly preserving ADR 0009's zero local disk footprint in remote mode.

## Decision

1. **Native Concat Demuxer Architecture (`-f concat`)**:
   - Replace raw byte pipe feeding (`feed_video_chunks` via `pipe:0`) with FFmpeg's native container-aware Concat Demuxer (`-f concat -safe 0`).
   - Rather than concatenating raw bytes, FFmpeg demuxes each segment through its container parser, honoring transport stream boundaries, cleanly resetting PAT/PMT state, and reconciling timestamps across chunk transitions without packet loss.

2. **Concat Manifest Script Generation & RAII Guard (`ConcatScriptGuard`)**:
   - Generate temporary concat manifest scripts (`file '<escaped-path>'`) in `std::env::temp_dir()`.
   - Enforce path normalization and escaping: normalize Windows backslashes (`\`) to forward slashes (`/`), and escape single quotes (`'`) as `'\''` to protect Korean streamer names, spaces, brackets, and HTTP URLs.
   - Strictly write UTF-8 content without Byte Order Mark (BOM) to prevent demuxer keyword parse errors.
   - Encapsulate the script path inside an RAII `ConcatScriptGuard` that unlinks the temporary file upon Drop (on normal completion, error, panic, or cancellation).

3. **Timestamp Normalization (`-avoid_negative_ts make_zero`)**:
   - Configure FFmpeg with `-avoid_negative_ts make_zero` across all consolidation remuxing jobs.
   - Resets container Presentation Timestamps (PTS) and Decoding Timestamps (DTS) so playback starts strictly at 0.0s, eliminating initial playback freezes and audio/video desync without video transcoding.

4. **Local Mode: Direct Seekable Remuxing with Faststart Seek Table**:
   - In local mode (`TargetLocation::Local`), output directly to `consolidated.mp4.part` on the local filesystem rather than piping through stdout.
   - Configure `-movflags +faststart` to relocate the `moov` atom header to the beginning of the MP4 file upon completion.
   - Enables instant seek table indexing, precise duration reporting, and smooth scrubbing in desktop players.

5. **Remote Mode: Ephemeral Loopback Server & Zero-Disk Streaming**:
   - In remote mode (`TargetLocation::Remote`), spawn an ephemeral `rclone serve http <remote_base> --addr 127.0.0.1:0 --read-only` subprocess binding to a dynamic operating system port.
   - Asynchronously parse the dynamically assigned port from rclone's startup log (`HTTP Server started on [http://127.0.0.1:PORT/]`).
   - Feed loopback URLs (`http://127.0.0.1:<port>/chunk_%04d.ts`) to FFmpeg's Concat Demuxer using `-protocol_whitelist file,http,tcp`.
   - Retain fragmented MP4 container flags for streaming writes (`-movflags frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets`) and pipe FFmpeg stdout into `rclone rcat <staged.part>`.
   - Encapsulate the loopback subprocess in an RAII `EphemeralLoopbackServer` that terminates (`child.start_kill()`) upon normal exit, error, or cooperative cancellation.
   - Preserves ADR 0009's core invariant: zero local disk chunk staging and bounded $O(1)$ memory consumption.

6. **Progress Telemetry Adaptation**:
   - Include `-progress pipe:2` across both local and remote FFmpeg invocations to emit structured key-value progress lines (`total_size=...`, `speed=...`) on stderr.
   - Update `VideoProgressTelemetry::update_from_line` to parse both structured `-progress pipe:2` key-values and traditional FFmpeg status lines (`frame=... size=...kB speed=...`).
   - Proportionally map `total_size` against cumulative manifest chunk bytes to dispatch `VideoProgressUpdate::ChunkFed` and `VideoProgressUpdate::Speed`, driving interactive dual-line Cloud Upload progress bars and non-interactive milestone logs smoothly.

## Considered Options

- **Full Video Re-encoding (`-c:v libx264 -c:a aac`)**: Rejected because re-encoding multi-hour 1080p60 broadcasts requires massive CPU/GPU resources, introduces generational loss, and takes hours instead of seconds. Stream-copy (`-c copy`) remuxing with Concat Demuxer achieves boundary-free continuity in seconds at near I/O speed.
- **Downloading Remote Chunks to Local Disk for Staging**: Rejected because multi-hour streams produce 20–50 GB of chunk data, immediately exhausting disk quotas on low-cost VPS instances and violating ADR 0009 invariants.
- **Retaining Raw Stdin Pipe Feeding with Filter Graphs**: Rejected because MPEG-TS container reset discontinuities occur at the transport stream layer before filter graphs execute. FFmpeg's native Concat Demuxer is explicitly designed for multi-file container stitching.

## Consequences

- 100% preserved frame counts (`nb_read_frames`) and packet counts across chunk seams with zero dropped boundary packets.
- Container timelines start precisely at 0.0s, eliminating playback freezing and audio/video desynchronization.
- Locally consolidated MP4 files feature a centralized `moov` atom seek table (`+faststart`) for instant seeking in VLC, QuickTime, and Windows Media Player.
- Remote consolidation retains zero local disk usage and bounded $O(1)$ RAM consumption while gaining the exact same stitching fidelity as local mode.
- RAII guards (`ConcatScriptGuard`, `EphemeralLoopbackServer`) guarantee zero resource leakage under all execution paths (success, error, panic, or cancellation).
