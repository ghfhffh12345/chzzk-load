# VOD Archival Pipeline and Multi-Connection Ingestion

To extend `chzzk-load` with high-throughput VOD downloading without targeting ultra-low-spec SBC devices, we introduce the `vod` subcommand backed by an external `aria2c` multi-connection ingestion pipeline, timeline-synchronized chat pagination, and lossless container finalization. This architecture produces a canonical VOD Artifact Triad (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`) staged under two-stage atomic guarantees, anchored chronologically by the broadcast's original `liveOpenDate`.

## Status

Accepted (Amended: Dual Ingestion Pipeline)

## Considered Options

1. **Native Tokio Async HTTP Chunk Workers**: Downloading segments entirely within Rust Tokio tasks without external `aria2c`. Rejected to maximize raw saturation on multi-gigabit connections and leverage battle-tested connection pooling, multi-socket chunking, and session file recovery provided by `aria2c`.
2. **Direct FFmpeg Manifest Ingestion (`ffmpeg -i <hls_url> -c copy`)**: Delegating the entire download to FFmpeg's internal HLS demuxer. Rejected because FFmpeg downloads segments sequentially over a single connection, leading to suboptimal throughput on large multi-hour 1080p60 VODs.
3. **Live Recording Name Reuse (`live_stream.mp4`)**: Naming output files `live_stream.mp4` to match live capture. Rejected in favor of the VOD Artifact Triad (`vod.mp4`, `vod_chat.jsonl`, `vod_metadata.jsonl`) to distinguish post-hoc VOD downloads from real-time recordings while preserving identical internal JSON Lines and MP4 container conventions.
4. **Exclusive Segmented HLS Ingestion**: Restricting downloads to HLS `.ts` playlists only. Superseded by the Dual Ingestion Pipeline: progressive MP4 single-file streams are prioritized via multi-socket byte-range chunking (`aria2c -x 16 -s 16 -k 1M`) to match multi-gigabit throughput line speeds, with HLS segment ingestion retained strictly as a fallback.

## Consequences

- An external `aria2c` binary is strictly required for the `vod` subcommand (resolved via `CHZZK_LOAD_ARIA2C_BIN`, `[aria2c]` in `settings.toml`, and `PATH`).
- NeonPlayer playback manifests prioritize monolithic progressive MP4 representations matching requested quality (`find_progressive_mp4_url`), streaming directly into staged `vod.mp4.part` via 16 concurrent HTTP byte-range connections.
- When progressive streams are absent or quality constraints require HLS variants, the pipeline seamlessly falls back to Segmented Stream Fallback, downloading `.ts` segments into the staging workspace and remuxing via FFmpeg concat demuxer.
- Signed CDN tokens (e.g. `__gda__` query parameters on Akamai) and browser request headers (`User-Agent`, `Referer`, `Origin`) are forwarded to `aria2c` to avoid CDN QoS bandwidth shaping.
- Interrupted downloads are cursor-resumable via `aria2c -c` control files (`vod.mp4.part.aria2`) or retained segment files, paired with cursor-resumable `vod_chat.jsonl.part`.
- Container integrity is verified via `validate_mp4_container_integrity` prior to two-stage atomic promotion to `<output_dir>/vod.mp4`, invoking FFmpeg faststart normalization only if structural repairs are needed.
- Terminal telemetry provides a 10 Hz dual-bar display (`[VID]` and `[CHAT]`) with windowed byte-delta aggregate throughput calculation.
