pub mod manifest;
pub mod video;

pub use manifest::{
    ChunkGap, ConsolidationChunk, ConsolidationManifest, RawManifestEntry, TargetLocation,
    build_manifest_from_entries, detect_index_gaps, discover_manifest, discover_manifest_with_bin,
    is_remote_path, parse_chat_chunk_index, parse_video_chunk_index, resolve_rclone_bin,
};
pub use video::{
    VideoConsolidationOptions, VideoConsolidationResult, build_ffmpeg_remux_args,
    build_ffmpeg_remux_command, cleanup_staged_video, consolidate_video, feed_video_chunks,
    finalize_staged_video, join_remote_path, rename_local_file_with_retry, resolve_ffmpeg_bin,
};

use crate::cli::ConsolidateArgs;

/// Summary report returned upon consolidation completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationSummary {
    pub target: TargetLocation,
    pub video_chunks_count: usize,
    pub chat_chunks_count: usize,
    pub has_metadata: bool,
    pub warnings: Vec<String>,
}

/// Runs the post-recording consolidation workflow.
///
/// In Ticket #22, this discovers and validates the manifest against contiguity,
/// single-media presence, and pre-existing file invariants.
pub async fn run_consolidation(args: ConsolidateArgs) -> anyhow::Result<ConsolidationSummary> {
    let target = TargetLocation::parse(&args.path);
    let manifest = discover_manifest(&target, args.strict, args.overwrite).await?;

    println!(
        "[INFO] Discovered manifest for '{}': {} video chunk(s), {} chat chunk(s) (metadata: {})",
        target.raw(),
        manifest.video_chunks.len(),
        manifest.chat_chunks.len(),
        manifest.has_metadata,
    );
    for w in &manifest.warnings {
        eprintln!("[WARN] {w}");
    }

    Ok(ConsolidationSummary {
        target,
        video_chunks_count: manifest.video_chunks.len(),
        chat_chunks_count: manifest.chat_chunks.len(),
        has_metadata: manifest.has_metadata,
        warnings: manifest.warnings,
    })
}
