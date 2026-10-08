pub mod chat;
pub mod manifest;

pub use chat::{
    ChatConsolidationStats, ChatDeduplicator, ChatMessageKey, DEFAULT_CHAT_DEDUP_WINDOW_MS,
    consolidate_chat, consolidate_chat_local, consolidate_chat_remote, delete_remote_file,
    join_remote_path,
};
pub use manifest::{
    ChunkGap, ConsolidationChunk, ConsolidationManifest, RawManifestEntry, TargetLocation,
    build_manifest_from_entries, detect_index_gaps, discover_manifest, discover_manifest_with_bin,
    is_remote_path, parse_chat_chunk_index, parse_video_chunk_index, resolve_rclone_bin,
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
    pub chat_stats: Option<ChatConsolidationStats>,
}

/// Runs the post-recording consolidation workflow.
///
/// In Ticket #22, this discovers and validates the manifest against contiguity,
/// single-media presence, and pre-existing file invariants.
/// In Ticket #23, this executes streaming chat deduplication when chat chunks are present.
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

    let mut chat_stats = None;
    if !manifest.chat_chunks.is_empty() {
        let stats =
            consolidate_chat(&manifest.target, &manifest.chat_chunks, args.strict, None).await?;
        println!(
            "[INFO] Chat consolidation completed: {} total, {} deduplicated, {} malformed, {} emitted",
            stats.total_messages,
            stats.deduplicated_messages,
            stats.malformed_messages,
            stats.emitted_messages,
        );
        chat_stats = Some(stats);
    }

    Ok(ConsolidationSummary {
        target,
        video_chunks_count: manifest.video_chunks.len(),
        chat_chunks_count: manifest.chat_chunks.len(),
        has_metadata: manifest.has_metadata,
        warnings: manifest.warnings,
        chat_stats,
    })
}
