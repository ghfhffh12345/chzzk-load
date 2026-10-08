pub mod chat;
pub mod manifest;
pub mod video;

pub use chat::{
    ChatConsolidationStats, ChatDeduplicator, ChatMessageKey, DEFAULT_CHAT_DEDUP_WINDOW_MS,
    consolidate_chat, consolidate_chat_local, consolidate_chat_remote, delete_remote_file,
};
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
use crate::uploader::unlink_local_file_with_retry;
use anyhow::Context;

/// Summary report returned upon consolidation completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidationSummary {
    pub target: TargetLocation,
    pub video_chunks_count: usize,
    pub chat_chunks_count: usize,
    pub has_metadata: bool,
    pub warnings: Vec<String>,
    pub chat_stats: Option<ChatConsolidationStats>,
    pub video_result: Option<VideoConsolidationResult>,
}

/// Deletes original chunk files (.ts and .jsonl) upon successful consolidation completion.
///
/// Under `TargetLocation::Local`, unlinks files using bounded retry logic handling Windows locks.
/// Under `TargetLocation::Remote`, deletes files using `rclone deletefile`.
///
/// Invariant: `metadata.jsonl` is NEVER deleted.
pub async fn delete_original_chunks(
    target: &TargetLocation,
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
) -> anyhow::Result<()> {
    delete_original_chunks_with_bin(target, video_chunks, chat_chunks, None).await
}

/// Deletes original chunk files (.ts and .jsonl) with an optional custom rclone binary path.
pub async fn delete_original_chunks_with_bin(
    target: &TargetLocation,
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            for chunk in video_chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(".ts") {
                    continue;
                }
                let path = dir.join(&chunk.name);
                if let Err(e) = unlink_local_file_with_retry(&path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(e).with_context(|| {
                            format!("Failed to delete local video chunk '{}'", path.display())
                        });
                    }
                }
            }
            for chunk in chat_chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(".jsonl") {
                    continue;
                }
                let path = dir.join(&chunk.name);
                if let Err(e) = unlink_local_file_with_retry(&path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(e).with_context(|| {
                            format!("Failed to delete local chat chunk '{}'", path.display())
                        });
                    }
                }
            }
        }
        TargetLocation::Remote(remote_base) => {
            let bin = resolve_rclone_bin(rclone_bin);
            for chunk in video_chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(".ts") {
                    continue;
                }
                let remote_chunk_path = join_remote_path(remote_base, &chunk.name);
                delete_remote_file(&bin, &remote_chunk_path).await;
            }
            for chunk in chat_chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(".jsonl") {
                    continue;
                }
                let remote_chunk_path = join_remote_path(remote_base, &chunk.name);
                delete_remote_file(&bin, &remote_chunk_path).await;
            }
        }
    }
    Ok(())
}

/// Runs the post-recording consolidation workflow.
///
/// Discovers and validates the manifest, concurrently executes streaming video remuxing
/// and chat deduplication pipelines via `tokio::try_join!`, and purges original chunks
/// when `--keep-original` is false while strictly preserving `metadata.jsonl`.
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

    let has_video = !manifest.video_chunks.is_empty();
    let has_chat = !manifest.chat_chunks.is_empty();

    let video_options = VideoConsolidationOptions::default();
    let (video_result, chat_stats) = if has_video && has_chat {
        let (v_res, c_res) = tokio::try_join!(
            consolidate_video(&manifest.target, &manifest.video_chunks, &video_options),
            consolidate_chat(&manifest.target, &manifest.chat_chunks, args.strict, None),
        )?;
        (Some(v_res), Some(c_res))
    } else if has_video {
        let v_res =
            consolidate_video(&manifest.target, &manifest.video_chunks, &video_options).await?;
        (Some(v_res), None)
    } else if has_chat {
        let c_res =
            consolidate_chat(&manifest.target, &manifest.chat_chunks, args.strict, None).await?;
        (None, Some(c_res))
    } else {
        (None, None)
    };

    if let Some(ref v) = video_result {
        println!(
            "[INFO] Video consolidation completed: {} chunk(s) processed, {} bytes written",
            v.chunks_processed, v.bytes_written
        );
    }
    if let Some(ref c) = chat_stats {
        println!(
            "[INFO] Chat consolidation completed: {} total, {} deduplicated, {} malformed, {} emitted",
            c.total_messages, c.deduplicated_messages, c.malformed_messages, c.emitted_messages,
        );
    }

    if !args.keep_original {
        delete_original_chunks(
            &manifest.target,
            &manifest.video_chunks,
            &manifest.chat_chunks,
        )
        .await?;
        println!("[INFO] Original chunks cleaned up successfully");
    }

    Ok(ConsolidationSummary {
        target,
        video_chunks_count: manifest.video_chunks.len(),
        chat_chunks_count: manifest.chat_chunks.len(),
        has_metadata: manifest.has_metadata,
        warnings: manifest.warnings,
        chat_stats,
        video_result,
    })
}
