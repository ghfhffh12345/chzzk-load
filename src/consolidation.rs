pub mod chat;
pub mod manifest;
pub mod video;

pub use chat::{
    ChatConsolidationStats, ChatDeduplicator, ChatMessageKey, DEFAULT_CHAT_DEDUP_WINDOW_MS,
    cleanup_staged_chat, consolidate_chat, consolidate_chat_local, consolidate_chat_remote,
    delete_remote_file, finalize_staged_chat,
};
pub use manifest::{
    ChunkGap, ConsolidationChunk, ConsolidationManifest, RawManifestEntry, TargetLocation,
    build_manifest_from_entries, detect_index_gaps, discover_manifest, discover_manifest_with_bin,
    is_remote_path, join_remote_path, parse_chat_chunk_index, parse_video_chunk_index,
    resolve_rclone_bin,
};
pub use video::{
    VideoConsolidationOptions, VideoConsolidationResult, VideoProgressTelemetry,
    build_ffmpeg_remux_args, build_ffmpeg_remux_command, cleanup_staged_video, consolidate_video,
    feed_video_chunks, finalize_staged_video, rename_local_file_with_retry, resolve_ffmpeg_bin,
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

/// Helper function to purge a list of chunk files matching an expected extension.
async fn purge_chunk_list(
    target: &TargetLocation,
    chunks: &[ConsolidationChunk],
    expected_extension: &str,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            for chunk in chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(expected_extension) {
                    continue;
                }
                let path = dir.join(&chunk.name);
                if let Err(e) = unlink_local_file_with_retry(&path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(e).with_context(|| {
                            format!("Failed to delete local chunk '{}'", path.display())
                        });
                    }
                }
            }
        }
        TargetLocation::Remote(remote_base) => {
            let bin = resolve_rclone_bin(rclone_bin);
            for chunk in chunks {
                if chunk.name == "metadata.jsonl" || !chunk.name.ends_with(expected_extension) {
                    continue;
                }
                let remote_chunk_path = join_remote_path(remote_base, &chunk.name);
                delete_remote_file(&bin, &remote_chunk_path).await;
            }
        }
    }
    Ok(())
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
    purge_chunk_list(target, video_chunks, ".ts", rclone_bin).await?;
    purge_chunk_list(target, chat_chunks, ".jsonl", rclone_bin).await?;
    Ok(())
}

/// Runs the post-recording consolidation workflow.
///
/// Discovers and validates the manifest, concurrently executes streaming video remuxing
/// and chat deduplication pipelines via `tokio::try_join!`, atomically finalizes staged `.part`
/// files upon zero-exit completion of all active pipelines, and purges original chunks
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
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (video_result, chat_stats) = if has_video && has_chat {
        let video_fut = consolidate_video(
            &manifest.target,
            &manifest.video_chunks,
            &video_options,
            cancel_token.clone(),
        );
        let chat_fut = consolidate_chat(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            None,
            cancel_token.clone(),
        );

        match tokio::try_join!(video_fut, chat_fut) {
            Ok((v_res, c_res)) => {
                // Both pipelines completed with zero-exit: atomically finalize both .part files
                let finalize_res = async {
                    finalize_staged_video(&manifest.target, None).await?;
                    finalize_staged_chat(&manifest.target, None).await?;
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                if let Err(e) = finalize_res {
                    let _ = cleanup_staged_video(&manifest.target, None).await;
                    let _ = cleanup_staged_chat(&manifest.target, None).await;
                    return Err(e);
                }
                (Some(v_res), Some(c_res))
            }
            Err(err) => {
                cancel_token.cancel();
                // Clean up both .part files; original chunks remain untouched
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(err);
            }
        }
    } else if has_video {
        eprintln!(
            "[INFO] No chat chunks found; skipping chat consolidation pipeline (video-only session)"
        );
        let v_res = match consolidate_video(
            &manifest.target,
            &manifest.video_chunks,
            &video_options,
            cancel_token.clone(),
        )
        .await
        {
            Ok(res) => {
                if let Err(e) = finalize_staged_video(&manifest.target, None).await {
                    let _ = cleanup_staged_video(&manifest.target, None).await;
                    return Err(e);
                }
                res
            }
            Err(err) => {
                cancel_token.cancel();
                let _ = cleanup_staged_video(&manifest.target, None).await;
                return Err(err);
            }
        };
        (Some(v_res), None)
    } else if has_chat {
        eprintln!(
            "[INFO] No video chunks found; skipping video consolidation pipeline (chat-only session)"
        );
        let c_res = match consolidate_chat(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            None,
            cancel_token.clone(),
        )
        .await
        {
            Ok(res) => {
                if let Err(e) = finalize_staged_chat(&manifest.target, None).await {
                    let _ = cleanup_staged_chat(&manifest.target, None).await;
                    return Err(e);
                }
                res
            }
            Err(err) => {
                cancel_token.cancel();
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(err);
            }
        };
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
