pub mod chat;
pub mod manifest;
pub mod video;

pub use chat::{
    ChatConsolidationStats, ChatDeduplicator, ChatMessageKey, DEFAULT_CHAT_DEDUP_WINDOW_MS,
    cleanup_staged_chat, consolidate_chat, consolidate_chat_local, consolidate_chat_remote,
    delete_remote_file, delete_remote_file_checked, finalize_staged_chat,
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

use std::sync::Arc;

use crate::cli::ConsolidateArgs;
use crate::uploader::unlink_local_file_with_retry;

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

pub const DEFAULT_DELETE_CONCURRENCY: usize = 16;

/// Deletes original chunk files (.ts and .jsonl) upon successful consolidation completion
/// using the default deletion concurrency (16).
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
    delete_original_chunks_with_concurrency(
        target,
        video_chunks,
        chat_chunks,
        DEFAULT_DELETE_CONCURRENCY,
        None,
    )
    .await
}

/// Deletes original chunk files (.ts and .jsonl) with an optional custom rclone binary path
/// using the default deletion concurrency (16).
pub async fn delete_original_chunks_with_bin(
    target: &TargetLocation,
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    delete_original_chunks_with_concurrency(
        target,
        video_chunks,
        chat_chunks,
        DEFAULT_DELETE_CONCURRENCY,
        rclone_bin,
    )
    .await
}

/// Deletes original chunk files (.ts and .jsonl) concurrently under a bounded semaphore limit.
///
/// Pools video chunks (`.ts`) and chat chunks (`.jsonl`) into a single unified queue,
/// strictly excluding `metadata.jsonl`.
///
/// Implements best-effort failure aggregation: all tasks run to completion, and any failures
/// are collected and reported in an aggregate error summary upon completion.
pub async fn delete_original_chunks_with_concurrency(
    target: &TargetLocation,
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
    concurrency: usize,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    let mut eligible = Vec::with_capacity(video_chunks.len() + chat_chunks.len());
    for chunk in video_chunks {
        if chunk.name != "metadata.jsonl" && chunk.name.ends_with(".ts") {
            eligible.push(chunk.name.clone());
        }
    }
    for chunk in chat_chunks {
        if chunk.name != "metadata.jsonl" && chunk.name.ends_with(".jsonl") {
            eligible.push(chunk.name.clone());
        }
    }

    if eligible.is_empty() {
        return Ok(());
    }

    let concurrency_limit = concurrency.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency_limit));
    let mut join_set = tokio::task::JoinSet::new();

    for name in eligible {
        let sem = Arc::clone(&semaphore);
        let target = target.clone();
        let bin = rclone_bin.map(str::to_string);
        join_set.spawn(async move {
            let permit = sem
                .acquire_owned()
                .await
                .map_err(|e| anyhow::anyhow!("Semaphore acquire failed: {e}"))?;
            let res = match target {
                TargetLocation::Local(ref dir) => {
                    let path = dir.join(&name);
                    match unlink_local_file_with_retry(&path).await {
                        Ok(()) => Ok(()),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(e) => Err(anyhow::Error::from(e)
                            .context(format!("Failed to delete local chunk '{}'", path.display()))),
                    }
                }
                TargetLocation::Remote(ref remote_base) => {
                    let path = join_remote_path(remote_base, &name);
                    let bin_str = resolve_rclone_bin(bin.as_deref());
                    delete_remote_file_checked(&bin_str, &path).await
                }
            };
            drop(permit);
            res
        });
    }

    let mut errors = Vec::new();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                errors.push(err);
            }
            Err(join_err) => {
                errors.push(anyhow::anyhow!(
                    "Purge task panicked or aborted: {join_err}"
                ));
            }
        }
    }

    if !errors.is_empty() {
        let count = errors.len();
        let sample: Vec<String> = errors.iter().take(5).map(|e| format!("{e:#}")).collect();
        let sample_str = sample.join("\n  - ");
        let extra = if count > 5 {
            format!("\n  ... and {} more error(s)", count - 5)
        } else {
            String::new()
        };
        anyhow::bail!(
            "Failed to delete {count} chunk(s) during concurrent purge:\n  - {sample_str}{extra}"
        );
    }

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

        let (video_res, chat_res) = tokio::join!(video_fut, chat_fut);
        match (video_res, chat_res) {
            (Ok(v_res), Ok(c_res)) => {
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
            (Err(v_err), Ok(_)) => {
                cancel_token.cancel();
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(v_err);
            }
            (Ok(_), Err(c_err)) => {
                cancel_token.cancel();
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(c_err);
            }
            (Err(v_err), Err(c_err)) => {
                cancel_token.cancel();
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;

                let v_is_cancel = is_cancellation_error(&v_err);
                let c_is_cancel = is_cancellation_error(&c_err);

                let primary_err = match (v_is_cancel, c_is_cancel) {
                    (false, true) => v_err,
                    (true, false) => c_err,
                    (false, false) => anyhow::anyhow!(
                        "Dual consolidation pipeline failure: video error: {v_err:#}; chat error: {c_err:#}"
                    ),
                    (true, true) => v_err,
                };
                return Err(primary_err);
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
        delete_original_chunks_with_concurrency(
            &manifest.target,
            &manifest.video_chunks,
            &manifest.chat_chunks,
            args.delete_concurrency,
            None,
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

fn is_cancellation_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("cancelled by cooperative cancellation token")
}
