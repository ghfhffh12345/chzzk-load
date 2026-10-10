pub mod chat;
pub mod loopback;
pub mod manifest;
pub mod progress;
pub mod video;

pub use loopback::{
    DEFAULT_LOOPBACK_STARTUP_TIMEOUT, EphemeralLoopbackServer, parse_loopback_port,
};

pub use progress::{
    ChatProgressSnapshot, ChatProgressUpdate, ConsolidationProgressCoordinator,
    MediaProgressSession, PurgeProgressSession, PurgeProgressSnapshot, PurgeProgressUpdate,
    VideoProgressSnapshot, VideoProgressUpdate,
};

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
    ConcatScriptGuard, VideoConsolidationOptions, VideoConsolidationResult, VideoProgressTelemetry,
    build_local_concat_ffmpeg_args, build_local_concat_ffmpeg_command,
    build_remote_concat_ffmpeg_args, build_remote_concat_ffmpeg_command, cleanup_staged_video,
    consolidate_video, consolidate_video_local, consolidate_video_remote,
    create_temp_concat_script, create_temp_concat_script_in, create_temp_concat_script_in_sync,
    create_temp_concat_script_sync, escape_concat_path, finalize_staged_video,
    generate_concat_script, rename_local_file_with_retry, resolve_ffmpeg_bin,
    spawn_stderr_telemetry_monitor,
};

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

/// Returns true if a chunk is eligible for post-consolidation purging (.ts or .jsonl, strictly excluding metadata.jsonl).
pub(crate) fn is_purgeable_chunk(chunk: &ConsolidationChunk) -> bool {
    chunk.name != "metadata.jsonl"
        && (chunk.name.ends_with(".ts") || chunk.name.ends_with(".jsonl"))
}

/// Collects chunk names eligible for post-consolidation purging from video and chat manifests.
pub(crate) fn collect_purgeable_chunks(
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
) -> Vec<String> {
    video_chunks
        .iter()
        .chain(chat_chunks.iter())
        .filter(|c| is_purgeable_chunk(c))
        .map(|c| c.name.clone())
        .collect()
}

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
    delete_original_chunks_with_progress(
        target,
        video_chunks,
        chat_chunks,
        DEFAULT_DELETE_CONCURRENCY,
        None,
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
    delete_original_chunks_with_progress(
        target,
        video_chunks,
        chat_chunks,
        DEFAULT_DELETE_CONCURRENCY,
        rclone_bin,
        None,
    )
    .await
}

/// Deletes original chunk files (.ts and .jsonl) concurrently under a bounded worker limit.
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
    delete_original_chunks_with_progress(
        target,
        video_chunks,
        chat_chunks,
        concurrency,
        rclone_bin,
        None,
    )
    .await
}

async fn delete_chunk_task(
    target: TargetLocation,
    name: String,
    bin: Option<String>,
) -> anyhow::Result<()> {
    match target {
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
    }
}

/// Deletes original chunk files (.ts and .jsonl) concurrently under a bounded worker limit,
/// sending progress updates as chunks are successfully deleted.
///
/// Throttles task spawning so that only up to `concurrency` tasks are spawned in-flight
/// in Tokio at any time (bounded worker queue pattern).
pub async fn delete_original_chunks_with_progress(
    target: &TargetLocation,
    video_chunks: &[ConsolidationChunk],
    chat_chunks: &[ConsolidationChunk],
    concurrency: usize,
    rclone_bin: Option<&str>,
    progress_sender: Option<tokio::sync::mpsc::UnboundedSender<PurgeProgressUpdate>>,
) -> anyhow::Result<()> {
    let eligible = collect_purgeable_chunks(video_chunks, chat_chunks);
    if eligible.is_empty() {
        return Ok(());
    }

    let concurrency_limit = concurrency.max(1);
    let mut join_set = tokio::task::JoinSet::new();
    let mut chunk_iter = eligible.into_iter();

    // Seed worker queue with up to concurrency_limit in-flight tasks
    for _ in 0..concurrency_limit {
        if let Some(name) = chunk_iter.next() {
            let target = target.clone();
            let bin = rclone_bin.map(str::to_string);
            join_set.spawn(delete_chunk_task(target, name, bin));
        } else {
            break;
        }
    }

    let mut chunks_deleted = 0usize;
    let mut errors = Vec::new();

    // Consume completed tasks and replenish worker queue until all chunks processed
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {
                chunks_deleted += 1;
                if let Some(ref tx) = progress_sender {
                    let _ = tx.send(PurgeProgressUpdate { chunks_deleted });
                }
            }
            Ok(Err(err)) => {
                errors.push(err);
            }
            Err(join_err) => {
                errors.push(anyhow::anyhow!(
                    "Purge task panicked or aborted: {join_err}"
                ));
            }
        }

        // Replenish worker queue
        if let Some(name) = chunk_iter.next() {
            let target = target.clone();
            let bin = rclone_bin.map(str::to_string);
            join_set.spawn(delete_chunk_task(target, name, bin));
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

/// Runs the post-recording consolidation workflow with the default progress coordinator.
pub async fn run_consolidation(args: ConsolidateArgs) -> anyhow::Result<ConsolidationSummary> {
    run_consolidation_with_coordinator(args, ConsolidationProgressCoordinator::default()).await
}

/// Runs the post-recording consolidation workflow with a custom progress coordinator.
///
/// Discovers and validates the manifest, concurrently executes streaming video remuxing
/// and chat deduplication pipelines via `tokio::join!`, atomically finalizes staged `.part`
/// files upon zero-exit completion of all active pipelines, and purges original chunks
/// when `--keep-original` is false while strictly preserving `metadata.jsonl`.
pub async fn run_consolidation_with_coordinator(
    args: ConsolidateArgs,
    coordinator: ConsolidationProgressCoordinator,
) -> anyhow::Result<ConsolidationSummary> {
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

    if has_video && !has_chat {
        eprintln!(
            "[INFO] No chat chunks found; skipping chat consolidation pipeline (video-only session)"
        );
    } else if !has_video && has_chat {
        eprintln!(
            "[INFO] No video chunks found; skipping video consolidation pipeline (chat-only session)"
        );
    }

    let total_video_bytes: u64 = manifest.video_chunks.iter().map(|c| c.size).sum();
    let mut media_session = coordinator.start_media_with_bytes(
        manifest.video_chunks.len(),
        manifest.chat_chunks.len(),
        total_video_bytes,
    );
    let mut video_options = VideoConsolidationOptions::default();
    if let Some(v_tx) = media_session.video_sender() {
        video_options = video_options.with_progress_sender(v_tx);
    }
    let chat_progress_sender = media_session.chat_sender();

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (video_result, chat_stats) = if has_video && has_chat {
        let video_fut = consolidate_video(
            &manifest.target,
            &manifest.video_chunks,
            &video_options,
            cancel_token.clone(),
        );
        let chat_fut = chat::consolidate_chat_with_progress(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            None,
            cancel_token.clone(),
            chat_progress_sender,
        );

        let (video_res, chat_res) = tokio::join!(video_fut, chat_fut);
        match (video_res, chat_res) {
            (Ok(v_res), Ok(c_res)) => {
                media_session.finish(true).await;
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
                media_session.finish(false).await;
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(v_err);
            }
            (Ok(_), Err(c_err)) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ = cleanup_staged_video(&manifest.target, None).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(c_err);
            }
            (Err(v_err), Err(c_err)) => {
                cancel_token.cancel();
                media_session.finish(false).await;
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
        let v_res = match consolidate_video(
            &manifest.target,
            &manifest.video_chunks,
            &video_options,
            cancel_token.clone(),
        )
        .await
        {
            Ok(res) => {
                media_session.finish(true).await;
                if let Err(e) = finalize_staged_video(&manifest.target, None).await {
                    let _ = cleanup_staged_video(&manifest.target, None).await;
                    return Err(e);
                }
                res
            }
            Err(err) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ = cleanup_staged_video(&manifest.target, None).await;
                return Err(err);
            }
        };
        (Some(v_res), None)
    } else if has_chat {
        let c_res = match chat::consolidate_chat_with_progress(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            None,
            cancel_token.clone(),
            chat_progress_sender,
        )
        .await
        {
            Ok(res) => {
                media_session.finish(true).await;
                if let Err(e) = finalize_staged_chat(&manifest.target, None).await {
                    let _ = cleanup_staged_chat(&manifest.target, None).await;
                    return Err(e);
                }
                res
            }
            Err(err) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ = cleanup_staged_chat(&manifest.target, None).await;
                return Err(err);
            }
        };
        (None, Some(c_res))
    } else {
        media_session.finish(true).await;
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
        let eligible_chunks =
            collect_purgeable_chunks(&manifest.video_chunks, &manifest.chat_chunks);
        let total_purge = eligible_chunks.len();

        let mut purge_session = coordinator.start_purge(total_purge);
        let purge_sender = purge_session.purge_sender();

        let purge_res = delete_original_chunks_with_progress(
            &manifest.target,
            &manifest.video_chunks,
            &manifest.chat_chunks,
            args.delete_concurrency,
            None,
            purge_sender,
        )
        .await;

        match purge_res {
            Ok(()) => {
                purge_session.finish(true).await;
                println!("[INFO] Original chunks cleaned up successfully");
            }
            Err(err) => {
                purge_session.finish(false).await;
                return Err(err);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_purgeable_chunk_filters_metadata_and_non_chunks() {
        let meta = ConsolidationChunk {
            name: "metadata.jsonl".to_string(),
            index: 0,
            size: 100,
        };
        assert!(!is_purgeable_chunk(&meta));

        let vid = ConsolidationChunk {
            name: "chunk_0000.ts".to_string(),
            index: 0,
            size: 500,
        };
        assert!(is_purgeable_chunk(&vid));

        let chat = ConsolidationChunk {
            name: "chat_0000.jsonl".to_string(),
            index: 0,
            size: 200,
        };
        assert!(is_purgeable_chunk(&chat));

        let other = ConsolidationChunk {
            name: "readme.txt".to_string(),
            index: 0,
            size: 10,
        };
        assert!(!is_purgeable_chunk(&other));
    }

    #[test]
    fn test_collect_purgeable_chunks() {
        let v_chunks = vec![
            ConsolidationChunk {
                name: "chunk_0000.ts".to_string(),
                index: 0,
                size: 100,
            },
            ConsolidationChunk {
                name: "metadata.jsonl".to_string(),
                index: 1,
                size: 50,
            },
        ];
        let c_chunks = vec![
            ConsolidationChunk {
                name: "chat_0000.jsonl".to_string(),
                index: 0,
                size: 200,
            },
            ConsolidationChunk {
                name: "notes.log".to_string(),
                index: 1,
                size: 20,
            },
        ];

        let eligible = collect_purgeable_chunks(&v_chunks, &c_chunks);
        assert_eq!(eligible, vec!["chunk_0000.ts", "chat_0000.jsonl"]);
    }
}
