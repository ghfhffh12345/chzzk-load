pub mod chat;
pub mod loopback;
pub mod manifest;
pub mod progress;
pub mod staging;
pub mod video;

pub use staging::{
    STAGING_HEADROOM_MULTIPLIER, calculate_required_staging_bytes, check_staging_resume_readiness,
    compute_target_hash, invalidate_staging_files, probe_moov_atom, remove_dir_all_with_retry,
    resolve_staging_dir, staging_folder_name, validate_staging_workspace, verify_staging_capacity,
    verify_staging_capacity_with_space,
};

pub use loopback::{
    DEFAULT_LOOPBACK_STARTUP_TIMEOUT, EphemeralLoopbackServer, parse_loopback_port,
};

pub use progress::{
    ChatProgressSnapshot, ChatProgressUpdate, ConsolidationProgressCoordinator, CoordinatorOutput,
    MediaProgressSession, PurgeProgressSession, PurgeProgressSnapshot, PurgeProgressUpdate,
    UploadProgressSession, UploadProgressSnapshot, UploadProgressUpdate, UploadTrack,
    VideoProgressSnapshot, VideoProgressUpdate,
};

pub use chat::{
    ChatConsolidationOptions, ChatConsolidationStats, ChatDeduplicator, ChatMessageKey,
    DEFAULT_CHAT_DEDUP_WINDOW_MS, cleanup_staged_chat, cleanup_staged_chat_in, consolidate_chat,
    consolidate_chat_local, consolidate_chat_remote, consolidate_chat_with_options,
    delete_remote_file, delete_remote_file_checked, finalize_staged_chat,
};
pub use manifest::{
    ChunkGap, ConsolidationChunk, ConsolidationManifest, RawManifestEntry, TargetLocation,
    build_manifest_from_entries, detect_index_gaps, discover_manifest, discover_manifest_with_bin,
    is_remote_path, join_remote_path, parse_chat_chunk_index, parse_video_chunk_index,
    resolve_rclone_bin, resolve_rclone_bin_with_config,
};
pub use video::{
    ConcatScriptGuard, VideoConsolidationOptions, VideoConsolidationResult, VideoProgressTelemetry,
    build_local_concat_ffmpeg_args, build_local_concat_ffmpeg_command,
    build_remote_concat_ffmpeg_args, build_remote_concat_ffmpeg_command, cleanup_staged_video,
    cleanup_staged_video_in, consolidate_video, consolidate_video_local, consolidate_video_remote,
    create_temp_concat_script, create_temp_concat_script_in, create_temp_concat_script_in_sync,
    create_temp_concat_script_sync, escape_concat_path, finalize_staged_video,
    generate_concat_script, rename_local_file_with_retry, resolve_ffmpeg_bin,
    spawn_stderr_telemetry_monitor,
};

/// Re-exported CLI argument model for consolidation operations. Defined in [`crate::cli::ConsolidateArgs`].
pub use crate::cli::ConsolidateArgs;
use crate::uploader::{parse_rclone_log_line, unlink_local_file_with_retry};
use anyhow::Context;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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

/// Returns true if a chunk is eligible for post-consolidation purging (.ts or .jsonl, strictly excluding live_metadata.jsonl and livemetadata.jsonl).
pub(crate) fn is_purgeable_chunk(chunk: &ConsolidationChunk) -> bool {
    chunk.name != "live_metadata.jsonl"
        && chunk.name != "livemetadata.jsonl"
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
/// Invariant: `live_metadata.jsonl` and `livemetadata.jsonl` are NEVER deleted.
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
/// strictly excluding `live_metadata.jsonl` and `livemetadata.jsonl`.
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

/// Default rclone flags applied to consolidation remote uploads for large-file throughput optimization.
pub const HIGH_THROUGHPUT_UPLOAD_ARGS: &[&str] = &[
    "--drive-chunk-size",
    "128M",
    "--s3-chunk-size",
    "64M",
    "--s3-upload-concurrency",
    "4",
    "--buffer-size",
    "64M",
    "--multi-thread-streams",
    "4",
    "--multi-thread-cutoff",
    "250M",
];

/// Builds command arguments for high-throughput consolidation staged file upload,
/// appending any user-defined `consolidate_extra_args`.
pub fn build_consolidate_upload_args(extra_args: &[String]) -> Vec<String> {
    let mut args: Vec<String> = HIGH_THROUGHPUT_UPLOAD_ARGS
        .iter()
        .map(|s| s.to_string())
        .collect();
    for extra in extra_args {
        args.push(extra.clone());
    }
    args
}

/// Builds a `tokio::process::Command` configured for high-throughput `rclone copyto`.
pub fn build_remote_staged_upload_command(
    bin: &str,
    local_path: &std::path::Path,
    remote_dest: &str,
    extra_args: &[String],
) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("copyto")
        .arg(local_path)
        .arg(remote_dest)
        .arg("--use-json-log")
        .arg("--stats")
        .arg("250ms")
        .arg("--stats-log-level")
        .arg("NOTICE");
    for arg in build_consolidate_upload_args(extra_args) {
        cmd.arg(arg);
    }
    cmd
}

/// Uploads a local file to a remote destination via `rclone copyto` using default
/// high-throughput transfer options and optional user-specified extra arguments,
/// dispatching live progress updates through `progress_tx`.
pub async fn upload_remote_staged_file_with_progress(
    bin: &str,
    local_path: &std::path::Path,
    remote_dest: &str,
    extra_args: &[String],
    progress_tx: Option<mpsc::UnboundedSender<UploadProgressUpdate>>,
) -> anyhow::Result<()> {
    let file_size = tokio::fs::metadata(local_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    let mut cmd = build_remote_staged_upload_command(bin, local_path, remote_dest, extra_args);
    let mut child = cmd.spawn().with_context(|| {
        format!(
            "Failed to execute '{bin} copyto {} {remote_dest}'",
            local_path.display()
        )
    })?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture stderr from rclone process"))?;

    let mut reader = tokio::io::BufReader::new(stderr).lines();
    let mut stderr_logs = Vec::new();
    let mut last_speed = 0.0;

    while let Ok(Some(line)) = reader.next_line().await {
        if let Some(stats) = parse_rclone_log_line(&line) {
            let total = if stats.total_bytes > 0 {
                stats.total_bytes
            } else {
                file_size
            };
            let speed_mb_s = stats.speed / 1_048_576.0;
            last_speed = speed_mb_s;
            if let Some(ref tx) = progress_tx {
                let _ = tx.send(UploadProgressUpdate {
                    bytes_uploaded: stats.bytes,
                    total_bytes: total,
                    speed_mb_s,
                });
            }
        } else {
            if stderr_logs.len() >= 30 {
                stderr_logs.remove(0);
            }
            stderr_logs.push(line);
        }
    }

    let status = child
        .wait()
        .await
        .with_context(|| "failed to wait for rclone copyto process")?;

    if status.success() {
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(UploadProgressUpdate {
                bytes_uploaded: file_size,
                total_bytes: file_size,
                speed_mb_s: last_speed,
            });
        }
        Ok(())
    } else {
        let err_msg = stderr_logs.join("\n");
        let trimmed = err_msg.trim();
        if trimmed.is_empty() {
            anyhow::bail!(
                "rclone copyto for '{}' failed with status {status}",
                local_path.display()
            );
        } else {
            anyhow::bail!(
                "rclone copyto for '{}' failed with status {status}: {trimmed}",
                local_path.display()
            );
        }
    }
}

/// Uploads a local file to a remote destination via `rclone copyto` using default
/// high-throughput transfer options and optional user-specified extra arguments.
pub async fn upload_remote_staged_file_with_args(
    bin: &str,
    local_path: &std::path::Path,
    remote_dest: &str,
    extra_args: &[String],
) -> anyhow::Result<()> {
    upload_remote_staged_file_with_progress(bin, local_path, remote_dest, extra_args, None).await
}

/// Uploads a local file to a remote destination via `rclone copyto` using default
/// high-throughput transfer options.
pub async fn upload_remote_staged_file(
    bin: &str,
    local_path: &std::path::Path,
    remote_dest: &str,
) -> anyhow::Result<()> {
    upload_remote_staged_file_with_args(bin, local_path, remote_dest, &[]).await
}

/// Concurrently uploads staged media files (`live_stream.mp4` and `live_chat.jsonl`) to remote destinations
/// (`remote_base/live_stream.mp4.part` and `remote_base/live_chat.jsonl.part`) via `rclone copyto`,
/// dispatching live progress telemetry to optional senders.
///
/// Implements fail-fast sibling abort: if either upload fails, the sibling upload process
/// is promptly aborted and killed (`kill_on_drop`).
///
/// Listens to cooperative cancellation via `cancel_token`: if cancelled, active upload
/// processes are immediately terminated.
///
/// On upload failure or cancellation, remote `.part` files are deleted via `rclone deletefile`
/// while finalized local staging files are strictly preserved.
#[allow(clippy::too_many_arguments)]
pub async fn upload_remote_staged_media_with_progress(
    bin: &str,
    remote_base: &str,
    staging_dir: &std::path::Path,
    has_video: bool,
    has_chat: bool,
    extra_args: &[String],
    cancel_token: CancellationToken,
    video_tx: Option<mpsc::UnboundedSender<UploadProgressUpdate>>,
    chat_tx: Option<mpsc::UnboundedSender<UploadProgressUpdate>>,
) -> anyhow::Result<()> {
    if !has_video && !has_chat {
        return Ok(());
    }

    let remote_video_part = join_remote_path(remote_base, "live_stream.mp4.part");
    let remote_chat_part = join_remote_path(remote_base, "live_chat.jsonl.part");

    let video_upload_fut = async {
        if has_video {
            let local_mp4 = staging_dir.join("live_stream.mp4");
            upload_remote_staged_file_with_progress(
                bin,
                &local_mp4,
                &remote_video_part,
                extra_args,
                video_tx,
            )
            .await
        } else {
            Ok(())
        }
    };

    let chat_upload_fut = async {
        if has_chat {
            let local_jsonl = staging_dir.join("live_chat.jsonl");
            upload_remote_staged_file_with_progress(
                bin,
                &local_jsonl,
                &remote_chat_part,
                extra_args,
                chat_tx,
            )
            .await
        } else {
            Ok(())
        }
    };

    let upload_res = tokio::select! {
        _ = cancel_token.cancelled() => {
            Err(anyhow::anyhow!("Cloud upload cancelled by cooperative cancellation token"))
        }
        res = async {
            tokio::try_join!(video_upload_fut, chat_upload_fut)
        } => {
            res.map(|_| ())
        }
    };

    if let Err(upload_err) = upload_res {
        // Remote partial cleanup: delete any incomplete remote .part files via rclone deletefile
        if has_video {
            delete_remote_file(bin, &remote_video_part).await;
        }
        if has_chat {
            delete_remote_file(bin, &remote_chat_part).await;
        }
        return Err(upload_err);
    }

    Ok(())
}

/// Concurrently uploads staged media files (`live_stream.mp4` and `live_chat.jsonl`) to remote destinations
/// (`remote_base/live_stream.mp4.part` and `remote_base/live_chat.jsonl.part`) via `rclone copyto`.
///
/// Implements fail-fast sibling abort: if either upload fails, the sibling upload process
/// is promptly aborted and killed (`kill_on_drop`).
///
/// Listens to cooperative cancellation via `cancel_token`: if cancelled, active upload
/// processes are immediately terminated.
///
/// On upload failure or cancellation, remote `.part` files are deleted via `rclone deletefile`
/// while finalized local staging files are strictly preserved.
pub async fn upload_remote_staged_media(
    bin: &str,
    remote_base: &str,
    staging_dir: &std::path::Path,
    has_video: bool,
    has_chat: bool,
    extra_args: &[String],
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    upload_remote_staged_media_with_progress(
        bin,
        remote_base,
        staging_dir,
        has_video,
        has_chat,
        extra_args,
        cancel_token,
        None,
        None,
    )
    .await
}

/// Resolves configuration for consolidation.
///
/// Priority:
/// 1. Explicit path in `config_path` (e.g., from `--config <path>`)
/// 2. Auto-detected `settings.toml` in current directory or executable directory
/// 3. If neither exists, returns default settings (with empty `consolidate_extra_args` and default `rclone_bin`)
pub fn resolve_consolidation_settings(
    config_path: Option<&std::path::Path>,
) -> anyhow::Result<crate::config::Settings> {
    crate::config::Settings::resolve_for_consolidation(config_path)
}

/// Runs the post-recording consolidation workflow with the default progress coordinator.
pub async fn run_consolidation(args: ConsolidateArgs) -> anyhow::Result<ConsolidationSummary> {
    run_consolidation_with_coordinator(args, ConsolidationProgressCoordinator::default()).await
}

/// Runs the post-recording consolidation workflow with an external cancellation token.
pub async fn run_consolidation_with_cancel(
    args: ConsolidateArgs,
    cancel_token: CancellationToken,
) -> anyhow::Result<ConsolidationSummary> {
    run_consolidation_with_coordinator_and_cancel(
        args,
        ConsolidationProgressCoordinator::default(),
        cancel_token,
    )
    .await
}

/// Runs the post-recording consolidation workflow with a custom progress coordinator,
/// automatically wiring Ctrl+C signal handling to cooperative cancellation.
pub async fn run_consolidation_with_coordinator(
    args: ConsolidateArgs,
    coordinator: ConsolidationProgressCoordinator,
) -> anyhow::Result<ConsolidationSummary> {
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let ctrl_c_handle = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_token_clone.cancel();
        }
    });
    let result =
        run_consolidation_with_coordinator_and_cancel(args, coordinator, cancel_token).await;
    ctrl_c_handle.abort();
    result
}

/// Runs the post-recording consolidation workflow with a custom progress coordinator and cancellation token.
///
/// Discovers and validates the manifest, concurrently executes streaming video remuxing
/// and chat deduplication pipelines via `tokio::join!`, atomically finalizes staged `.part`
/// files upon zero-exit completion of all active pipelines, concurrently uploads finalized
/// media files to cloud remotes with fail-fast sibling abort, and purges original chunks
/// when `--keep-original` is false while strictly preserving `live_metadata.jsonl` and `livemetadata.jsonl`.
pub async fn run_consolidation_with_coordinator_and_cancel(
    args: ConsolidateArgs,
    coordinator: ConsolidationProgressCoordinator,
    cancel_token: CancellationToken,
) -> anyhow::Result<ConsolidationSummary> {
    let settings = resolve_consolidation_settings(args.config.as_deref())?;
    let rclone_bin = resolve_rclone_bin_with_config(None, Some(&settings.rclone.rclone_bin));

    let target = TargetLocation::parse(&args.path);
    let manifest =
        discover_manifest_with_bin(&target, args.strict, args.overwrite, Some(&rclone_bin)).await?;

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

    // Pre-flight disk capacity verification and resume sanity check for remote recordings
    let (staging_dir, can_resume) = if manifest.target.is_remote() {
        let dir = resolve_staging_dir(&manifest.target, args.temp_dir.as_deref());
        if args.overwrite {
            // Passing --overwrite invalidates existing local staging files and forces a full clean remux
            let _ = invalidate_staging_files(&dir).await;
            verify_staging_capacity(&dir, total_video_bytes)?;
            (Some(dir), false)
        } else if check_staging_resume_readiness(&dir, has_video, has_chat) {
            println!(
                "[INFO] Detected valid existing consolidated media in staging directory '{}' (moov atom verified). Resuming cloud upload directly.",
                dir.display()
            );
            (Some(dir), true)
        } else {
            // Existing staging files are missing or incomplete; invalidate any stale fragments and verify capacity
            let _ = invalidate_staging_files(&dir).await;
            verify_staging_capacity(&dir, total_video_bytes)?;
            (Some(dir), false)
        }
    } else {
        (None, false)
    };

    let mut media_session = coordinator.start_media_with_bytes(
        manifest.video_chunks.len(),
        manifest.chat_chunks.len(),
        total_video_bytes,
    );
    let mut video_options = VideoConsolidationOptions::default().with_rclone_bin(&rclone_bin);
    if let Some(v_tx) = media_session.video_sender() {
        video_options = video_options.with_progress_sender(v_tx);
    }
    if let Some(ref dir) = staging_dir {
        video_options = video_options.with_staging_dir(dir.clone());
    }

    let mut chat_options = ChatConsolidationOptions::default().with_rclone_bin(&rclone_bin);
    if let Some(c_tx) = media_session.chat_sender() {
        chat_options = chat_options.with_progress_sender(c_tx);
    }
    if let Some(ref dir) = staging_dir {
        chat_options = chat_options.with_staging_dir(dir.clone());
    }

    let (video_result, chat_stats) = if can_resume {
        let staging_dir = staging_dir
            .as_ref()
            .expect("staging_dir must be resolved for remote resume");

        let resumed_video_result = if has_video {
            let mp4_path = staging_dir.join("live_stream.mp4");
            let bytes_written = std::fs::metadata(&mp4_path).map(|m| m.len()).unwrap_or(0);
            if let Some(v_tx) = media_session.video_sender() {
                let _ = v_tx.send(VideoProgressUpdate::ChunkFed {
                    chunks_fed: manifest.video_chunks.len(),
                    bytes_fed: bytes_written,
                });
            }
            Some(VideoConsolidationResult {
                chunks_processed: manifest.video_chunks.len(),
                bytes_written,
            })
        } else {
            None
        };

        let resumed_chat_stats = if has_chat {
            if let Some(c_tx) = media_session.chat_sender() {
                let _ = c_tx.send(ChatProgressUpdate {
                    chunks_read: manifest.chat_chunks.len(),
                    total_messages: 0,
                    deduplicated_messages: 0,
                    emitted_messages: 0,
                });
            }
            Some(ChatConsolidationStats {
                total_messages: 0,
                deduplicated_messages: 0,
                malformed_messages: 0,
                emitted_messages: 0,
            })
        } else {
            None
        };

        media_session.finish(true).await;
        (resumed_video_result, resumed_chat_stats)
    } else if has_video && has_chat {
        let video_fut = consolidate_video(
            &manifest.target,
            &manifest.video_chunks,
            &video_options,
            cancel_token.clone(),
        );
        let chat_fut = chat::consolidate_chat_with_options(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            &chat_options,
            cancel_token.clone(),
        );

        let (video_res, chat_res) = tokio::join!(video_fut, chat_fut);
        match (video_res, chat_res) {
            (Ok(v_res), Ok(c_res)) => {
                media_session.finish(true).await;
                (Some(v_res), Some(c_res))
            }
            (Err(v_err), Ok(_)) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ =
                    cleanup_staged_video_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                let _ =
                    cleanup_staged_chat_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                if let Some(ref dir) = staging_dir {
                    let _ = invalidate_staging_files(dir).await;
                }
                return Err(v_err);
            }
            (Ok(_), Err(c_err)) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ =
                    cleanup_staged_video_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                let _ =
                    cleanup_staged_chat_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                if let Some(ref dir) = staging_dir {
                    let _ = invalidate_staging_files(dir).await;
                }
                return Err(c_err);
            }
            (Err(v_err), Err(c_err)) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ =
                    cleanup_staged_video_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                let _ =
                    cleanup_staged_chat_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                if let Some(ref dir) = staging_dir {
                    let _ = invalidate_staging_files(dir).await;
                }

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
                res
            }
            Err(err) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ =
                    cleanup_staged_video_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                if let Some(ref dir) = staging_dir {
                    let _ = invalidate_staging_files(dir).await;
                }
                return Err(err);
            }
        };
        (Some(v_res), None)
    } else if has_chat {
        let c_res = match chat::consolidate_chat_with_options(
            &manifest.target,
            &manifest.chat_chunks,
            args.strict,
            &chat_options,
            cancel_token.clone(),
        )
        .await
        {
            Ok(res) => {
                media_session.finish(true).await;
                res
            }
            Err(err) => {
                cancel_token.cancel();
                media_session.finish(false).await;
                let _ =
                    cleanup_staged_chat_in(&manifest.target, args.temp_dir.as_deref(), None).await;
                if let Some(ref dir) = staging_dir {
                    let _ = invalidate_staging_files(dir).await;
                }
                return Err(err);
            }
        };
        (None, Some(c_res))
    } else {
        media_session.finish(true).await;
        (None, None)
    };

    // Upload & Finalization Stage
    if manifest.target.is_remote() {
        let remote_base = manifest.target.raw();
        let staging_dir = staging_dir
            .as_ref()
            .expect("staging_dir must be resolved for remote consolidation");

        // Validate staging workspace prior to cloud upload
        validate_staging_workspace(staging_dir, has_video, has_chat)?;

        // 1. Two-stage cloud upload: upload local files to remote .part destinations concurrently via rclone copyto
        let video_bytes = if has_video {
            let mp4_path = staging_dir.join("live_stream.mp4");
            tokio::fs::metadata(mp4_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0)
        } else {
            0
        };
        let chat_bytes = if has_chat {
            let chat_path = staging_dir.join("live_chat.jsonl");
            tokio::fs::metadata(chat_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0)
        } else {
            0
        };

        let mut upload_session = coordinator.start_upload_tracks(
            has_video.then_some(video_bytes),
            has_chat.then_some(chat_bytes),
        );

        let upload_res = upload_remote_staged_media_with_progress(
            &rclone_bin,
            remote_base,
            staging_dir,
            has_video,
            has_chat,
            &settings.rclone.consolidate_extra_args,
            cancel_token.clone(),
            upload_session.video_sender(),
            upload_session.chat_sender(),
        )
        .await;

        upload_session.finish(upload_res.is_ok()).await;

        if let Err(upload_err) = upload_res {
            eprintln!(
                "[INFO] Cloud upload failed or was interrupted, but finalized local staging files were preserved in '{}'. Re-run consolidation to resume cloud upload without re-running FFmpeg.",
                staging_dir.display()
            );
            return Err(upload_err);
        }

        // 2. Atomically rename remote .part files via rclone moveto
        let rename_res = async {
            if has_video {
                finalize_staged_video(&manifest.target, Some(&rclone_bin)).await?;
            }
            if has_chat {
                finalize_staged_chat(&manifest.target, Some(&rclone_bin)).await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(rename_err) = rename_res {
            let _ = cleanup_staged_video_in(
                &manifest.target,
                args.temp_dir.as_deref(),
                Some(&rclone_bin),
            )
            .await;
            let _ = cleanup_staged_chat_in(
                &manifest.target,
                args.temp_dir.as_deref(),
                Some(&rclone_bin),
            )
            .await;
            delete_remote_file(
                &rclone_bin,
                &join_remote_path(remote_base, "live_stream.mp4"),
            )
            .await;
            delete_remote_file(
                &rclone_bin,
                &join_remote_path(remote_base, "live_chat.jsonl"),
            )
            .await;
            eprintln!(
                "[INFO] Cloud finalization failed, but finalized local staging files were preserved in '{}'. Re-run consolidation to resume cloud upload.",
                staging_dir.display()
            );
            return Err(rename_err);
        }
    } else {
        // Local mode: atomically finalize local .part files directly
        let finalize_res = async {
            if has_video {
                finalize_staged_video(&manifest.target, None).await?;
            }
            if has_chat {
                finalize_staged_chat(&manifest.target, None).await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = finalize_res {
            let _ = cleanup_staged_video(&manifest.target, None).await;
            let _ = cleanup_staged_chat(&manifest.target, None).await;
            return Err(e);
        }
    }

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

    let purge_res = if !args.keep_original {
        let eligible_chunks =
            collect_purgeable_chunks(&manifest.video_chunks, &manifest.chat_chunks);
        let total_purge = eligible_chunks.len();

        let mut purge_session = coordinator.start_purge(total_purge);
        let purge_sender = purge_session.purge_sender();

        let res = delete_original_chunks_with_progress(
            &manifest.target,
            &manifest.video_chunks,
            &manifest.chat_chunks,
            args.delete_concurrency,
            Some(&rclone_bin),
            purge_sender,
        )
        .await;

        match res {
            Ok(()) => {
                purge_session.finish(true).await;
                println!("[INFO] Original chunks cleaned up successfully");
                Ok(())
            }
            Err(err) => {
                purge_session.finish(false).await;
                Err(err)
            }
        }
    } else {
        Ok(())
    };

    // Unlink local temporary staging directory upon verified completion with Windows retry resilience
    if let Some(ref dir) = staging_dir {
        if let Err(e) = remove_dir_all_with_retry(dir).await {
            eprintln!(
                "[WARN] Failed to unlink staging directory '{}': {e}",
                dir.display()
            );
        }
    }

    purge_res?;

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
        let meta_canonical = ConsolidationChunk {
            name: "live_metadata.jsonl".to_string(),
            index: 0,
            size: 100,
        };
        assert!(!is_purgeable_chunk(&meta_canonical));

        let meta = ConsolidationChunk {
            name: "livemetadata.jsonl".to_string(),
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
                name: "livemetadata.jsonl".to_string(),
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

    #[test]
    fn test_build_consolidate_upload_args_defaults() {
        let args = build_consolidate_upload_args(&[]);
        assert_eq!(
            args,
            vec![
                "--drive-chunk-size",
                "128M",
                "--s3-chunk-size",
                "64M",
                "--s3-upload-concurrency",
                "4",
                "--buffer-size",
                "64M",
                "--multi-thread-streams",
                "4",
                "--multi-thread-cutoff",
                "250M",
            ]
        );
    }

    #[test]
    fn test_build_consolidate_upload_args_appends_extra() {
        let extra = vec![
            "--drive-chunk-size".to_string(),
            "256M".to_string(),
            "--fast-list".to_string(),
        ];
        let args = build_consolidate_upload_args(&extra);
        assert_eq!(
            args,
            vec![
                "--drive-chunk-size",
                "128M",
                "--s3-chunk-size",
                "64M",
                "--s3-upload-concurrency",
                "4",
                "--buffer-size",
                "64M",
                "--multi-thread-streams",
                "4",
                "--multi-thread-cutoff",
                "250M",
                "--drive-chunk-size",
                "256M",
                "--fast-list",
            ]
        );
    }

    #[test]
    fn test_build_remote_staged_upload_command_structure() {
        let local_path = std::path::Path::new("local/live_stream.mp4");
        let remote_dest = "remote:bucket/live_stream.mp4.part";
        let extra = vec!["--checkers".to_string(), "8".to_string()];
        let cmd = build_remote_staged_upload_command("my_rclone", local_path, remote_dest, &extra);
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|s| s.to_string_lossy().to_string())
            .collect();

        assert_eq!(args[0], "copyto");
        assert_eq!(args[1], local_path.to_string_lossy().to_string());
        assert_eq!(args[2], remote_dest);
        assert_eq!(args[3], "--use-json-log");
        assert_eq!(args[4], "--stats");
        assert_eq!(args[5], "250ms");
        assert_eq!(args[6], "--stats-log-level");
        assert_eq!(args[7], "NOTICE");
        assert_eq!(args[8], "--drive-chunk-size");
        assert_eq!(args[9], "128M");
        assert_eq!(args[10], "--s3-chunk-size");
        assert_eq!(args[11], "64M");
        assert_eq!(args[12], "--s3-upload-concurrency");
        assert_eq!(args[13], "4");
        assert_eq!(args[14], "--buffer-size");
        assert_eq!(args[15], "64M");
        assert_eq!(args[16], "--multi-thread-streams");
        assert_eq!(args[17], "4");
        assert_eq!(args[18], "--multi-thread-cutoff");
        assert_eq!(args[19], "250M");
        assert_eq!(args[20], "--checkers");
        assert_eq!(args[21], "8");
    }

    #[test]
    fn test_resolve_rclone_bin_with_config_precedence() {
        // 1. override_bin takes absolute precedence
        assert_eq!(
            resolve_rclone_bin_with_config(Some("direct_bin"), Some("cfg_bin")),
            "direct_bin"
        );

        // 2. config_bin used when no override or env
        assert_eq!(
            resolve_rclone_bin_with_config(None, Some("cfg_rclone")),
            if let Ok(env_bin) = std::env::var("CHZZK_LOAD_RCLONE_BIN") {
                if !env_bin.trim().is_empty() {
                    env_bin
                } else {
                    "cfg_rclone".to_string()
                }
            } else {
                "cfg_rclone".to_string()
            }
        );

        // 3. Fallback to "rclone" when config_bin is empty/None (and env var absent)
        assert_eq!(
            resolve_rclone_bin_with_config(None, None),
            if let Ok(env_bin) = std::env::var("CHZZK_LOAD_RCLONE_BIN") {
                if !env_bin.trim().is_empty() {
                    env_bin
                } else {
                    "rclone".to_string()
                }
            } else {
                "rclone".to_string()
            }
        );
    }

    #[test]
    fn test_resolve_consolidation_settings_custom_and_fallback() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_resolve_cfg_{}", rand::random::<u32>()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        // 1. Explicit valid custom path
        let custom_file = temp_dir.join("my_settings.toml");
        std::fs::write(
            &custom_file,
            r#"
[rclone]
rclone_bin = "custom_bin"
consolidate_extra_args = ["--low-level-retries", "10"]
"#,
        )
        .unwrap();

        let loaded = resolve_consolidation_settings(Some(&custom_file))
            .expect("Explicit custom path should load");
        assert_eq!(loaded.rclone.rclone_bin, "custom_bin");
        assert_eq!(
            loaded.rclone.consolidate_extra_args,
            vec!["--low-level-retries".to_string(), "10".to_string()]
        );

        // 2. Explicit non-existent path errors
        let non_existent = temp_dir.join("missing.toml");
        let err = resolve_consolidation_settings(Some(&non_existent));
        assert!(err.is_err(), "Non-existent explicit config path must error");

        // Cleanup
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_consolidate_args_reexport() {
        let args = super::ConsolidateArgs {
            path: "remote:bucket/stream".to_string(),
            keep_original: false,
            overwrite: true,
            strict: false,
            delete_concurrency: 16,
            temp_dir: None,
            config: None,
        };
        assert_eq!(args.path, "remote:bucket/stream");
        assert_eq!(args.delete_concurrency, 16);
    }
}
