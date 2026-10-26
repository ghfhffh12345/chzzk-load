pub mod aria2c;
pub mod chat;
pub mod manifest;
pub mod metadata;
pub mod naming;
pub mod remux;
pub mod target;

use anyhow::{Context, Result, anyhow, bail};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::app_path::resolve_path;
use crate::chzzk::source::VodSource;
pub use crate::cli::VodArgs;
use crate::config::Settings;
use crate::consolidation::staging::remove_dir_all_with_retry;

pub use aria2c::{
    download_segments_with_aria2c, ensure_aria2c_available, generate_aria2c_input_file,
    resolve_aria2c_bin,
};
pub use chat::{
    VodChatArchivalSummary, VodChatPaginationOptions, VodChatStagedSummary, VodChatTarget,
    normalize_vod_chat_message, paginate_vod_chat, paginate_vod_chat_to_staged,
    probe_last_chat_player_time, promote_vod_chat,
};
pub use manifest::{
    HlsVariant, VodSegment, parse_master_playlist, parse_variant_playlist, select_variant,
};
pub use metadata::{
    VodMetadataEvent, VodMetadataEventType, VodMetadataState, VodMetadataTarget,
    format_vod_metadata_jsonl, promote_vod_metadata, synthesize_vod_metadata_events,
    synthesize_vod_metadata_to_staged,
};
pub use naming::{
    format_broadcast_anchor_timestamp, parse_broadcast_anchor_timestamp_ms, resolve_vod_folder_name,
};
pub use remux::{
    build_vod_concat_args, generate_vod_concat_script, remux_vod_to_staged, resolve_ffmpeg_bin,
};
pub use target::parse_video_target;

/// Summary metrics returned upon successful VOD ingestion and archival.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodArchivalSummary {
    pub video_no: u64,
    pub title: String,
    pub streamer: String,
    pub output_dir: PathBuf,
    pub video_path: PathBuf,
    pub chat_path: Option<PathBuf>,
    pub metadata_path: PathBuf,
    pub segments_downloaded: usize,
    pub chat_messages: usize,
}

/// Optional execution configuration for VOD archival sessions.
#[derive(Debug, Clone, Default)]
pub struct VodOptions {
    pub aria2c_bin: Option<String>,
    pub ffmpeg_bin: Option<String>,
    pub chat_options: Option<VodChatPaginationOptions>,
}

/// Cleans up staged files and the temporary staging workspace during abort or error recovery.
async fn cleanup_vod_staging_artifacts(
    staging_dir: &std::path::Path,
    staged_files: &[&std::path::Path],
) {
    for file in staged_files {
        let _ = crate::uploader::unlink_local_file_with_retry(file).await;
    }
    let _ = remove_dir_all_with_retry(staging_dir).await;
}

/// Coordinates the end-to-end VOD ingestion pipeline:
/// 1. Resolves video target to numeric ID
/// 2. Fetches video metadata and NeonPlayer playback manifests via `VodSource`
/// 3. Selects HLS variant matching requested quality and extracts segment list
/// 4. Verifies `aria2c` availability and downloads segments into `.vod_staging_<video_id>/`
/// 5. Remuxes segments using FFmpeg concat demuxer into seekable `vod.mp4.part`
/// 6. Validates structural MP4 container integrity and promotes to `vod.mp4`
/// 7. Cleans up temporary staging workspace
pub async fn run_vod_archival(
    args: VodArgs,
    source: Arc<dyn VodSource>,
    cancel_token: CancellationToken,
) -> Result<VodArchivalSummary> {
    run_vod_archival_with_options(args, source, VodOptions::default(), cancel_token).await
}

/// Coordinates VOD archival with explicit binary overrides.
pub async fn run_vod_archival_with_options(
    args: VodArgs,
    source: Arc<dyn VodSource>,
    options: VodOptions,
    cancel_token: CancellationToken,
) -> Result<VodArchivalSummary> {
    let video_no = parse_video_target(&args.target)
        .with_context(|| format!("Invalid VOD target: '{}'", args.target))?;

    // Resolve configuration
    let settings = Settings::resolve_for_consolidation(args.config.as_deref())
        .context("Failed to resolve configuration for VOD download")?;

    // 1. Resolve and verify external binaries
    let aria2c_bin = resolve_aria2c_bin(
        options.aria2c_bin.as_deref(),
        Some(&settings.aria2c.aria2c_bin),
    );
    ensure_aria2c_available(&aria2c_bin).await?;

    let ffmpeg_bin = resolve_ffmpeg_bin(options.ffmpeg_bin.as_deref());

    if cancel_token.is_cancelled() {
        bail!("VOD archival cancelled before fetching metadata");
    }

    // 2. Query VOD detail via VodSource interface seam
    let detail = source
        .get_video_detail(video_no)
        .await
        .with_context(|| format!("Failed to retrieve video details for video #{video_no}"))?;

    let in_key = detail.in_key.as_deref().unwrap_or("");

    // 3. Query NeonPlayer playback info
    let playback_info = source
        .get_playback_info(&detail.video_id, in_key)
        .await
        .with_context(|| {
            format!(
                "Failed to retrieve playback info for video ID '{}'",
                detail.video_id
            )
        })?;

    let master_url = playback_info
        .find_hls_master_url()
        .ok_or_else(|| anyhow!("No HLS media descriptor found in playback response"))?;

    // 4. Fetch and parse HLS playlists
    let playlist_content = source
        .fetch_hls_playlist(master_url)
        .await
        .with_context(|| format!("Failed to fetch HLS master playlist from '{master_url}'"))?;

    let segments = if playlist_content.contains("#EXT-X-STREAM-INF:") {
        let variants = parse_master_playlist(&playlist_content, master_url)
            .context("Failed to parse master HLS playlist variants")?;
        let selected = select_variant(&variants, &args.quality)
            .with_context(|| format!("Failed to resolve requested quality '{}'", args.quality))?;
        let variant_content = source
            .fetch_hls_playlist(&selected.url)
            .await
            .with_context(|| format!("Failed to fetch variant playlist from '{}'", selected.url))?;
        parse_variant_playlist(&variant_content, &selected.url)
            .context("Failed to parse variant playlist segments")?
    } else {
        // Direct variant playlist
        parse_variant_playlist(&playlist_content, master_url)
            .context("Failed to parse direct variant playlist segments")?
    };

    if segments.is_empty() {
        bail!("Discovered 0 video segments for VOD #{video_no}");
    }

    // 5. Resolve canonical output directory
    let base_recordings_dir = args
        .output_dir
        .unwrap_or_else(|| PathBuf::from(&settings.general.recordings_dir));
    let base_dir = resolve_path(&base_recordings_dir);

    let alias = settings
        .channels
        .iter()
        .find(|c| c.id == detail.channel.channel_id)
        .and_then(|c| c.alias.clone());

    let folder_name = resolve_vod_folder_name(&detail, alias.as_deref());
    let output_dir = base_dir.join(folder_name);

    tokio::fs::create_dir_all(&output_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create output directory '{}'",
                output_dir.display()
            )
        })?;

    // 6. Synthesize staged VOD metadata (vod_metadata.jsonl.part)
    let duration_ms = detail.duration.map(|d| d * 1000).unwrap_or_else(|| {
        (segments.iter().map(|s| s.duration).sum::<f64>() * 1000.0).round() as u64
    });
    let metadata_target = synthesize_vod_metadata_to_staged(&output_dir, &detail, duration_ms)
        .await
        .context("Failed to stage VOD metadata")?;

    // 7. Concurrently paginate VOD chat history if not opted out via --no-chat
    let chat_task = if !args.no_chat {
        let chat_source = Arc::clone(&source);
        let chat_cancel = cancel_token.clone();
        let chat_options = options.chat_options.clone().unwrap_or_default();
        let anchor_time_ms = parse_broadcast_anchor_timestamp_ms(
            detail.live_open_date.as_deref(),
            detail.publish_date.as_deref(),
        );
        let chat_target = VodChatTarget {
            video_no,
            duration_ms,
            anchor_time_ms,
            output_dir: output_dir.clone(),
        };

        Some(tokio::spawn(async move {
            paginate_vod_chat_to_staged(chat_source, chat_target, chat_options, chat_cancel).await
        }))
    } else {
        None
    };

    // 8. Staging directory: .vod_staging_<video_id>/ inside output folder
    let staging_dir = output_dir.join(format!(".vod_staging_{}", detail.video_id));
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create staging directory '{}'",
                staging_dir.display()
            )
        })?;

    // 9. Write aria2c input batch file
    let input_file = staging_dir.join("input_segments.txt");
    let batch_content = generate_aria2c_input_file(&segments);
    tokio::fs::write(&input_file, batch_content.as_bytes())
        .await
        .with_context(|| {
            format!(
                "Failed to write aria2c input file to '{}'",
                input_file.display()
            )
        })?;

    // 10. Download video segments via aria2c
    let download_res =
        download_segments_with_aria2c(&aria2c_bin, &staging_dir, &input_file, cancel_token.clone())
            .await;

    if let Err(err) = download_res {
        if let Some(ref task) = chat_task {
            task.abort();
        }
        cleanup_vod_staging_artifacts(&staging_dir, &[&metadata_target.part_path]).await;
        return Err(err);
    }

    // 11. Remux via FFmpeg Concat Demuxer to staged vod.mp4.part
    let staged_path = match remux_vod_to_staged(
        &ffmpeg_bin,
        &staging_dir,
        &output_dir,
        &segments,
        cancel_token.clone(),
    )
    .await
    {
        Ok(path) => path,
        Err(err) => {
            if let Some(ref task) = chat_task {
                task.abort();
            }
            cleanup_vod_staging_artifacts(&staging_dir, &[&metadata_target.part_path]).await;
            return Err(err);
        }
    };

    // 12. Await concurrent chat pagination completion prior to any file promotion
    let chat_summary = if let Some(task) = chat_task {
        match task.await {
            Ok(Ok(summary)) => Some(summary),
            Ok(Err(err)) => {
                cleanup_vod_staging_artifacts(
                    &staging_dir,
                    &[&staged_path, &metadata_target.part_path],
                )
                .await;
                return Err(err).context("VOD chat pagination failed");
            }
            Err(join_err) => {
                cleanup_vod_staging_artifacts(
                    &staging_dir,
                    &[&staged_path, &metadata_target.part_path],
                )
                .await;
                return Err(anyhow!("VOD chat pagination task panicked: {join_err}"));
            }
        }
    } else {
        None
    };

    // 13. Two-Stage Atomic Promotion at orchestrator seam:
    // Only promote staged files after all concurrent futures succeed with zero-exit status
    let finalized_video_path = output_dir.join("vod.mp4");
    if let Err(err) =
        crate::uploader::rename_local_file_with_retry(&staged_path, &finalized_video_path).await
    {
        let mut to_clean = vec![staged_path.as_path(), metadata_target.part_path.as_path()];
        if let Some(ref chat) = chat_summary {
            to_clean.push(&chat.part_path);
        }
        cleanup_vod_staging_artifacts(&staging_dir, &to_clean).await;
        return Err(err).context("Failed to promote staged VOD video to final destination");
    }

    let (final_chat_path, chat_messages) = if let Some(chat) = chat_summary {
        if chat.part_path != chat.final_path {
            if let Err(err) = promote_vod_chat(&chat.part_path, &chat.final_path).await {
                cleanup_vod_staging_artifacts(
                    &staging_dir,
                    &[
                        &finalized_video_path,
                        &chat.part_path,
                        &metadata_target.part_path,
                    ],
                )
                .await;
                return Err(err).context("Failed to promote staged VOD chat to final destination");
            }
        }
        (Some(chat.final_path), chat.message_count)
    } else {
        (None, 0)
    };

    let finalized_metadata_path = metadata_target.final_path.clone();
    if let Err(err) =
        promote_vod_metadata(&metadata_target.part_path, &finalized_metadata_path).await
    {
        let mut to_clean = vec![
            finalized_video_path.as_path(),
            metadata_target.part_path.as_path(),
        ];
        if let Some(ref chat) = final_chat_path {
            to_clean.push(chat.as_path());
        }
        cleanup_vod_staging_artifacts(&staging_dir, &to_clean).await;
        return Err(err).context("Failed to promote staged VOD metadata to final destination");
    }

    // 14. Automatically purge staging directory upon successful remux & promotion
    let _ = remove_dir_all_with_retry(&staging_dir).await;

    Ok(VodArchivalSummary {
        video_no,
        title: detail.video_title,
        streamer: detail.channel.channel_name,
        output_dir,
        video_path: finalized_video_path,
        chat_path: final_chat_path,
        metadata_path: finalized_metadata_path,
        segments_downloaded: segments.len(),
        chat_messages,
    })
}
