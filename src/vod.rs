pub mod aria2c;
pub mod chat;
pub mod manifest;
pub mod metadata;
pub mod naming;
pub mod progress;
pub mod remux;
pub mod target;
pub mod upload;

use anyhow::{Context, Result, anyhow, bail};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::app_path::resolve_path;
use crate::chzzk::source::VodSource;
pub use crate::cli::VodArgs;
use crate::config::Settings;
use crate::consolidation::staging::remove_dir_all_with_retry;

pub use aria2c::{
    build_aria2c_segment_args, download_segments_with_aria2c,
    download_segments_with_aria2c_and_progress, ensure_aria2c_available, filter_missing_segments,
    generate_aria2c_input_file, resolve_aria2c_bin, resolve_vod_staging_dir,
    scan_completed_segments,
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
pub use progress::{
    VodChatProgressSnapshot, VodChatProgressUpdate, VodMuxProgressSnapshot, VodProgressCoordinator,
    VodProgressSession, VodSpeedTracker, VodVideoProgressSnapshot, VodVideoProgressUpdate,
};
pub use remux::{
    build_vod_concat_args, generate_vod_concat_script, remux_vod_to_staged, resolve_ffmpeg_bin,
};
pub use target::parse_video_target;
pub use upload::{VodArtifactTriad, upload_vod_artifacts, validate_remote_connection};

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
    pub uploaded: bool,
}

/// Optional execution configuration for VOD archival sessions.
#[derive(Debug, Clone, Default)]
pub struct VodOptions {
    pub aria2c_bin: Option<String>,
    pub ffmpeg_bin: Option<String>,
    pub rclone_bin: Option<String>,
    pub rclone_config: Option<crate::config::RcloneConfig>,
    pub temp_dir: Option<PathBuf>,
    pub chat_options: Option<VodChatPaginationOptions>,
    pub coordinator: Option<VodProgressCoordinator>,
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
    let is_tty = !args.headless && std::io::stdout().is_terminal();
    let coordinator = VodProgressCoordinator::new(is_tty);
    let options = VodOptions {
        coordinator: Some(coordinator),
        ..Default::default()
    };
    run_vod_archival_with_options(args, source, options, cancel_token).await
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

    // Resolve rclone backend and run pre-flight remote path validation check if --upload is set
    let rclone_backend = if args.upload {
        let rclone_config = options
            .rclone_config
            .clone()
            .unwrap_or_else(|| settings.rclone.clone());
        let rclone_bin = crate::consolidation::manifest::resolve_rclone_bin_with_config(
            options.rclone_bin.as_deref(),
            Some(&rclone_config.rclone_bin),
        );
        let backend =
            crate::uploader::rclone::RcloneBackend::new(rclone_config).with_bin(rclone_bin);
        validate_remote_connection(&backend).await?;
        Some(backend)
    } else {
        None
    };

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

    let mut segments = if playlist_content.contains("#EXT-X-STREAM-INF:") {
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

    if let Some(max) = args.max_segments {
        segments.truncate(max);
    }

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
    let output_dir = base_dir.join(&folder_name);

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

    // Initialize progress session if coordinator is provided
    let has_chat = !args.no_chat;
    let mut progress_session = options
        .coordinator
        .as_ref()
        .map(|coord| coord.start_session(segments.len(), duration_ms, true, has_chat));

    let video_tx = progress_session.as_ref().and_then(|s| s.video_sender());
    let chat_tx = progress_session.as_ref().and_then(|s| s.chat_sender());

    // 7. Concurrently paginate VOD chat history if not opted out via --no-chat
    let chat_task = if !args.no_chat {
        let chat_source = Arc::clone(&source);
        let chat_cancel = cancel_token.clone();
        let mut chat_options = options.chat_options.clone().unwrap_or_default();
        if chat_options.progress_sender.is_none() {
            chat_options.progress_sender = chat_tx;
        }
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

    // 8. Staging directory: resolve custom temp-dir or default to .vod_staging_<video_id>/ inside output folder
    let temp_dir_override = options.temp_dir.as_ref().or(args.temp_dir.as_ref());
    let staging_dir = resolve_vod_staging_dir(
        &output_dir,
        &detail.video_id,
        temp_dir_override.map(|p| p.as_path()),
    );
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create staging directory '{}'",
                staging_dir.display()
            )
        })?;

    // 9. Filter already downloaded segments and write aria2c input batch file
    let missing_segments = filter_missing_segments(&staging_dir, &segments).await;
    let input_file = staging_dir.join("input_segments.txt");
    let batch_content = generate_aria2c_input_file(&missing_segments);
    tokio::fs::write(&input_file, batch_content.as_bytes())
        .await
        .with_context(|| {
            format!(
                "Failed to write aria2c input file to '{}'",
                input_file.display()
            )
        })?;

    // 10. Download video segments via aria2c with live progress reporting
    let cookie_header = source.cookie_header();
    let download_res = download_segments_with_aria2c_and_progress(
        &aria2c_bin,
        &staging_dir,
        &input_file,
        &segments,
        video_tx,
        cookie_header,
        cancel_token.clone(),
    )
    .await;

    if let Err(err) = download_res {
        if let Some(ref task) = chat_task {
            task.abort();
        }
        if let Some(mut s) = progress_session {
            s.finish(false).await;
        }
        // Retain unfinished .part files and staging folder upon failure for manual resume
        return Err(err);
    }

    // Transition progress coordinator to MUX phase
    if let Some(ref mut s) = progress_session {
        s.transition_to_mux();
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
            if let Some(mut s) = progress_session {
                s.finish(false).await;
            }
            // Retain unfinished .part files and staging folder upon failure for manual resume
            return Err(err);
        }
    };

    // 12. Await concurrent chat pagination completion prior to any file promotion
    let chat_summary = if let Some(task) = chat_task {
        match task.await {
            Ok(Ok(summary)) => Some(summary),
            Ok(Err(err)) => {
                if let Some(mut s) = progress_session {
                    s.finish(false).await;
                }
                // Retain unfinished .part files and staging folder upon failure for manual resume
                return Err(err).context("VOD chat pagination failed");
            }
            Err(join_err) => {
                if let Some(mut s) = progress_session {
                    s.finish(false).await;
                }
                // Retain unfinished .part files and staging folder upon failure for manual resume
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
        if let Some(mut s) = progress_session {
            s.finish(false).await;
        }
        return Err(err).context("Failed to promote staged VOD video to final destination");
    }

    let (final_chat_path, chat_messages) = if let Some(chat) = chat_summary {
        if chat.part_path != chat.final_path {
            if let Err(err) = promote_vod_chat(&chat.part_path, &chat.final_path).await {
                if let Some(mut s) = progress_session {
                    s.finish(false).await;
                }
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
        if let Some(mut s) = progress_session {
            s.finish(false).await;
        }
        return Err(err).context("Failed to promote staged VOD metadata to final destination");
    }

    // 14. Finalize progress session on orchestrator success
    if let Some(mut s) = progress_session {
        s.finish(true).await;
    }

    // 15. Automatically purge staging directory upon successful remux & promotion
    let _ = remove_dir_all_with_retry(&staging_dir).await;

    // 16. Cloud synchronization if --upload requested
    if let Some(ref backend) = rclone_backend {
        let artifacts = VodArtifactTriad {
            video_path: &finalized_video_path,
            chat_path: final_chat_path.as_deref(),
            metadata_path: &finalized_metadata_path,
        };
        upload_vod_artifacts(
            backend,
            &folder_name,
            artifacts,
            args.delete_after_upload,
            cancel_token.clone(),
        )
        .await
        .context("VOD cloud upload failed")?;
    }

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
        uploaded: rclone_backend.is_some(),
    })
}
