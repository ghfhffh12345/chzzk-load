use anyhow::{Context, Result, anyhow, bail};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::uploader::backend::UploadBackend;
use crate::uploader::rclone::{RcloneBackend, format_destination};
use crate::uploader::unlink_local_file_with_retry;

/// Verifies remote path accessibility before beginning any long-running downloads.
pub async fn validate_remote_connection(backend: &RcloneBackend) -> Result<()> {
    if backend.config.remote_path.trim().is_empty() {
        bail!("Cannot upload VOD: rclone remote_path is not configured (empty)");
    }
    if backend.config.skip_connection_check {
        return Ok(());
    }
    backend.check_connection().await.with_context(|| {
        format!(
            "Pre-flight rclone remote connection validation check failed for '{}'",
            backend.config.remote_path
        )
    })?;
    Ok(())
}

/// Local file paths representing the canonical VOD Artifact Triad.
#[derive(Debug, Clone)]
pub struct VodArtifactTriad<'a> {
    pub video_path: &'a Path,
    pub chat_path: Option<&'a Path>,
    pub metadata_path: &'a Path,
}

/// Helper function to clean up lingering remote .part files upon upload or promotion failure.
async fn cleanup_remote_parts(
    backend: &RcloneBackend,
    remote_video_part: &str,
    remote_chat_part: Option<&str>,
    remote_meta_part: &str,
) {
    let _ = backend.delete_file(remote_video_part).await;
    if let Some(chat_part) = remote_chat_part {
        let _ = backend.delete_file(chat_part).await;
    }
    let _ = backend.delete_file(remote_meta_part).await;
}

/// Orchestrates the two-stage remote staging upload of the completed VOD Artifact Triad:
/// 1. Transmits local artifacts to remote `.part` files via `rclone copyto` bounded by `upload_concurrency`.
///    Upon failure or cancellation, incomplete remote `.part` files are removed while local
///    artifacts remain strictly preserved.
/// 2. Atomically promotes remote `.part` files to final names via `rclone moveto`, rolling back
///    any partially promoted files if promotion fails.
/// 3. If `delete_after_upload` is true, purges local artifact files with lock retry resilience.
///    Otherwise, preserves local files intact.
pub async fn upload_vod_artifacts(
    backend: &RcloneBackend,
    folder_name: &str,
    artifacts: VodArtifactTriad<'_>,
    delete_after_upload: bool,
    cancel_token: CancellationToken,
) -> Result<()> {
    let remote_base = &backend.config.remote_path;

    let remote_video_part = format_destination(remote_base, folder_name, "vod.mp4.part");
    let remote_video_final = format_destination(remote_base, folder_name, "vod.mp4");

    let remote_chat_part = artifacts
        .chat_path
        .map(|_| format_destination(remote_base, folder_name, "vod_chat.jsonl.part"));
    let remote_chat_final = artifacts
        .chat_path
        .map(|_| format_destination(remote_base, folder_name, "vod_chat.jsonl"));

    let remote_meta_part = format_destination(remote_base, folder_name, "vod_metadata.jsonl.part");
    let remote_meta_final = format_destination(remote_base, folder_name, "vod_metadata.jsonl");

    // Stage 1: Upload files to remote .part destinations respecting configured concurrency
    let concurrency = backend.config.upload_concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));

    let video_sem = Arc::clone(&semaphore);
    let video_fut = async {
        let _permit = video_sem
            .acquire()
            .await
            .map_err(|e| anyhow!("Failed to acquire upload concurrency permit: {e}"))?;
        backend
            .upload_file_to_dest(
                artifacts.video_path,
                &remote_video_part,
                Box::new(|_, _, _| {}),
            )
            .await
            .with_context(|| format!("Failed to upload staged video to '{remote_video_part}'"))
    };

    let chat_sem = Arc::clone(&semaphore);
    let chat_fut = async {
        if let (Some(chat_path), Some(chat_part_dest)) = (artifacts.chat_path, &remote_chat_part) {
            let _permit = chat_sem
                .acquire()
                .await
                .map_err(|e| anyhow!("Failed to acquire upload concurrency permit: {e}"))?;
            backend
                .upload_file_to_dest(chat_path, chat_part_dest, Box::new(|_, _, _| {}))
                .await
                .with_context(|| format!("Failed to upload staged chat to '{chat_part_dest}'"))?;
        }
        Ok::<(), anyhow::Error>(())
    };

    let meta_sem = Arc::clone(&semaphore);
    let meta_fut = async {
        let _permit = meta_sem
            .acquire()
            .await
            .map_err(|e| anyhow!("Failed to acquire upload concurrency permit: {e}"))?;
        backend
            .upload_file_to_dest(
                artifacts.metadata_path,
                &remote_meta_part,
                Box::new(|_, _, _| {}),
            )
            .await
            .with_context(|| format!("Failed to upload staged metadata to '{remote_meta_part}'"))
    };

    let upload_res = tokio::select! {
        _ = cancel_token.cancelled() => {
            Err(anyhow!("VOD cloud upload cancelled by cancellation token"))
        }
        res = async {
            tokio::try_join!(video_fut, chat_fut, meta_fut)
        } => {
            res.map(|_| ())
        }
    };

    if let Err(upload_err) = upload_res {
        // Purge remote partial files upon upload failure; local files remain preserved
        cleanup_remote_parts(
            backend,
            &remote_video_part,
            remote_chat_part.as_deref(),
            &remote_meta_part,
        )
        .await;
        return Err(upload_err);
    }

    // Stage 2: Atomically promote remote .part files to final destinations via rclone moveto
    // Video promotion
    if let Err(err) = backend
        .move_file(&remote_video_part, &remote_video_final)
        .await
    {
        cleanup_remote_parts(
            backend,
            &remote_video_part,
            remote_chat_part.as_deref(),
            &remote_meta_part,
        )
        .await;
        return Err(err).context("Failed to promote staged remote video file to final destination");
    }

    // Chat promotion
    if let (Some(chat_part), Some(chat_final)) = (&remote_chat_part, &remote_chat_final) {
        if let Err(err) = backend.move_file(chat_part, chat_final).await {
            cleanup_remote_parts(backend, "", Some(chat_part), &remote_meta_part).await;
            let _ = backend.delete_file(&remote_video_final).await;
            return Err(err)
                .context("Failed to promote staged remote chat file to final destination");
        }
    }

    // Metadata promotion
    if let Err(err) = backend
        .move_file(&remote_meta_part, &remote_meta_final)
        .await
    {
        let _ = backend.delete_file(&remote_meta_part).await;
        let _ = backend.delete_file(&remote_video_final).await;
        if let Some(ref chat_final) = remote_chat_final {
            let _ = backend.delete_file(chat_final).await;
        }
        return Err(err)
            .context("Failed to promote staged remote metadata file to final destination");
    }

    // Stage 3: Local retention policy
    if delete_after_upload {
        unlink_local_file_with_retry(artifacts.video_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to delete local video file '{}'",
                    artifacts.video_path.display()
                )
            })?;

        if let Some(chat_path) = artifacts.chat_path {
            unlink_local_file_with_retry(chat_path)
                .await
                .with_context(|| {
                    format!("Failed to delete local chat file '{}'", chat_path.display())
                })?;
        }

        unlink_local_file_with_retry(artifacts.metadata_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to delete local metadata file '{}'",
                    artifacts.metadata_path.display()
                )
            })?;
    }

    Ok(())
}
