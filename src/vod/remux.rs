use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::consolidation::staging::validate_mp4_container_integrity;
pub use crate::consolidation::video::resolve_ffmpeg_bin;
use crate::uploader::unlink_local_file_with_retry;
use crate::vod::manifest::VodSegment;

/// Generates the full content of an FFmpeg concat demuxer manifest script from segment filenames.
pub fn generate_vod_concat_script(staging_dir: &Path, segments: &[VodSegment]) -> String {
    let mut script = String::new();
    let canonical_dir =
        std::fs::canonicalize(staging_dir).unwrap_or_else(|_| staging_dir.to_path_buf());

    for seg in segments {
        let seg_path = canonical_dir.join(&seg.filename);
        let escaped = crate::consolidation::video::escape_concat_path(&seg_path.to_string_lossy());
        script.push_str("file '");
        script.push_str(&escaped);
        script.push_str("'\n");
    }
    script
}

/// Constructs FFmpeg CLI arguments for lossless stream-copy remuxing of downloaded VOD segments.
pub fn build_vod_concat_args(manifest_path: &Path, output_part_path: &Path) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-y".to_string(),
        "-fflags".to_string(),
        "+discardcorrupt".to_string(),
        "-safe".to_string(),
        "0".to_string(),
        "-f".to_string(),
        "concat".to_string(),
        "-i".to_string(),
        manifest_path.to_string_lossy().to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-avoid_negative_ts".to_string(),
        "make_zero".to_string(),
        "-movflags".to_string(),
        "+faststart".to_string(),
        "-f".to_string(),
        "mp4".to_string(),
        output_part_path.to_string_lossy().to_string(),
    ]
}

/// Remuxes downloaded VOD segments into seekable `vod.mp4.part` and validates MP4 container integrity.
/// Finalization and promotion to `vod.mp4` is deferred to the caller orchestrator seam under
/// the Two-Stage Atomic Staging pattern (`CODING_STANDARDS.md` §2).
pub async fn remux_vod_to_staged(
    ffmpeg_bin: &str,
    staging_dir: &Path,
    output_dir: &Path,
    segments: &[VodSegment],
    cancel_token: CancellationToken,
) -> Result<PathBuf> {
    if segments.is_empty() {
        bail!("Cannot remux empty segment list");
    }

    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create output directory '{}'",
                output_dir.display()
            )
        })?;

    let staged_path = output_dir.join("vod.mp4.part");

    // Clean up any lingering part file from prior run
    let _ = unlink_local_file_with_retry(&staged_path).await;

    if cancel_token.is_cancelled() {
        bail!("VOD remuxing cancelled by token before execution");
    }

    // 1. Write concat manifest script inside staging directory
    let manifest_path = staging_dir.join("concat_manifest.txt");
    let script_content = generate_vod_concat_script(staging_dir, segments);
    tokio::fs::write(&manifest_path, script_content.as_bytes())
        .await
        .with_context(|| {
            format!(
                "Failed to write concat script to '{}'",
                manifest_path.display()
            )
        })?;

    // 2. Build and spawn FFmpeg Concat Demuxer command
    let mut cmd = Command::new(ffmpeg_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());
    cmd.args(build_vod_concat_args(&manifest_path, &staged_path));

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn FFmpeg binary: '{ffmpeg_bin}'"))?;

    let stderr = child.stderr.take();
    let stderr_handle = tokio::spawn(async move {
        if let Some(mut err_pipe) = stderr {
            let mut buf = Vec::new();
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut err_pipe, &mut buf).await;
            String::from_utf8_lossy(&buf).to_string()
        } else {
            String::new()
        }
    });

    let remux_res: Result<()> = async {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = child.kill().await;
                stderr_handle.abort();
                let _ = unlink_local_file_with_retry(&staged_path).await;
                bail!("VOD remuxing cancelled by token");
            }
            res = child.wait() => {
                let status = res.with_context(|| "Failed waiting on FFmpeg process")?;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                if !status.success() {
                    let _ = unlink_local_file_with_retry(&staged_path).await;
                    bail!(
                        "FFmpeg remuxing exited with status {status}: {}",
                        stderr_logs.trim()
                    );
                }

                if !staged_path.exists() {
                    bail!(
                        "FFmpeg reported success but staged output file was not created at '{}'",
                        staged_path.display()
                    );
                }

                // 3. Structural MP4 container validation confirms single moov, mdat, and chunk offsets
                validate_mp4_container_integrity(&staged_path).with_context(|| {
                    format!(
                        "FFmpeg produced an invalid MP4 container at '{}'",
                        staged_path.display()
                    )
                })?;

                Ok(())
            }
        }
    }
    .await;

    if let Err(err) = remux_res {
        let _ = unlink_local_file_with_retry(&staged_path).await;
        return Err(err);
    }

    Ok(staged_path)
}

/// Constructs FFmpeg CLI arguments for lossless stream-copy repair and faststart reorganization
/// of a downloaded progressive MP4 container.
pub fn build_mp4_repair_args(input_path: &Path, output_path: &Path) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-y".to_string(),
        "-fflags".to_string(),
        "+discardcorrupt".to_string(),
        "-i".to_string(),
        input_path.to_string_lossy().to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-avoid_negative_ts".to_string(),
        "make_zero".to_string(),
        "-movflags".to_string(),
        "+faststart".to_string(),
        "-f".to_string(),
        "mp4".to_string(),
        output_path.to_string_lossy().to_string(),
    ]
}

/// Invokes FFmpeg to perform lossless stream-copy repair and faststart reorganization on `input`,
/// writing the seekable output file to `output`.
///
/// If `output` already exists, it is unlinked prior to FFmpeg execution.
/// Aborts and cleans up on cancellation or failure.
pub async fn repair_mp4_faststart(
    input: &Path,
    output: &Path,
    ffmpeg_bin: Option<&str>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    if cancel_token.is_cancelled() {
        bail!("VOD MP4 repair cancelled by token before execution");
    }

    if let Some(parent) = output.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "Failed to create parent directory for '{}'",
                output.display()
            )
        })?;
    }

    // Clean up any lingering output part file
    let _ = unlink_local_file_with_retry(output).await;

    let bin = resolve_ffmpeg_bin(ffmpeg_bin);

    let mut cmd = Command::new(&bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());
    cmd.args(build_mp4_repair_args(input, output));

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn FFmpeg binary: '{bin}'"))?;

    let stderr = child.stderr.take();
    let stderr_handle = tokio::spawn(async move {
        if let Some(mut err_pipe) = stderr {
            let mut buf = Vec::new();
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut err_pipe, &mut buf).await;
            String::from_utf8_lossy(&buf).to_string()
        } else {
            String::new()
        }
    });

    let repair_res: Result<()> = async {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = child.kill().await;
                stderr_handle.abort();
                let _ = unlink_local_file_with_retry(output).await;
                bail!("VOD MP4 repair cancelled by token");
            }
            res = child.wait() => {
                let status = res.with_context(|| "Failed waiting on FFmpeg process")?;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                if !status.success() {
                    let _ = unlink_local_file_with_retry(output).await;
                    bail!(
                        "FFmpeg MP4 repair exited with status {status}: {}",
                        stderr_logs.trim()
                    );
                }

                if !output.exists() {
                    bail!(
                        "FFmpeg reported success but repaired output file was not created at '{}'",
                        output.display()
                    );
                }

                Ok(())
            }
        }
    }
    .await;

    if let Err(err) = repair_res {
        let _ = unlink_local_file_with_retry(output).await;
        return Err(err);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vod_concat_args() {
        let manifest = Path::new("staging/concat.txt");
        let output = Path::new("recordings/vod.mp4.part");
        let args = build_vod_concat_args(manifest, output);

        assert!(args.contains(&"-safe".to_string()));
        assert!(args.contains(&"0".to_string()));
        assert!(args.contains(&"-c".to_string()));
        assert!(args.contains(&"copy".to_string()));
        assert!(args.contains(&"-avoid_negative_ts".to_string()));
        assert!(args.contains(&"make_zero".to_string()));
        assert!(args.contains(&"+faststart".to_string()));
    }

    #[test]
    fn test_build_mp4_repair_args() {
        let input = Path::new("staging/vod.mp4.part");
        let output = Path::new("recordings/vod.mp4.part");
        let args = build_mp4_repair_args(input, output);

        assert!(args.contains(&"-c".to_string()));
        assert!(args.contains(&"copy".to_string()));
        assert!(args.contains(&"-avoid_negative_ts".to_string()));
        assert!(args.contains(&"make_zero".to_string()));
        assert!(args.contains(&"+faststart".to_string()));
        assert_eq!(args.last().unwrap(), &output.to_string_lossy().to_string());
    }
}
