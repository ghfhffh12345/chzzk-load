pub use super::manifest::join_remote_path;
use super::manifest::{ConsolidationChunk, TargetLocation, resolve_rclone_bin};
pub use crate::uploader::worker::rename_local_file_with_retry;
use crate::uploader::worker::unlink_local_file_with_retry;
use anyhow::Context;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Configuration options for video consolidation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VideoConsolidationOptions {
    pub ffmpeg_bin: Option<String>,
    pub rclone_bin: Option<String>,
}

impl VideoConsolidationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ffmpeg_bin(mut self, bin: impl Into<String>) -> Self {
        self.ffmpeg_bin = Some(bin.into());
        self
    }

    pub fn with_rclone_bin(mut self, bin: impl Into<String>) -> Self {
        self.rclone_bin = Some(bin.into());
        self
    }
}

/// Result metrics returned upon successful video consolidation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoConsolidationResult {
    pub chunks_processed: usize,
    pub bytes_written: u64,
}

/// Resolves the FFmpeg binary path to use:
/// `override_bin` > `CHZZK_LOAD_FFMPEG_BIN` env var > `"ffmpeg"`.
pub fn resolve_ffmpeg_bin(override_bin: Option<&str>) -> String {
    if let Some(bin) = override_bin {
        if !bin.trim().is_empty() {
            return bin.to_string();
        }
    }
    if let Some(bin) = std::env::var("CHZZK_LOAD_FFMPEG_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        return bin;
    }
    "ffmpeg".to_string()
}

/// Constructs the list of command line arguments for the FFmpeg remuxing process.
///
/// Implements ADR 0009 and Ticket #24:
/// - `-c copy`: Lossless stream-copy without transcoding
/// - `-bsf:a aac_adtstoasc`: Converts ADTS AAC headers to AudioSpecificConfig (ASC) for MP4 container
/// - `-progress pipe:2`: Emits progress telemetry lines to stderr
/// - `-movflags frag_keyframe+empty_moov`: Fragmented MP4 for live streaming writes
/// - `-f mp4`: MPEG-4 container
/// - `-fflags +genpts+discardcorrupt`: Absorbs PTS gaps and sequence discontinuities
/// - `-i pipe:0`: Reads MPEG-TS stream from stdin
/// - `pipe:1`: Writes fragmented MP4 stream to stdout
pub fn build_ffmpeg_remux_args() -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-progress".to_string(),
        "pipe:2".to_string(),
        "-y".to_string(),
        "-fflags".to_string(),
        "+genpts+discardcorrupt".to_string(),
        "-i".to_string(),
        "pipe:0".to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-bsf:a".to_string(),
        "aac_adtstoasc".to_string(),
        "-movflags".to_string(),
        "frag_keyframe+empty_moov".to_string(),
        "-f".to_string(),
        "mp4".to_string(),
        "pipe:1".to_string(),
    ]
}

/// Builds the Tokio `Command` for the FFmpeg remuxing process with piped stdio.
pub fn build_ffmpeg_remux_command(ffmpeg_bin: &str) -> Command {
    let mut cmd = Command::new(ffmpeg_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.args(build_ffmpeg_remux_args());
    cmd
}

/// Atomically finalizes the staged `consolidated.mp4.part` into `consolidated.mp4`.
///
/// In local mode: uses `tokio::fs::rename` with Windows file lock retries.
/// In remote mode: executes `rclone moveto <staged> <final>`.
pub async fn finalize_staged_video(
    target: &TargetLocation,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            let staged = dir.join("consolidated.mp4.part");
            let finalized = dir.join("consolidated.mp4");
            let _ = unlink_local_file_with_retry(&finalized).await;
            rename_local_file_with_retry(&staged, &finalized)
                .await
                .with_context(|| {
                    format!(
                        "Failed to rename staged video '{}' to '{}'",
                        staged.display(),
                        finalized.display()
                    )
                })?;
            Ok(())
        }
        TargetLocation::Remote(remote_base) => {
            let staged = join_remote_path(remote_base, "consolidated.mp4.part");
            let finalized = join_remote_path(remote_base, "consolidated.mp4");
            let bin = resolve_rclone_bin(rclone_bin);

            let mut cmd = Command::new(&bin);
            cmd.kill_on_drop(true);
            cmd.stdin(Stdio::null());
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::piped());
            cmd.arg("moveto").arg(&staged).arg(&finalized);

            let output = cmd.output().await.with_context(|| {
                format!("Failed to execute '{bin} moveto {staged} {finalized}'")
            })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!(
                    "rclone moveto failed with status {}: {}",
                    output.status,
                    stderr.trim()
                );
            }
            Ok(())
        }
    }
}

/// Deletes the temporary staged `consolidated.mp4.part` if present upon error or abort.
///
/// In local mode: unlinks `consolidated.mp4.part` (treating NotFound as success).
/// In remote mode: executes `rclone deletefile <staged>`.
pub async fn cleanup_staged_video(
    target: &TargetLocation,
    rclone_bin: Option<&str>,
) -> anyhow::Result<()> {
    match target {
        TargetLocation::Local(dir) => {
            let staged = dir.join("consolidated.mp4.part");
            let _ = unlink_local_file_with_retry(&staged).await;
            Ok(())
        }
        TargetLocation::Remote(remote_base) => {
            let staged = join_remote_path(remote_base, "consolidated.mp4.part");
            let bin = resolve_rclone_bin(rclone_bin);
            super::chat::delete_remote_file(&bin, &staged).await;
            Ok(())
        }
    }
}

/// Sequentially streams chunk bytes into the provided writer sink without buffering chunks to local disk.
///
/// - In local mode: opens each chunk file on the local filesystem and streams bytes.
/// - In remote mode: spawns `rclone cat <remote-path>/<chunk>` and streams its stdout.
pub async fn feed_video_chunks<W>(
    target: &TargetLocation,
    chunks: &[ConsolidationChunk],
    rclone_bin: Option<&str>,
    writer: &mut W,
) -> anyhow::Result<u64>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut total_bytes = 0u64;

    for chunk in chunks {
        match target {
            TargetLocation::Local(dir) => {
                let chunk_path = dir.join(&chunk.name);
                let mut file = tokio::fs::File::open(&chunk_path).await.with_context(|| {
                    format!("Failed to open local chunk file: {}", chunk_path.display())
                })?;
                let copied = tokio::io::copy(&mut file, writer).await.with_context(|| {
                    format!(
                        "Error writing local chunk '{}' to pipe sink",
                        chunk_path.display()
                    )
                })?;
                total_bytes = total_bytes.saturating_add(copied);
            }
            TargetLocation::Remote(remote_base) => {
                let remote_chunk = join_remote_path(remote_base, &chunk.name);
                let bin = resolve_rclone_bin(rclone_bin);

                let mut cmd = Command::new(&bin);
                cmd.kill_on_drop(true);
                cmd.stdin(Stdio::null());
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::piped());
                cmd.arg("cat").arg(&remote_chunk);

                let mut child = cmd.spawn().with_context(|| {
                    format!("Failed to spawn '{bin} cat {remote_chunk}' process")
                })?;

                let mut stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout of rclone cat"))?;

                let copied = tokio::io::copy(&mut stdout, writer)
                    .await
                    .with_context(|| {
                        format!(
                            "Error streaming rclone cat output for '{remote_chunk}' to pipe sink"
                        )
                    })?;
                total_bytes = total_bytes.saturating_add(copied);

                let status = child
                    .wait()
                    .await
                    .with_context(|| format!("Failed to wait for '{bin} cat {remote_chunk}'"))?;

                if !status.success() {
                    let mut stderr_content = String::new();
                    if let Some(mut stderr) = child.stderr.take() {
                        let _ = stderr.read_to_string(&mut stderr_content).await;
                    }
                    anyhow::bail!(
                        "rclone cat for '{remote_chunk}' exited with status {}: {}",
                        status,
                        stderr_content.trim()
                    );
                }
            }
        }
    }

    writer
        .flush()
        .await
        .context("Failed to flush writer sink after streaming video chunks")?;
    Ok(total_bytes)
}

/// Telemetry metrics parsed asynchronously from FFmpeg progress output on stderr (Issue #24).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VideoProgressTelemetry {
    pub frame: Option<u64>,
    pub fps: Option<String>,
    pub out_time: Option<String>,
    pub speed: Option<String>,
    pub total_size: Option<u64>,
}

impl VideoProgressTelemetry {
    pub fn update_from_line(&mut self, line: &str) -> bool {
        let trimmed = line.trim();
        if let Some((k, v)) = trimmed.split_once('=') {
            let key = k.trim();
            let val = v.trim();
            match key {
                "frame" => {
                    if let Ok(num) = val.parse::<u64>() {
                        self.frame = Some(num);
                    }
                    return true;
                }
                "fps" => {
                    self.fps = Some(val.to_string());
                    return true;
                }
                "out_time" => {
                    self.out_time = Some(val.to_string());
                    return true;
                }
                "speed" => {
                    self.speed = Some(val.to_string());
                    return true;
                }
                "total_size" => {
                    if let Ok(num) = val.parse::<u64>() {
                        self.total_size = Some(num);
                    }
                    return true;
                }
                "progress" | "out_time_us" | "out_time_ms" | "dup_frames" | "drop_frames"
                | "bitrate" => {
                    return true;
                }
                _ if key.starts_with("stream_") => {
                    return true;
                }
                _ => {}
            }
        }
        false
    }
}

/// Executes the resilient video pipe remuxing and streaming consolidation pipeline into staged `consolidated.mp4.part`.
///
/// Features:
/// - Pure stream remuxing via FFmpeg without local intermediate buffering
/// - Fragmented MP4 generation (`-movflags frag_keyframe+empty_moov`)
/// - Asynchronous stderr progress telemetry parsing (`-progress pipe:2`)
/// - Staged output streaming to `consolidated.mp4.part` without final rename
/// - Cooperative cancellation: reacts to `cancel_token` and cancels child processes
/// - Error abort isolation: terminates subprocesses, removes `.part`, leaves original `.ts` untouched.
pub async fn consolidate_video(
    target: &TargetLocation,
    chunks: &[ConsolidationChunk],
    options: &VideoConsolidationOptions,
    cancel_token: CancellationToken,
) -> anyhow::Result<VideoConsolidationResult> {
    if chunks.is_empty() {
        return Ok(VideoConsolidationResult {
            chunks_processed: 0,
            bytes_written: 0,
        });
    }

    let ffmpeg_bin = resolve_ffmpeg_bin(options.ffmpeg_bin.as_deref());
    let rclone_bin = options.rclone_bin.clone();

    // Spawn FFmpeg remuxing process
    let mut ffmpeg_cmd = build_ffmpeg_remux_command(&ffmpeg_bin);
    let mut ffmpeg_child = ffmpeg_cmd
        .spawn()
        .with_context(|| format!("Failed to spawn ffmpeg binary: '{ffmpeg_bin}'"))?;

    let mut ffmpeg_stdin = ffmpeg_child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stdin pipe"))?;
    let mut ffmpeg_stdout = ffmpeg_child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stdout pipe"))?;
    let ffmpeg_stderr = ffmpeg_child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stderr pipe"))?;

    // Background task to monitor FFmpeg stderr diagnostics and parse progress telemetry lines
    let stderr_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(ffmpeg_stderr).lines();
        let mut logs = Vec::new();
        let mut telemetry = VideoProgressTelemetry::default();
        while let Ok(Some(line)) = reader.next_line().await {
            if telemetry.update_from_line(&line) {
                // Successfully parsed progress telemetry line
            } else if logs.len() < 100 {
                logs.push(line);
            }
        }
        logs.join("\n")
    });

    // Output sink task writing FFmpeg stdout to staged .part destination
    let target_for_sink = target.clone();
    let rclone_bin_for_sink = rclone_bin.clone();
    let sink_handle = tokio::spawn(async move {
        match target_for_sink {
            TargetLocation::Local(dir) => {
                let staged_path = dir.join("consolidated.mp4.part");
                let mut file = tokio::fs::File::create(&staged_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create staged output file: {}",
                            staged_path.display()
                        )
                    })?;
                let bytes = tokio::io::copy(&mut ffmpeg_stdout, &mut file)
                    .await
                    .with_context(|| "Failed streaming ffmpeg stdout to staged local file")?;
                file.flush()
                    .await
                    .context("Failed flushing staged local file")?;
                Ok(bytes)
            }
            TargetLocation::Remote(remote_base) => {
                let staged_remote = join_remote_path(&remote_base, "consolidated.mp4.part");
                let bin = resolve_rclone_bin(rclone_bin_for_sink.as_deref());

                let mut cmd = Command::new(&bin);
                cmd.kill_on_drop(true);
                cmd.stdin(Stdio::piped());
                cmd.stdout(Stdio::null());
                cmd.stderr(Stdio::piped());
                cmd.arg("rcat").arg(&staged_remote);

                let mut child = cmd.spawn().with_context(|| {
                    format!("Failed to spawn '{bin} rcat {staged_remote}' process")
                })?;

                let mut stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("Failed to open rclone rcat stdin pipe"))?;

                let bytes = tokio::io::copy(&mut ffmpeg_stdout, &mut stdin)
                    .await
                    .with_context(|| "Failed streaming ffmpeg stdout to rclone rcat stdin")?;
                stdin
                    .flush()
                    .await
                    .context("Failed flushing rclone rcat stdin")?;
                drop(stdin); // Send EOF to rclone rcat so upload finalizes

                let status = child
                    .wait()
                    .await
                    .with_context(|| format!("Failed to wait for '{bin} rcat {staged_remote}'"))?;

                if !status.success() {
                    let mut stderr_content = String::new();
                    if let Some(mut stderr) = child.stderr.take() {
                        let _ = stderr.read_to_string(&mut stderr_content).await;
                    }
                    anyhow::bail!(
                        "rclone rcat for '{staged_remote}' exited with status {}: {}",
                        status,
                        stderr_content.trim()
                    );
                }
                Ok(bytes)
            }
        }
    });

    // Chunk feeder task writing TS chunks to FFmpeg stdin
    let target_for_feeder = target.clone();
    let chunks_to_feed = chunks.to_vec();
    let rclone_bin_for_feeder = rclone_bin.clone();
    let feeder_handle = tokio::spawn(async move {
        let res = feed_video_chunks(
            &target_for_feeder,
            &chunks_to_feed,
            rclone_bin_for_feeder.as_deref(),
            &mut ffmpeg_stdin,
        )
        .await;
        // Explicitly drop stdin to signal EOF to FFmpeg
        drop(ffmpeg_stdin);
        res
    });

    // Coordinated execution with failure isolation and cooperative cancellation
    let run_result: anyhow::Result<u64> = async {
        let mut feeder_handle = feeder_handle;
        let mut sink_handle = sink_handle;

        tokio::select! {
            _ = cancel_token.cancelled() => {
                feeder_handle.abort();
                sink_handle.abort();
                let _ = ffmpeg_child.kill().await;
                let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
            }
            feed_res = &mut feeder_handle => {
                match feed_res {
                    Ok(Ok(_)) => {
                        // Feeder finished cleanly and closed stdin.
                        // Wait for FFmpeg process and sink task to complete.
                        tokio::select! {
                            _ = cancel_token.cancelled() => {
                                sink_handle.abort();
                                let _ = ffmpeg_child.kill().await;
                                let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                                anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
                            }
                            sink_out = &mut sink_handle => {
                                let bytes_written = sink_out
                                    .map_err(|e| anyhow::anyhow!("Sink task panicked: {e}"))??;
                                let status = ffmpeg_child.wait().await.context("Failed waiting on ffmpeg process")?;
                                let stderr_logs = stderr_handle.await.unwrap_or_default();
                                if !status.success() {
                                    cancel_token.cancel();
                                    let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                                    anyhow::bail!("FFmpeg remuxing exited with status {status}: {stderr_logs}");
                                }
                                Ok(bytes_written)
                            }
                            status_res = ffmpeg_child.wait() => {
                                let status = status_res.context("Failed waiting on ffmpeg process")?;
                                let sink_out = sink_handle.await
                                    .map_err(|e| anyhow::anyhow!("Sink task panicked: {e}"))??;
                                let stderr_logs = stderr_handle.await.unwrap_or_default();
                                if !status.success() {
                                    cancel_token.cancel();
                                    let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                                    anyhow::bail!("FFmpeg remuxing exited with status {status}: {stderr_logs}");
                                }
                                Ok(sink_out)
                            }
                        }
                    }
                    Ok(Err(feed_err)) => {
                        cancel_token.cancel();
                        sink_handle.abort();
                        let _ = ffmpeg_child.kill().await;
                        let stderr_logs = stderr_handle.await.unwrap_or_default();
                        let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                        anyhow::bail!("Video chunk feeder failed: {feed_err}. FFmpeg stderr: {stderr_logs}");
                    }
                    Err(join_err) => {
                        cancel_token.cancel();
                        sink_handle.abort();
                        let _ = ffmpeg_child.kill().await;
                        let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                        anyhow::bail!("Video feeder task panicked: {join_err}");
                    }
                }
            }
            sink_res = &mut sink_handle => {
                cancel_token.cancel();
                feeder_handle.abort();
                let _ = ffmpeg_child.kill().await;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                match sink_res {
                    Ok(Ok(_)) => anyhow::bail!("Video sink completed prematurely before feeder. FFmpeg stderr: {stderr_logs}"),
                    Ok(Err(e)) => anyhow::bail!("Video sink error: {e}. FFmpeg stderr: {stderr_logs}"),
                    Err(e) => anyhow::bail!("Video sink task panicked: {e}. FFmpeg stderr: {stderr_logs}"),
                }
            }
            status_res = ffmpeg_child.wait() => {
                cancel_token.cancel();
                feeder_handle.abort();
                sink_handle.abort();
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
                let status = status_res.context("Failed waiting on ffmpeg process")?;
                anyhow::bail!("FFmpeg exited prematurely with status {status} before feeder completed. FFmpeg stderr: {stderr_logs}");
            }
        }
    }.await;

    match run_result {
        Ok(bytes_written) => {
            // Do NOT finalize to consolidated.mp4 here!
            // Staged output is retained in consolidated.mp4.part for atomic two-stage finalization.
            Ok(VideoConsolidationResult {
                chunks_processed: chunks.len(),
                bytes_written,
            })
        }
        Err(err) => {
            cancel_token.cancel();
            // Error abort handling: clean up .part file and leave original chunks untouched
            let _ = cleanup_staged_video(target, rclone_bin.as_deref()).await;
            Err(err)
        }
    }
}
