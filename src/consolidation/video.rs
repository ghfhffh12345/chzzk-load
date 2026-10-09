pub use super::loopback::{
    DEFAULT_LOOPBACK_STARTUP_TIMEOUT, EphemeralLoopbackServer, parse_loopback_port,
};
pub use super::manifest::join_remote_path;
use super::manifest::{ConsolidationChunk, TargetLocation, resolve_rclone_bin};
pub use crate::uploader::worker::rename_local_file_with_retry;
use crate::uploader::{unlink_local_file_with_retry, unlink_local_file_with_retry_sync};
use anyhow::Context;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use super::progress::VideoProgressUpdate;

/// Configuration options for video consolidation.
#[derive(Debug, Clone, Default)]
pub struct VideoConsolidationOptions {
    pub ffmpeg_bin: Option<String>,
    pub rclone_bin: Option<String>,
    pub progress_sender: Option<tokio::sync::mpsc::UnboundedSender<VideoProgressUpdate>>,
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

    pub fn with_progress_sender(
        mut self,
        sender: tokio::sync::mpsc::UnboundedSender<VideoProgressUpdate>,
    ) -> Self {
        self.progress_sender = Some(sender);
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

/// Escapes and normalizes a file path or URL for use in an FFmpeg concat demuxer script.
///
/// Concat demuxer escaping rules:
/// - Normalizes Windows backslashes `\` to forward slashes `/`.
/// - Escapes single quotes `'` as `'\\''` so that when wrapped in `'...'` by FFmpeg's
///   concat demuxer, single quotes are safely closed and escaped.
/// - Preserves Unicode, Korean streamer names, spaces, brackets, and HTTP URLs cleanly.
pub fn escape_concat_path(path: &str) -> String {
    path.replace('\\', "/").replace('\'', r"'\''")
}

/// Generates the full content of an FFmpeg concat demuxer manifest script from a slice of paths or URLs.
///
/// Each entry is escaped via [`escape_concat_path`] and formatted as:
/// ```text
/// file '<escaped-path>'
/// ```
/// All lines strictly end with `\n`.
pub fn generate_concat_script<S: AsRef<str>>(entries: &[S]) -> String {
    let mut script = String::new();
    for entry in entries {
        let escaped = escape_concat_path(entry.as_ref());
        script.push_str("file '");
        script.push_str(&escaped);
        script.push_str("'\n");
    }
    script
}

/// RAII guard managing a temporary FFmpeg concat demuxer manifest file on disk.
///
/// Automatically removes the temporary file when dropped, unless ownership of the path
/// is extracted via [`ConcatScriptGuard::into_path`].
#[derive(Debug)]
pub struct ConcatScriptGuard {
    path: PathBuf,
    active: bool,
}

impl ConcatScriptGuard {
    /// Creates a new guard for the given temporary file path.
    pub fn new(path: PathBuf) -> Self {
        Self { path, active: true }
    }

    /// Returns the reference to the underlying file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Defuses the guard and extracts the underlying path without deleting the file.
    pub fn into_path(mut self) -> PathBuf {
        self.active = false;
        self.path.clone()
    }
}

impl Drop for ConcatScriptGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _ = unlink_local_file_with_retry_sync(&self.path);
    }
}

impl AsRef<Path> for ConcatScriptGuard {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl std::ops::Deref for ConcatScriptGuard {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl std::fmt::Display for ConcatScriptGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

static CONCAT_MANIFEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn generate_unique_concat_script_path(dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let counter = CONCAT_MANIFEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!("concat_{pid}_{now}_{counter}.txt"))
}

/// Creates a temporary concat script in the specified directory, writing UTF-8 content without BOM.
///
/// If writing fails, any partially created file is guaranteed to be cleaned up before returning `Err`.
pub async fn create_temp_concat_script_in<S: AsRef<str>>(
    dir: &Path,
    entries: &[S],
) -> anyhow::Result<ConcatScriptGuard> {
    let path = generate_unique_concat_script_path(dir);
    let guard = ConcatScriptGuard::new(path);
    let content = generate_concat_script(entries);

    // Writing strictly UTF-8 bytes without BOM.
    // If write fails, `guard` drops on the `?` error path and unlinks any partial file.
    tokio::fs::write(guard.path(), content.as_bytes())
        .await
        .with_context(|| {
            format!(
                "Failed to write concat script to {}",
                guard.path().display()
            )
        })?;

    Ok(guard)
}

/// Creates a temporary concat script in `std::env::temp_dir()`, writing UTF-8 content without BOM.
///
/// If writing fails, any partially created file is guaranteed to be cleaned up before returning `Err`.
pub async fn create_temp_concat_script<S: AsRef<str>>(
    entries: &[S],
) -> anyhow::Result<ConcatScriptGuard> {
    create_temp_concat_script_in(&std::env::temp_dir(), entries).await
}

/// Synchronous version of [`create_temp_concat_script_in`].
pub fn create_temp_concat_script_in_sync<S: AsRef<str>>(
    dir: &Path,
    entries: &[S],
) -> anyhow::Result<ConcatScriptGuard> {
    let path = generate_unique_concat_script_path(dir);
    let guard = ConcatScriptGuard::new(path);
    let content = generate_concat_script(entries);

    std::fs::write(guard.path(), content.as_bytes()).with_context(|| {
        format!(
            "Failed to write concat script to {}",
            guard.path().display()
        )
    })?;

    Ok(guard)
}

/// Synchronous version of [`create_temp_concat_script`].
pub fn create_temp_concat_script_sync<S: AsRef<str>>(
    entries: &[S],
) -> anyhow::Result<ConcatScriptGuard> {
    create_temp_concat_script_in_sync(&std::env::temp_dir(), entries)
}

/// Constructs the list of command line arguments for local mode FFmpeg Concat Demuxer remuxing (Spec #56, Ticket #58).
///
/// Features:
/// - `-safe 0`: Allows arbitrary and absolute paths in the concat manifest script
/// - `-f concat`: Invokes FFmpeg's native container-aware Concat Demuxer
/// - `-i <manifest_path>`: Reads file entries from the manifest script
/// - `-c copy`: Lossless stream-copy without transcoding
/// - `-bsf:a aac_adtstoasc`: Converts ADTS AAC headers to AudioSpecificConfig (ASC) for MP4 container
/// - `-avoid_negative_ts make_zero`: Resets container timeline start to 0.0s, eliminating initial playback freezing
/// - `-movflags +faststart`: Relocates `moov` atom seek table to beginning of MP4 for instant desktop seeking
/// - `-progress pipe:2`: Emits progress telemetry key-value pairs to stderr
/// - `<output_path>`: Direct staged file destination (`consolidated.mp4.part`) enabling seekable writes
pub fn build_local_concat_ffmpeg_args(manifest_path: &Path, output_path: &Path) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-progress".to_string(),
        "pipe:2".to_string(),
        "-y".to_string(),
        "-fflags".to_string(),
        "+genpts+discardcorrupt".to_string(),
        "-safe".to_string(),
        "0".to_string(),
        "-f".to_string(),
        "concat".to_string(),
        "-i".to_string(),
        manifest_path.to_string_lossy().to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-bsf:a".to_string(),
        "aac_adtstoasc".to_string(),
        "-avoid_negative_ts".to_string(),
        "make_zero".to_string(),
        "-movflags".to_string(),
        "+faststart".to_string(),
        "-f".to_string(),
        "mp4".to_string(),
        output_path.to_string_lossy().to_string(),
    ]
}

/// Builds the Tokio `Command` for the local mode FFmpeg Concat Demuxer process.
///
/// Configures `stdin(Stdio::null())`, `stdout(Stdio::null())`, `stderr(Stdio::piped())`,
/// and `kill_on_drop(true)`.
pub fn build_local_concat_ffmpeg_command(
    ffmpeg_bin: &str,
    manifest_path: &Path,
    output_path: &Path,
) -> Command {
    let mut cmd = Command::new(ffmpeg_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());
    cmd.args(build_local_concat_ffmpeg_args(manifest_path, output_path));
    cmd
}

/// Constructs the list of command line arguments for remote mode FFmpeg Concat Demuxer remuxing (Spec #56, Ticket #59).
///
/// Features:
/// - `-protocol_whitelist file,http,tcp`: Whitelists file, http, and tcp protocols for reading the manifest and loopback chunk streams
/// - `-safe 0`: Allows loopback HTTP URLs in the concat manifest script
/// - `-f concat`: Invokes FFmpeg's native container-aware Concat Demuxer
/// - `-i <manifest_path>`: Reads chunk URLs from the manifest script
/// - `-c copy`: Lossless stream-copy without transcoding
/// - `-bsf:a aac_adtstoasc`: Converts ADTS AAC headers to AudioSpecificConfig (ASC) for MP4 container
/// - `-avoid_negative_ts make_zero`: Resets container timeline start to 0.0s, eliminating initial playback freezing
/// - `-movflags frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets`: Fragmented MP4 for live streaming writes
/// - `-progress pipe:2`: Emits progress telemetry key-value pairs to stderr
/// - `pipe:1`: Emits fragmented MP4 byte stream to stdout for rclone rcat
pub fn build_remote_concat_ffmpeg_args(manifest_path: &Path) -> Vec<String> {
    vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-progress".to_string(),
        "pipe:2".to_string(),
        "-y".to_string(),
        "-fflags".to_string(),
        "+genpts+discardcorrupt".to_string(),
        "-protocol_whitelist".to_string(),
        "file,http,tcp".to_string(),
        "-safe".to_string(),
        "0".to_string(),
        "-f".to_string(),
        "concat".to_string(),
        "-i".to_string(),
        manifest_path.to_string_lossy().to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-bsf:a".to_string(),
        "aac_adtstoasc".to_string(),
        "-avoid_negative_ts".to_string(),
        "make_zero".to_string(),
        "-movflags".to_string(),
        "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets".to_string(),
        "-f".to_string(),
        "mp4".to_string(),
        "pipe:1".to_string(),
    ]
}

/// Builds the Tokio `Command` for the remote mode FFmpeg Concat Demuxer process.
///
/// Configures `stdin(Stdio::null())`, `stdout(Stdio::piped())`, `stderr(Stdio::piped())`,
/// and `kill_on_drop(true)`.
pub fn build_remote_concat_ffmpeg_command(ffmpeg_bin: &str, manifest_path: &Path) -> Command {
    let mut cmd = Command::new(ffmpeg_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.args(build_remote_concat_ffmpeg_args(manifest_path));
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

fn normalize_status_line(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        result.push(c);
        if c == '=' {
            while let Some(&next) = chars.peek() {
                if next.is_whitespace() {
                    chars.next();
                } else {
                    break;
                }
            }
        }
    }
    result
}

fn parse_size_bytes(v: &str) -> Option<u64> {
    let v = v.trim();
    if v.is_empty() || v.eq_ignore_ascii_case("n/a") {
        return None;
    }
    if let Ok(num) = v.parse::<u64>() {
        return Some(num);
    }
    let (num_str, mult) = if let Some(s) = v.strip_suffix("kib").or_else(|| v.strip_suffix("KiB")) {
        (s, 1024u64)
    } else if let Some(s) = v
        .strip_suffix("kB")
        .or_else(|| v.strip_suffix("kb"))
        .or_else(|| v.strip_suffix("KB"))
    {
        (s, 1024u64)
    } else if let Some(s) = v.strip_suffix("mib").or_else(|| v.strip_suffix("MiB")) {
        (s, 1024 * 1024u64)
    } else if let Some(s) = v.strip_suffix("MB").or_else(|| v.strip_suffix("mb")) {
        (s, 1024 * 1024u64)
    } else if let Some(s) = v.strip_suffix("gib").or_else(|| v.strip_suffix("GiB")) {
        (s, 1024 * 1024 * 1024u64)
    } else if let Some(s) = v.strip_suffix("GB").or_else(|| v.strip_suffix("gb")) {
        (s, 1024 * 1024 * 1024u64)
    } else {
        let s = v.strip_suffix('b').or_else(|| v.strip_suffix('B'))?;
        (s, 1u64)
    };
    num_str
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
}

/// Telemetry metrics parsed asynchronously from FFmpeg progress output on stderr (Issue #24, Spec #56, Ticket #60).
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
        if trimmed.is_empty() {
            return false;
        }

        let normalized = normalize_status_line(trimmed);
        let mut recognized = false;

        for token in normalized.split_whitespace() {
            if let Some((k, v)) = token.split_once('=') {
                let key = k.trim();
                let val = v.trim();
                match key {
                    "frame" => {
                        if let Ok(num) = val.parse::<u64>() {
                            self.frame = Some(num);
                        }
                        recognized = true;
                    }
                    "fps" => {
                        self.fps = Some(val.to_string());
                        recognized = true;
                    }
                    "out_time" | "time" => {
                        self.out_time = Some(val.to_string());
                        recognized = true;
                    }
                    "speed" => {
                        self.speed = Some(val.to_string());
                        recognized = true;
                    }
                    "total_size" => {
                        if let Ok(num) = val.parse::<u64>() {
                            self.total_size = Some(num);
                        }
                        recognized = true;
                    }
                    "size" => {
                        if let Some(bytes) = parse_size_bytes(val) {
                            self.total_size = Some(bytes);
                        }
                        recognized = true;
                    }
                    "progress" | "out_time_us" | "out_time_ms" | "dup_frames" | "drop_frames"
                    | "bitrate" | "q" | "Lq" | "dup" | "drop" => {
                        recognized = true;
                    }
                    _ if key.starts_with("stream_") => {
                        recognized = true;
                    }
                    _ => {}
                }
            }
        }

        recognized
    }
}

/// Spawns an asynchronous background task monitoring FFmpeg stderr diagnostics, parsing
/// progress telemetry lines, and reporting metrics via the optional progress channel.
pub fn spawn_stderr_telemetry_monitor(
    ffmpeg_stderr: tokio::process::ChildStderr,
    progress_sender: Option<tokio::sync::mpsc::UnboundedSender<VideoProgressUpdate>>,
    total_manifest_bytes: u64,
    total_chunks: usize,
) -> tokio::task::JoinHandle<String> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(ffmpeg_stderr).lines();
        let mut logs = Vec::new();
        let mut telemetry = VideoProgressTelemetry::default();
        let mut last_speed = None;
        let mut last_chunks_fed = 0usize;
        let mut last_bytes_fed = 0u64;

        while let Ok(Some(line)) = reader.next_line().await {
            if telemetry.update_from_line(&line) {
                if let Some(ref speed) = telemetry.speed {
                    if Some(speed) != last_speed.as_ref() {
                        last_speed = Some(speed.clone());
                        if let Some(ref tx) = progress_sender {
                            let _ = tx.send(VideoProgressUpdate::Speed(speed.clone()));
                        }
                    }
                }
                if let Some(total_size) = telemetry.total_size {
                    if total_manifest_bytes > 0 && total_chunks > 0 {
                        let chunks_fed = ((total_size as f64 / total_manifest_bytes as f64)
                            * total_chunks as f64)
                            .round()
                            .min(total_chunks as f64)
                            as usize;
                        if chunks_fed != last_chunks_fed || total_size != last_bytes_fed {
                            last_chunks_fed = chunks_fed;
                            last_bytes_fed = total_size;
                            if let Some(ref tx) = progress_sender {
                                let _ = tx.send(VideoProgressUpdate::ChunkFed {
                                    chunks_fed,
                                    bytes_fed: total_size,
                                });
                            }
                        }
                    }
                }
            } else if logs.len() < 100 {
                logs.push(line);
            }
        }
        logs.join("\n")
    })
}

/// Executes the resilient video consolidation pipeline into staged `consolidated.mp4.part`.
///
/// In local mode: uses FFmpeg's native container-aware Concat Demuxer with `-movflags +faststart`
/// and `-avoid_negative_ts make_zero` directly targeting `consolidated.mp4.part`.
/// In remote mode: uses FFmpeg's native container-aware Concat Demuxer with Ephemeral Loopback Server
/// streaming fragmented MP4 to rclone rcat.
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

    match target {
        TargetLocation::Local(dir) => {
            consolidate_video_local(dir, chunks, options, cancel_token).await
        }
        TargetLocation::Remote(remote_base) => {
            consolidate_video_remote(remote_base, chunks, options, cancel_token).await
        }
    }
}

/// Local mode video consolidation using FFmpeg Concat Demuxer and faststart seek table (Spec #56, Ticket #58).
pub async fn consolidate_video_local(
    dir: &Path,
    chunks: &[ConsolidationChunk],
    options: &VideoConsolidationOptions,
    cancel_token: CancellationToken,
) -> anyhow::Result<VideoConsolidationResult> {
    let total_chunks = chunks.len();
    let total_manifest_bytes: u64 = chunks.iter().map(|c| c.size).sum();
    let staged_path = dir.join("consolidated.mp4.part");
    let target = TargetLocation::Local(dir.to_path_buf());

    // Clean up any lingering .part file from prior aborted run
    let _ = unlink_local_file_with_retry(&staged_path).await;

    if cancel_token.is_cancelled() {
        anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
    }

    // 1. Generate entries for temporary concat script
    let entries: Vec<String> = chunks
        .iter()
        .map(|c| dir.join(&c.name).to_string_lossy().to_string())
        .collect();

    // 2. Create temporary concat script guarded by ConcatScriptGuard (RAII cleanup)
    let manifest_guard = create_temp_concat_script(&entries).await.with_context(|| {
        format!(
            "Failed to create temporary concat script for {} chunks",
            chunks.len()
        )
    })?;

    // 3. Resolve FFmpeg binary
    let ffmpeg_bin = resolve_ffmpeg_bin(options.ffmpeg_bin.as_deref());

    // 4. Build and spawn FFmpeg Concat Demuxer command directly outputting to staged file
    let mut cmd =
        build_local_concat_ffmpeg_command(&ffmpeg_bin, manifest_guard.path(), &staged_path);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = cleanup_staged_video(&target, None).await;
            return Err(e)
                .with_context(|| format!("Failed to spawn ffmpeg binary: '{ffmpeg_bin}'"));
        }
    };

    let ffmpeg_stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stderr pipe"))?;

    // 5. Background task to monitor FFmpeg stderr diagnostics and parse progress telemetry lines
    let stderr_handle = spawn_stderr_telemetry_monitor(
        ffmpeg_stderr,
        options.progress_sender.clone(),
        total_manifest_bytes,
        total_chunks,
    );

    // 6. Cooperative cancellation & execution barrier
    let run_res: anyhow::Result<u64> = async {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = child.kill().await;
                let _ = cleanup_staged_video(&target, None).await;
                anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
            }
            status_res = child.wait() => {
                let status = status_res.context("Failed waiting on ffmpeg process")?;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                if !status.success() {
                    cancel_token.cancel();
                    let _ = cleanup_staged_video(&target, None).await;
                    anyhow::bail!("FFmpeg remuxing exited with status {status}: {stderr_logs}");
                }

                let metadata = tokio::fs::metadata(&staged_path)
                    .await
                    .with_context(|| {
                        format!(
                            "FFmpeg completed with exit status {status} but staged output file was not produced: '{}'",
                            staged_path.display()
                        )
                    })?;
                let bytes_written = metadata.len();

                if let Some(ref tx) = options.progress_sender {
                    let _ = tx.send(VideoProgressUpdate::ChunkFed {
                        chunks_fed: total_chunks,
                        bytes_fed: bytes_written,
                    });
                }

                Ok(bytes_written)
            }
        }
    }
    .await;

    // ConcatScriptGuard drop will automatically remove the temp manifest script
    drop(manifest_guard);

    match run_res {
        Ok(bytes_written) => Ok(VideoConsolidationResult {
            chunks_processed: total_chunks,
            bytes_written,
        }),
        Err(err) => {
            cancel_token.cancel();
            let _ = cleanup_staged_video(&target, None).await;
            Err(err)
        }
    }
}

/// Remote mode video consolidation using FFmpeg Concat Demuxer and Ephemeral Loopback Server (Spec #56, Ticket #59).
pub async fn consolidate_video_remote(
    remote_base: &str,
    chunks: &[ConsolidationChunk],
    options: &VideoConsolidationOptions,
    cancel_token: CancellationToken,
) -> anyhow::Result<VideoConsolidationResult> {
    let total_chunks = chunks.len();
    let total_manifest_bytes: u64 = chunks.iter().map(|c| c.size).sum();
    let target = TargetLocation::Remote(remote_base.to_string());
    let staged_remote = join_remote_path(remote_base, "consolidated.mp4.part");
    let ffmpeg_bin = resolve_ffmpeg_bin(options.ffmpeg_bin.as_deref());
    let rclone_bin = options.rclone_bin.clone();

    // Clean up any lingering .part file from prior aborted run
    let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;

    if cancel_token.is_cancelled() {
        anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
    }

    // 1. Spawn Ephemeral Loopback Server scoped to remote_base
    let mut loopback_server = EphemeralLoopbackServer::start(remote_base, rclone_bin.as_deref())
        .await
        .with_context(|| {
            format!("Failed to start ephemeral loopback server for remote '{remote_base}'")
        })?;

    // 2. Construct manifest entries as http://127.0.0.1:{port}/{chunk.name}
    let entries: Vec<String> = chunks
        .iter()
        .map(|c| loopback_server.chunk_url(&c.name))
        .collect();

    // 3. Create temporary concat script guarded by ConcatScriptGuard (RAII cleanup)
    let manifest_guard = create_temp_concat_script(&entries).await.with_context(|| {
        format!(
            "Failed to create temporary concat script for {} remote chunks",
            chunks.len()
        )
    })?;

    // 4. Build and spawn FFmpeg Concat Demuxer command with piped stdout for rclone rcat
    let mut ffmpeg_cmd = build_remote_concat_ffmpeg_command(&ffmpeg_bin, manifest_guard.path());
    let mut ffmpeg_child = match ffmpeg_cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = loopback_server.kill().await;
            let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;
            return Err(e)
                .with_context(|| format!("Failed to spawn ffmpeg binary: '{ffmpeg_bin}'"));
        }
    };

    let mut ffmpeg_stdout = ffmpeg_child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stdout pipe"))?;
    let ffmpeg_stderr = ffmpeg_child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to open FFmpeg stderr pipe"))?;

    // 5. Background task to monitor FFmpeg stderr diagnostics and parse progress telemetry lines
    let stderr_handle = spawn_stderr_telemetry_monitor(
        ffmpeg_stderr,
        options.progress_sender.clone(),
        total_manifest_bytes,
        total_chunks,
    );

    // 6. Output sink task streaming FFmpeg stdout into rclone rcat
    let rclone_bin_for_sink = rclone_bin.clone();
    let staged_remote_for_sink = staged_remote.clone();
    let sink_handle = tokio::spawn(async move {
        let bin = resolve_rclone_bin(rclone_bin_for_sink.as_deref());

        let mut cmd = Command::new(&bin);
        cmd.kill_on_drop(true);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());
        cmd.arg("rcat").arg(&staged_remote_for_sink);

        let mut child = cmd.spawn().with_context(|| {
            format!("Failed to spawn '{bin} rcat {staged_remote_for_sink}' process")
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
            .with_context(|| format!("Failed to wait for '{bin} rcat {staged_remote_for_sink}'"))?;

        if !status.success() {
            let mut stderr_content = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut stderr_content).await;
            }
            anyhow::bail!(
                "rclone rcat for '{staged_remote_for_sink}' exited with status {}: {}",
                status,
                stderr_content.trim()
            );
        }
        Ok(bytes)
    });

    // 7. Cooperative cancellation & execution barrier
    let run_result: anyhow::Result<u64> = async {
        let mut sink_handle = sink_handle;
        tokio::select! {
            _ = cancel_token.cancelled() => {
                sink_handle.abort();
                let _ = ffmpeg_child.kill().await;
                let _ = loopback_server.kill().await;
                let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;
                anyhow::bail!("Video consolidation cancelled by cooperative cancellation token");
            }
            sink_out = &mut sink_handle => {
                let bytes_written = sink_out
                    .map_err(|e| anyhow::anyhow!("Sink task panicked: {e}"))??;
                let status = ffmpeg_child.wait().await.context("Failed waiting on ffmpeg process")?;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                if !status.success() {
                    cancel_token.cancel();
                    let _ = loopback_server.kill().await;
                    let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;
                    anyhow::bail!("FFmpeg remuxing exited with status {status}: {stderr_logs}");
                }
                Ok(bytes_written)
            }
            status_res = ffmpeg_child.wait() => {
                let status = status_res.context("Failed waiting on ffmpeg process")?;
                let stderr_logs = stderr_handle.await.unwrap_or_default();
                if !status.success() {
                    cancel_token.cancel();
                    sink_handle.abort();
                    let _ = loopback_server.kill().await;
                    let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;
                    anyhow::bail!("FFmpeg remuxing exited with status {status}: {stderr_logs}");
                }
                let bytes_written = sink_handle
                    .await
                    .map_err(|e| anyhow::anyhow!("Sink task panicked: {e}"))??;
                Ok(bytes_written)
            }
        }
    }
    .await;

    // Drop temp manifest and loopback server explicitly
    drop(manifest_guard);
    drop(loopback_server);

    match run_result {
        Ok(bytes_written) => {
            if let Some(ref tx) = options.progress_sender {
                let _ = tx.send(VideoProgressUpdate::ChunkFed {
                    chunks_fed: total_chunks,
                    bytes_fed: bytes_written,
                });
            }
            Ok(VideoConsolidationResult {
                chunks_processed: total_chunks,
                bytes_written,
            })
        }
        Err(err) => {
            cancel_token.cancel();
            let _ = cleanup_staged_video(&target, rclone_bin.as_deref()).await;
            Err(err)
        }
    }
}
