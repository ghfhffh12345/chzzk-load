use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::chzzk::DEFAULT_USER_AGENT;
use crate::consolidation::staging::validate_mp4_container_integrity;
use crate::uploader::{is_transient_lock_error, lock_backoff_delay, unlink_local_file_with_retry};
use crate::vod::manifest::VodSegment;

/// Resolves the path to the aria2c binary:
/// 1. `override_bin` (CLI argument or explicit option)
/// 2. `CHZZK_LOAD_ARIA2C_BIN` environment variable
/// 3. `config_bin` (`settings.toml` `[aria2c].aria2c_bin`)
/// 4. Default: `"aria2c"` (system PATH)
pub fn resolve_aria2c_bin(override_bin: Option<&str>, config_bin: Option<&str>) -> String {
    if let Some(bin) = override_bin {
        let trimmed = bin.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Ok(bin) = std::env::var("CHZZK_LOAD_ARIA2C_BIN") {
        let trimmed = bin.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    if let Some(bin) = config_bin {
        let trimmed = bin.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    "aria2c".to_string()
}

/// Verifies that the aria2c binary exists and is executable.
/// Fails fast with a clear, actionable diagnostic error if missing.
pub async fn ensure_aria2c_available(bin: &str) -> Result<()> {
    let mut cmd = Command::new(bin);
    cmd.arg("--version");
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    match cmd.output().await {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "aria2c binary '{bin}' returned non-zero exit status {}: {}",
                output.status,
                stderr.trim()
            );
        }
        Err(e) => {
            bail!(
                "aria2c binary '{bin}' was not found or could not be executed ({e}). \
                 Please install aria2c (e.g. 'winget install aria2' or 'apt install aria2') \
                 or specify CHZZK_LOAD_ARIA2C_BIN / [aria2c].aria2c_bin in settings.toml."
            );
        }
    }
}

/// Generates the contents of a batch input file formatted for aria2c (`--input-file`).
///
/// Each segment is formatted as:
/// ```text
/// <url>
///   out=<filename>
/// ```
pub fn generate_aria2c_input_file(segments: &[VodSegment]) -> String {
    let mut out = String::new();
    for seg in segments {
        out.push_str(&seg.url);
        out.push('\n');
        out.push_str("  out=");
        out.push_str(&seg.filename);
        out.push('\n');
    }
    out
}

/// Resolves the staging workspace directory for a VOD archival session.
///
/// If `temp_dir` is provided, stages inside `<temp_dir>/.vod_staging_<video_id>`
/// (or `<temp_dir>` directly if it already ends with that name).
/// Otherwise, defaults to `<output_dir>/.vod_staging_<video_id>`.
pub fn resolve_vod_staging_dir(
    output_dir: &Path,
    video_id: &str,
    temp_dir: Option<&Path>,
) -> PathBuf {
    let folder_name = format!(".vod_staging_{video_id}");
    if let Some(custom) = temp_dir {
        if custom.file_name().and_then(|n| n.to_str()) == Some(&folder_name) {
            custom.to_path_buf()
        } else {
            custom.join(folder_name)
        }
    } else {
        output_dir.join(folder_name)
    }
}

/// Opens a file synchronously with bounded exponential backoff retries (up to 10 attempts),
/// absorbing transient Windows sharing violations (32) and access denied errors (5).
fn open_file_with_retry_sync(path: &Path) -> std::io::Result<std::fs::File> {
    let mut attempts = 0;
    loop {
        match std::fs::File::open(path) {
            Ok(file) => return Ok(file),
            Err(e) if is_transient_lock_error(&e) && attempts < 10 => {
                attempts += 1;
                std::thread::sleep(lock_backoff_delay(attempts));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Queries file metadata asynchronously with bounded exponential backoff retries (up to 10 attempts),
/// absorbing transient Windows sharing violations (32) and access denied errors (5).
async fn metadata_with_retry(path: &Path) -> std::io::Result<std::fs::Metadata> {
    let mut attempts = 0;
    loop {
        match tokio::fs::metadata(path).await {
            Ok(meta) => return Ok(meta),
            Err(e) if is_transient_lock_error(&e) && attempts < 10 => {
                attempts += 1;
                tokio::time::sleep(lock_backoff_delay(attempts)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Known top-level box types that start an ISO BMFF / fMP4 segment or file.
const KNOWN_START_BOXES: &[&[u8]] = &[
    b"styp", b"moof", b"ftyp", b"moov", b"emsg", b"prft", b"sidx", b"ssix", b"free", b"skip",
    b"mdat", b"meta",
];

/// Validates whether an MP4 / fMP4 segment on disk has intact ISO BMFF box headers
/// that cleanly tile the entire file down to the exact byte without trailing corruption.
///
/// If the segment does not start with a recognized ISO BMFF start box (e.g. broadcast-drop
/// raw payload or stream glitch chunks served by the live CDN), it is accepted as a
/// non-box CDN segment to avoid infinite delete-and-redownload retry loops, and left to
/// FFmpeg to handle with `-fflags +discardcorrupt`.
fn is_valid_mp4_box_structure(path: &Path, file_len: u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    if file_len < 8 {
        return false;
    }

    let mut file = match open_file_with_retry_sync(path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    let mut offset = 0u64;
    let mut header = [0u8; 8];
    let mut box_count = 0usize;

    while offset < file_len {
        if offset.saturating_add(8) > file_len {
            return false;
        }
        if file.seek(SeekFrom::Start(offset)).is_err() {
            return false;
        }
        if file.read_exact(&mut header).is_err() {
            return false;
        }

        let size_u32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        if box_count == 0 && !KNOWN_START_BOXES.contains(&box_type) {
            // Segment is not a standard ISO BMFF box-structured file (e.g. raw / broadcast-drop
            // payload emitted by Chzzk live slit recorder). Since aria2c finished downloading
            // without error, accept the file to avoid an infinite purge-and-redownload retry cycle.
            return true;
        }

        let box_size = if size_u32 == 1 {
            if offset.saturating_add(16) > file_len {
                return false;
            }
            let mut ext = [0u8; 8];
            if file.read_exact(&mut ext).is_err() {
                return false;
            }
            let ext_size = u64::from_be_bytes(ext);
            if ext_size < 16 {
                return false;
            }
            ext_size
        } else if size_u32 == 0 {
            file_len - offset
        } else if size_u32 < 8 {
            return false;
        } else {
            size_u32 as u64
        };

        if !box_type
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b' ')
        {
            return false;
        }

        box_count += 1;
        offset = match offset.checked_add(box_size) {
            Some(n) if n <= file_len => n,
            _ => return false,
        };
    }

    offset == file_len && box_count > 0
}

/// Validates whether an MP4 / fMP4 segment on disk strictly conforms to ISO BMFF box structure,
/// starting with a recognized start box and cleanly tiling down to `file_len`.
/// Used by remuxing to ensure only parseable media segments are included in the FFmpeg playlist.
pub fn is_valid_mp4_box_structure_strict(path: &Path, file_len: u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    if file_len < 16 {
        return false;
    }

    let mut file = match open_file_with_retry_sync(path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    let mut offset = 0u64;
    let mut header = [0u8; 8];
    let mut box_count = 0usize;

    while offset < file_len {
        if offset.saturating_add(8) > file_len {
            return false;
        }
        if file.seek(SeekFrom::Start(offset)).is_err() {
            return false;
        }
        if file.read_exact(&mut header).is_err() {
            return false;
        }

        let size_u32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        if box_count == 0 && !KNOWN_START_BOXES.contains(&box_type) {
            return false;
        }

        let box_size = if size_u32 == 1 {
            if offset.saturating_add(16) > file_len {
                return false;
            }
            let mut ext = [0u8; 8];
            if file.read_exact(&mut ext).is_err() {
                return false;
            }
            let ext_size = u64::from_be_bytes(ext);
            if ext_size < 16 {
                return false;
            }
            ext_size
        } else if size_u32 == 0 {
            file_len - offset
        } else if size_u32 < 8 {
            return false;
        } else {
            size_u32 as u64
        };

        if !box_type
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b' ')
        {
            return false;
        }

        box_count += 1;
        offset = match offset.checked_add(box_size) {
            Some(n) if n <= file_len => n,
            _ => return false,
        };
    }

    offset == file_len && box_count > 0
}

/// Checks whether an MPEG-TS segment file is valid:
/// non-empty, packet-aligned (multiple of 188 bytes), and starts with 0x47 sync byte.
pub fn is_valid_ts_segment(path: &Path, file_len: u64) -> bool {
    use std::io::Read;

    if file_len < 188 || file_len % 188 != 0 {
        return false;
    }

    let mut file = match open_file_with_retry_sync(path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    let mut sync_byte = [0u8; 1];
    if file.read_exact(&mut sync_byte).is_err() {
        return false;
    }

    sync_byte[0] == 0x47
}

/// Checks whether a segment has already been fully downloaded to `staging_dir`.
///
/// A segment is considered completed if:
/// 1. Its file exists and has size > 0 bytes.
/// 2. No `.aria2` control file exists for it (allowing brief grace period for async unlinks).
/// 3. Its container structure is valid:
///    - For `.ts`: non-empty, multiple of 188 bytes, starts with `0x47`.
///    - For `.mp4`, `.m4s`, `.m4v`: valid ISO BMFF box headers cleanly tiling the file.
pub(crate) async fn is_segment_completed(staging_dir: &Path, filename: &str) -> bool {
    let path = staging_dir.join(filename);
    let aria2_path = staging_dir.join(format!("{filename}.aria2"));
    if tokio::fs::try_exists(&aria2_path).await.unwrap_or(false) {
        // If an .aria2 control file exists, verify if it is transiently pending deletion
        // upon completed aria2c process execution.
        let mut aria2_still_exists = true;
        for attempt in 1..=5 {
            tokio::time::sleep(lock_backoff_delay(attempt)).await;
            if !tokio::fs::try_exists(&aria2_path).await.unwrap_or(false) {
                aria2_still_exists = false;
                break;
            }
        }
        if aria2_still_exists {
            return false;
        }
    }
    let meta = match metadata_with_retry(&path).await {
        Ok(meta) => meta,
        Err(_) => return false,
    };
    if !meta.is_file() || meta.len() == 0 {
        return false;
    }

    let file_len = meta.len();
    let path_clone = path.clone();
    let filename_str = filename.to_string();

    tokio::task::spawn_blocking(move || {
        if filename_str.ends_with(".ts") {
            is_valid_ts_segment(&path_clone, file_len)
        } else if filename_str.ends_with(".mp4")
            || filename_str.ends_with(".m4s")
            || filename_str.ends_with(".m4v")
        {
            is_valid_mp4_box_structure(&path_clone, file_len)
        } else {
            file_len > 0
        }
    })
    .await
    .unwrap_or(false)
}

/// Dispatches completion progress metrics for all segments in `staging_dir`.
async fn send_completed_segments_progress(
    staging_dir: &Path,
    segments: &[VodSegment],
    progress_tx: Option<
        &tokio::sync::mpsc::UnboundedSender<crate::vod::progress::VodVideoProgressUpdate>,
    >,
) {
    if let Some(tx) = progress_tx {
        let mut final_bytes = 0;
        for seg in segments {
            let seg_path = staging_dir.join(&seg.filename);
            if let Ok(meta) = tokio::fs::metadata(&seg_path).await {
                final_bytes += meta.len();
            }
        }
        let _ = tx.send(
            crate::vod::progress::VodVideoProgressUpdate::SegmentsDownloaded {
                segments: segments.len(),
                bytes: final_bytes,
            },
        );
    }
}

/// Sweeps and deletes any lingering `.aria2` control files in `staging_dir`.
///
/// In segmented downloads, completed segments have no `.aria2` files (aria2c removes them
/// upon complete chunk download). Any `.aria2` file remaining from an interrupted session
/// represents an incomplete chunk whose control file state may desync with refreshed CDN URLs,
/// causing aria2c to fail with `total length mismatch` (errorCode=1).
pub async fn sweep_stale_aria2_control_files(staging_dir: &Path) {
    if let Ok(mut read_dir) = tokio::fs::read_dir(staging_dir).await {
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".aria2") {
                    let _ = unlink_local_file_with_retry(&path).await;
                }
            }
        }
    }
}

/// Filters `segments` against `staging_dir`, returning only those that are missing or incomplete.
///
/// If an incomplete segment file or its `.aria2` control file exists from a previous run,
/// they are purged to ensure aria2c downloads the segment cleanly from byte 0 without
/// triggering control file corruption or size mismatch errors (e.g. `total length mismatch`).
pub async fn filter_missing_segments(
    staging_dir: &Path,
    segments: &[VodSegment],
) -> Vec<VodSegment> {
    let mut missing = Vec::with_capacity(segments.len());
    for seg in segments {
        if !is_segment_completed(staging_dir, &seg.filename).await {
            // Clean up partial segment file and control file if they exist
            let seg_path = staging_dir.join(&seg.filename);
            let aria2_path = staging_dir.join(format!("{}.aria2", seg.filename));
            let _ = unlink_local_file_with_retry(&seg_path).await;
            let _ = unlink_local_file_with_retry(&aria2_path).await;

            missing.push(seg.clone());
        }
    }

    // Sweep any lingering *.aria2 control files in staging_dir
    sweep_stale_aria2_control_files(staging_dir).await;

    missing
}

/// Executes aria2c to download the specified segments into `staging_dir`.
///
/// Multi-socket chunking flags:
/// - `-j16`: maximum 16 concurrent downloads
/// - `-x16`: maximum 16 connections per server
/// - `-s16`: split file into 16 parts
/// - `--allow-overwrite=true`: allow overwriting existing segment files
/// - `--auto-file-renaming=false`: do not append .1 to filenames
pub async fn download_segments_with_aria2c(
    aria2c_bin: &str,
    staging_dir: &Path,
    input_file: &Path,
    cancel_token: CancellationToken,
) -> Result<()> {
    download_segments_with_aria2c_and_progress(
        aria2c_bin,
        staging_dir,
        input_file,
        &[],
        None,
        None,
        cancel_token,
    )
    .await
}

/// Constructs argument list for executing aria2c to download HLS segments.
///
/// Injects browser emulation headers (`User-Agent`, `Referer`, `Origin`),
/// `-k1M` for 1 MiB minimum chunk splitting, and connection concurrency flags.
/// Omits `-c` (`--continue`) so aria2c does not attempt byte-range continuation on
/// small individual segments; segment-level resume is orchestrated via `filter_missing_segments`.
pub fn build_aria2c_segment_args(
    staging_dir: &Path,
    input_file: &Path,
    cookie_header: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        "-j16".to_string(),
        "-x16".to_string(),
        "-s16".to_string(),
        "-k1M".to_string(),
        "--always-resume=false".to_string(),
        "--remove-control-file=true".to_string(),
        "--retry-wait=2".to_string(),
        "--max-tries=5".to_string(),
        "--allow-overwrite=true".to_string(),
        "--auto-file-renaming=false".to_string(),
        "--file-allocation=none".to_string(),
        "--summary-interval=1".to_string(),
        format!("--header=User-Agent: {DEFAULT_USER_AGENT}"),
        "--header=Referer: https://chzzk.naver.com/".to_string(),
        "--header=Origin: https://chzzk.naver.com".to_string(),
    ];

    if let Some(cookie) = cookie_header {
        let trimmed = cookie.trim();
        if !trimmed.is_empty() {
            args.push(format!("--header=Cookie: {trimmed}"));
        }
    }

    args.push("--dir".to_string());
    args.push(staging_dir.to_string_lossy().to_string());
    args.push("--input-file".to_string());
    args.push(input_file.to_string_lossy().to_string());

    args
}

/// Constructs argument list for executing aria2c to download a single monolithic progressive MP4 stream.
///
/// Injects browser emulation headers (`User-Agent`, `Referer`, `Origin`),
/// `-k1M` for 1 MiB minimum chunk splitting, `-x16` (16 connections per server),
/// `-s16` (16 split parts), and resumable continuous download flag `-c`.
pub fn build_aria2c_progressive_args(
    staging_dir: &Path,
    url: &str,
    out_filename: &str,
    cookie_header: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        "-x16".to_string(),
        "-s16".to_string(),
        "-k1M".to_string(),
        "-c".to_string(),
        "--allow-overwrite=true".to_string(),
        "--auto-file-renaming=false".to_string(),
        "--file-allocation=none".to_string(),
        "--summary-interval=1".to_string(),
        format!("--header=User-Agent: {DEFAULT_USER_AGENT}"),
        "--header=Referer: https://chzzk.naver.com/".to_string(),
        "--header=Origin: https://chzzk.naver.com".to_string(),
    ];

    if let Some(cookie) = cookie_header {
        let trimmed = cookie.trim();
        if !trimmed.is_empty() {
            args.push(format!("--header=Cookie: {trimmed}"));
        }
    }

    args.push(format!("--dir={}", staging_dir.display()));
    args.push(format!("--out={out_filename}"));
    args.push(url.to_string());

    args
}

fn format_aria2c_failure_message(
    status: std::process::ExitStatus,
    stdout_logs: &str,
    stderr_logs: &str,
) -> String {
    let mut details = Vec::new();

    // Check stdout logs first since aria2c emits errors and exceptions to stdout
    let cleaned_stdout = stdout_logs
        .lines()
        .map(|l| l.trim())
        .filter(|l| {
            !l.is_empty() && !l.starts_with("***") && !l.starts_with("===") && !l.starts_with("---")
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !cleaned_stdout.is_empty() {
        details.push(cleaned_stdout);
    }

    let cleaned_stderr = stderr_logs.trim();
    if !cleaned_stderr.is_empty() {
        details.push(cleaned_stderr.to_string());
    }

    if let Some(code) = status.code() {
        let explanation = match code {
            2 => "Time out occurred",
            3 => "Resource not found (404)",
            4 => "Resource not found (max file not found limit reached)",
            5 => "Download speed was too slow",
            6 => "Network problem occurred",
            7 => "Unfinished download remained",
            8 => "Remote server does not support resume",
            9 => "Not enough disk space available on target storage drive",
            10 => "Piece length was different from .aria2 control file",
            14 => "HTTP response code was bad / server error",
            19 => "Name resolution (DNS lookup) failed",
            24 => "File allocation error",
            _ => "",
        };
        if !explanation.is_empty() && !details.iter().any(|d| d.contains(explanation)) {
            details.push(explanation.to_string());
        }
    }

    let detail_str = details.join(" | ");
    if detail_str.is_empty() {
        format!("aria2c failed with exit status {status}")
    } else {
        format!("aria2c failed with exit status {status}: {detail_str}")
    }
}

/// Executes aria2c to download a single progressive MP4 video stream into `staging_dir`.
///
/// Implements:
/// - Single-file progressive streaming via multi-connection range requests (`-x16 -s16 -k1M -c`).
/// - Line-by-line asynchronous stdout parsing for progress without any filesystem `read_dir` scanning.
/// - Cursor-resumable staging: if `vod.mp4.part` already exists without `.aria2` and passes container
///   integrity, skips aria2c execution; if `.aria2` exists, resumes from previous cursor.
/// - Cooperative cancellation: immediately terminates and unlinks/kills child process when cancelled.
pub async fn download_progressive_mp4_with_aria2c(
    aria2c_bin: &str,
    staging_dir: &Path,
    url: &str,
    progress_tx: Option<
        tokio::sync::mpsc::UnboundedSender<crate::vod::progress::VodVideoProgressUpdate>,
    >,
    cookie_header: Option<&str>,
    cancel_token: CancellationToken,
) -> Result<PathBuf> {
    if cancel_token.is_cancelled() {
        bail!("VOD download cancelled by token before aria2c execution");
    }

    tokio::fs::create_dir_all(staging_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create staging directory '{}'",
                staging_dir.display()
            )
        })?;

    let out_filename = "vod.mp4.part";
    let staged_file_path = staging_dir.join(out_filename);
    let control_file_path = staging_dir.join(format!("{out_filename}.aria2"));

    // Check if file is already completed (e.g. from prior interrupted session)
    if staged_file_path.exists() && !control_file_path.exists() {
        if let Ok(meta) = tokio::fs::metadata(&staged_file_path).await {
            if meta.len() > 0 && validate_mp4_container_integrity(&staged_file_path).is_ok() {
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(
                        crate::vod::progress::VodVideoProgressUpdate::ProgressiveProgress {
                            bytes: meta.len(),
                            total_bytes: Some(meta.len()),
                            speed: None,
                            eta: None,
                        },
                    );
                }
                return Ok(staged_file_path);
            }
        }
    }

    let mut cmd = Command::new(aria2c_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let args = build_aria2c_progressive_args(staging_dir, url, out_filename, cookie_header);
    cmd.args(&args);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn aria2c binary: '{aria2c_bin}'"))?;

    let stdout = child.stdout.take();
    let stdout_tx = progress_tx.clone();
    let mut stdout_handle = tokio::spawn(async move {
        let mut err_lines = std::collections::VecDeque::with_capacity(15);
        if let Some(pipe) = stdout {
            let mut lines = tokio::io::BufReader::new(pipe).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(ref tx) = stdout_tx {
                    if let Some(prog) =
                        crate::vod::progress::parse_aria2c_single_file_progress(&line)
                    {
                        let _ = tx.send(prog);
                        continue;
                    }
                }
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if err_lines.len() >= 15 {
                        err_lines.pop_front();
                    }
                    err_lines.push_back(trimmed.to_string());
                }
            }
        }
        err_lines.into_iter().collect::<Vec<_>>().join("\n")
    });

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

    tokio::select! {
        _ = cancel_token.cancelled() => {
            let _ = child.kill().await;
            stdout_handle.abort();
            stderr_handle.abort();
            bail!("VOD download cancelled by token during aria2c execution");
        }
        res = child.wait() => {
            let stdout_logs = (&mut stdout_handle).await.unwrap_or_default();
            let stderr_logs = stderr_handle.await.unwrap_or_default();
            let status = res.with_context(|| "Failed waiting on aria2c process")?;
            if !status.success() {
                bail!("{}", format_aria2c_failure_message(status, &stdout_logs, &stderr_logs));
            }

            if let Some(ref tx) = progress_tx {
                if let Ok(meta) = tokio::fs::metadata(&staged_file_path).await {
                    let _ = tx.send(
                        crate::vod::progress::VodVideoProgressUpdate::ProgressiveProgress {
                            bytes: meta.len(),
                            total_bytes: Some(meta.len()),
                            speed: None,
                            eta: None,
                        },
                    );
                }
            }

            Ok(staged_file_path)
        }
    }
}

/// Scans `staging_dir` for completed `.ts` segments, using `completed_cache`
/// to eliminate redundant filesystem `stat` calls on previously completed files.
///
/// Returns `(completed_count, total_completed_bytes)`.
pub async fn scan_completed_segments(
    staging_dir: &Path,
    completed_cache: &mut HashSet<String>,
    cached_bytes: &mut u64,
) -> (usize, u64) {
    if let Ok(mut read_dir) = tokio::fs::read_dir(staging_dir).await {
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "ts" || ext == "mp4" || ext == "m4s" || ext == "m4v" {
                    let filename = path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    if !filename.ends_with(".part")
                        && !filename.ends_with(".aria2")
                        && !completed_cache.contains(&filename)
                    {
                        let aria2_path = staging_dir.join(format!("{filename}.aria2"));
                        if !tokio::fs::try_exists(&aria2_path).await.unwrap_or(false)
                            && let Ok(meta) = entry.metadata().await
                            && meta.is_file()
                            && meta.len() > 0
                        {
                            *cached_bytes += meta.len();
                            completed_cache.insert(filename);
                        }
                    }
                }
            }
        }
    }
    (completed_cache.len(), *cached_bytes)
}

/// Executes aria2c to download the specified segments into `staging_dir` with live progress reporting.
pub async fn download_segments_with_aria2c_and_progress(
    aria2c_bin: &str,
    staging_dir: &Path,
    input_file: &Path,
    segments: &[VodSegment],
    progress_tx: Option<
        tokio::sync::mpsc::UnboundedSender<crate::vod::progress::VodVideoProgressUpdate>,
    >,
    cookie_header: Option<&str>,
    cancel_token: CancellationToken,
) -> Result<()> {
    if cancel_token.is_cancelled() {
        bail!("VOD download cancelled by token before aria2c execution");
    }

    // If input file has 0 bytes (no missing segments), bypass aria2c execution
    let input_len = tokio::fs::metadata(input_file)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    if input_len == 0 {
        send_completed_segments_progress(staging_dir, segments, progress_tx.as_ref()).await;
        return Ok(());
    }

    let mut cmd = Command::new(aria2c_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let args = build_aria2c_segment_args(staging_dir, input_file, cookie_header);
    cmd.args(&args);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn aria2c binary: '{aria2c_bin}'"))?;

    let stdout = child.stdout.take();
    let mut stdout_handle = tokio::spawn(async move {
        let mut err_lines = std::collections::VecDeque::with_capacity(15);
        if let Some(pipe) = stdout {
            let mut lines = tokio::io::BufReader::new(pipe).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if err_lines.len() >= 15 {
                        err_lines.pop_front();
                    }
                    err_lines.push_back(trimmed.to_string());
                }
            }
        }
        err_lines.into_iter().collect::<Vec<_>>().join("\n")
    });

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

    let monitor_tx = progress_tx.clone();
    let staging_dir_buf = staging_dir.to_path_buf();
    let monitor_handle = tokio::spawn(async move {
        if let Some(tx) = monitor_tx {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.tick().await;
            let mut completed_cache = HashSet::new();
            let mut cached_bytes = 0u64;
            loop {
                interval.tick().await;
                let (completed, bytes) = scan_completed_segments(
                    &staging_dir_buf,
                    &mut completed_cache,
                    &mut cached_bytes,
                )
                .await;
                let _ = tx.send(
                    crate::vod::progress::VodVideoProgressUpdate::SegmentsDownloaded {
                        segments: completed,
                        bytes,
                    },
                );
            }
        }
    });

    tokio::select! {
        _ = cancel_token.cancelled() => {
            let _ = child.kill().await;
            stdout_handle.abort();
            stderr_handle.abort();
            monitor_handle.abort();
            bail!("VOD download cancelled by token during aria2c execution");
        }
        res = child.wait() => {
            let stdout_logs = (&mut stdout_handle).await.unwrap_or_default();
            monitor_handle.abort();
            let status = res.with_context(|| "Failed waiting on aria2c process")?;
            let stderr_logs = stderr_handle.await.unwrap_or_default();
            if !status.success() {
                // If every requested segment is already fully downloaded on disk,
                // do not fail on transient aria2c process exit status.
                let missing = filter_missing_segments(staging_dir, segments).await;
                if !missing.is_empty() {
                    bail!("{}", format_aria2c_failure_message(status, &stdout_logs, &stderr_logs));
                }
            }

            send_completed_segments_progress(staging_dir, segments, progress_tx.as_ref()).await;

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_aria2c_bin_precedence() {
        // 1. Explicit override wins
        assert_eq!(
            resolve_aria2c_bin(Some("/custom/aria2c"), Some("config_aria2c")),
            "/custom/aria2c"
        );

        // 2. Config bin used if no override or env
        assert_eq!(
            resolve_aria2c_bin(None, Some("config_aria2c")),
            "config_aria2c"
        );

        // 3. Default to "aria2c"
        assert_eq!(resolve_aria2c_bin(None, None), "aria2c");
    }

    #[test]
    fn test_generate_aria2c_input_file() {
        let segments = vec![
            VodSegment::new(0, "segment_00000.ts", "https://example.com/seg0.ts", 6.0),
            VodSegment::new(1, "segment_00001.ts", "https://example.com/seg1.ts", 6.0),
        ];

        let content = generate_aria2c_input_file(&segments);
        assert_eq!(
            content,
            "https://example.com/seg0.ts\n  out=segment_00000.ts\nhttps://example.com/seg1.ts\n  out=segment_00001.ts\n"
        );
    }

    #[test]
    fn test_resolve_vod_staging_dir() {
        let output_dir = Path::new("/var/recordings/vod_session");
        let video_id = "abc123xyz";

        // Default: nested .vod_staging_<video_id> under output_dir
        let default_staging = resolve_vod_staging_dir(output_dir, video_id, None);
        assert_eq!(
            default_staging,
            PathBuf::from("/var/recordings/vod_session/.vod_staging_abc123xyz")
        );

        // Custom temp_dir path
        let custom_temp = Path::new("/mnt/fast_nvme");
        let custom_staging = resolve_vod_staging_dir(output_dir, video_id, Some(custom_temp));
        assert_eq!(
            custom_staging,
            PathBuf::from("/mnt/fast_nvme/.vod_staging_abc123xyz")
        );

        // Custom temp_dir already ending with .vod_staging_<video_id>
        let already_nested = Path::new("/mnt/fast_nvme/.vod_staging_abc123xyz");
        let direct_staging = resolve_vod_staging_dir(output_dir, video_id, Some(already_nested));
        assert_eq!(
            direct_staging,
            PathBuf::from("/mnt/fast_nvme/.vod_staging_abc123xyz")
        );
    }

    #[tokio::test]
    async fn test_filter_missing_segments() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_filter_seg_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let segments = vec![
            VodSegment::new(0, "seg_00.ts", "https://example.com/00.ts", 6.0),
            VodSegment::new(1, "seg_01.ts", "https://example.com/01.ts", 6.0),
            VodSegment::new(2, "seg_02.ts", "https://example.com/02.ts", 6.0),
            VodSegment::new(3, "seg_03.ts", "https://example.com/03.ts", 6.0),
        ];

        // Case 1: seg_00.ts is fully completed (file exists, valid TS packet 188 bytes, starts with 0x47, no .aria2 file)
        let mut valid_ts = vec![0u8; 188];
        valid_ts[0] = 0x47;
        tokio::fs::write(temp_dir.join("seg_00.ts"), &valid_ts)
            .await
            .unwrap();

        // Case 2: seg_01.ts is incomplete (file exists, but seg_01.ts.aria2 control file exists)
        tokio::fs::write(temp_dir.join("seg_01.ts"), &valid_ts)
            .await
            .unwrap();
        tokio::fs::write(temp_dir.join("seg_01.ts.aria2"), b"aria2 control")
            .await
            .unwrap();

        // Case 3: seg_02.ts is 0 bytes (corrupt/empty)
        tokio::fs::write(temp_dir.join("seg_02.ts"), b"")
            .await
            .unwrap();

        // Case 4: seg_03.ts does not exist at all

        // Case 5: orphan control file without segment
        tokio::fs::write(temp_dir.join("orphan.ts.aria2"), b"orphan control")
            .await
            .unwrap();

        let missing = filter_missing_segments(&temp_dir, &segments).await;

        // Only seg_01, seg_02, seg_03 are missing; seg_00 must be skipped
        let missing_names: Vec<String> = missing.into_iter().map(|s| s.filename).collect();
        assert_eq!(
            missing_names,
            vec![
                "seg_01.ts".to_string(),
                "seg_02.ts".to_string(),
                "seg_03.ts".to_string()
            ]
        );

        // Verify partial segment and control files were purged
        assert!(
            !temp_dir.join("seg_01.ts").exists(),
            "seg_01.ts must be purged"
        );
        assert!(
            !temp_dir.join("seg_01.ts.aria2").exists(),
            "seg_01.ts.aria2 must be purged"
        );
        assert!(
            !temp_dir.join("seg_02.ts").exists(),
            "seg_02.ts (0 bytes) must be purged"
        );
        assert!(
            !temp_dir.join("orphan.ts.aria2").exists(),
            "orphan.ts.aria2 must be purged"
        );
        assert!(
            temp_dir.join("seg_00.ts").exists(),
            "seg_00.ts must be preserved"
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_is_segment_completed_fmp4() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_is_seg_fmp4_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Build valid fMP4 fragment: styp (20 bytes) + moof (16 bytes) + mdat (16 bytes) = 52 bytes
        let mut valid_fmp4 = Vec::new();
        // styp box (20 bytes)
        valid_fmp4.extend_from_slice(&20u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"stypmsix");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        // moof box (16 bytes)
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"moof");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        // mdat box (16 bytes)
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"mdat");
        valid_fmp4.extend_from_slice(&[0u8; 8]);

        tokio::fs::write(temp_dir.join("segment_00000.mp4"), &valid_fmp4)
            .await
            .unwrap();
        assert!(is_segment_completed(&temp_dir, "segment_00000.mp4").await);

        // Truncated fMP4: last box declared 100 bytes but file truncated at 40 bytes
        let mut truncated_fmp4 = valid_fmp4.clone();
        truncated_fmp4.truncate(40);
        tokio::fs::write(temp_dir.join("segment_00001.mp4"), &truncated_fmp4)
            .await
            .unwrap();
        assert!(!is_segment_completed(&temp_dir, "segment_00001.mp4").await);

        // 0-byte file
        tokio::fs::write(temp_dir.join("segment_00002.mp4"), b"")
            .await
            .unwrap();
        assert!(!is_segment_completed(&temp_dir, "segment_00002.mp4").await);

        // Valid file but .aria2 file exists
        tokio::fs::write(temp_dir.join("segment_00003.mp4"), &valid_fmp4)
            .await
            .unwrap();
        tokio::fs::write(temp_dir.join("segment_00003.mp4.aria2"), b"active")
            .await
            .unwrap();
        assert!(!is_segment_completed(&temp_dir, "segment_00003.mp4").await);

        // Raw CDN payload (non-box broadcast drop / stream glitch emitted by CDN)
        let raw_cdn_payload = vec![0xf9, 0x35, 0x6c, 0x29, 0x24, 0x00, 0x11, 0x22, 0x33, 0x44];
        tokio::fs::write(temp_dir.join("segment_00004.mp4"), &raw_cdn_payload)
            .await
            .unwrap();
        assert!(
            is_segment_completed(&temp_dir, "segment_00004.mp4").await,
            "Non-box CDN payload must be accepted as completed"
        );

        // Truncated tiny file (< 8 bytes)
        tokio::fs::write(temp_dir.join("segment_00005.mp4"), &[0x01, 0x02, 0x03])
            .await
            .unwrap();
        assert!(
            !is_segment_completed(&temp_dir, "segment_00005.mp4").await,
            "File smaller than 8 bytes must be rejected as incomplete"
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_is_segment_completed_tolerates_transient_lock() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_is_seg_lock_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let filename = "segment_00000.mp4";
        let path = temp_dir.join(filename);

        // Build valid fMP4 fragment: 52 bytes
        let mut valid_fmp4 = Vec::new();
        valid_fmp4.extend_from_slice(&20u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"stypmsix");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"moof");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"mdat");
        valid_fmp4.extend_from_slice(&[0u8; 8]);

        tokio::fs::write(&path, &valid_fmp4).await.unwrap();

        // Lock file with exclusive access for 50ms (simulating Windows Defender / process closing handle)
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            opts.share_mode(0); // Exclusive lock (ERROR_SHARING_VIOLATION to other openers)
        }
        let lock_file = opts.open(&path).unwrap();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(lock_file);
        });

        let completed = is_segment_completed(&temp_dir, filename).await;
        assert!(
            completed,
            "is_segment_completed must tolerate transient file locks without failing"
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_filter_missing_segments_does_not_purge_transiently_locked_valid_segment() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_filter_lock_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let filename = "segment_00000.mp4";
        let path = temp_dir.join(filename);

        // Build valid fMP4 fragment: 52 bytes
        let mut valid_fmp4 = Vec::new();
        valid_fmp4.extend_from_slice(&20u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"stypmsix");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"moof");
        valid_fmp4.extend_from_slice(&[0u8; 8]);
        valid_fmp4.extend_from_slice(&16u32.to_be_bytes());
        valid_fmp4.extend_from_slice(b"mdat");
        valid_fmp4.extend_from_slice(&[0u8; 8]);

        tokio::fs::write(&path, &valid_fmp4).await.unwrap();

        // Lock file exclusively for 50ms
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            opts.share_mode(0); // Exclusive lock
        }
        let lock_file = opts.open(&path).unwrap();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(lock_file);
        });

        let segments = vec![VodSegment {
            index: 0,
            filename: filename.to_string(),
            url: "https://example.com/segment_00000.mp4".to_string(),
            duration: 4.0,
            map_index: None,
            discontinuity: false,
        }];

        let missing = filter_missing_segments(&temp_dir, &segments).await;
        assert!(
            missing.is_empty(),
            "filter_missing_segments must not mark transiently locked valid segment as missing: {:?}",
            missing
        );
        assert!(
            path.exists(),
            "filter_missing_segments must not purge transiently locked valid segment"
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_filter_missing_segments_preserves_raw_cdn_payload() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_filter_raw_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let filename = "segment_00922.mp4";
        let path = temp_dir.join(filename);
        let raw_payload = vec![0xf9, 0x35, 0x6c, 0x29, 0x24, 0x00, 0x11, 0x22, 0x33, 0x44];
        tokio::fs::write(&path, &raw_payload).await.unwrap();

        let segments = vec![VodSegment {
            index: 922,
            filename: filename.to_string(),
            url: "https://example.com/segment_00922.mp4".to_string(),
            duration: 4.0,
            map_index: None,
            discontinuity: false,
        }];

        let missing = filter_missing_segments(&temp_dir, &segments).await;
        assert!(
            missing.is_empty(),
            "Raw CDN payload must not be marked as missing: {:?}",
            missing
        );
        assert!(
            path.exists(),
            "filter_missing_segments must not purge raw CDN payload file"
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
