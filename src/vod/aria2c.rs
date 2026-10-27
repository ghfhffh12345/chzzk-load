use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

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

/// Checks whether a segment has already been fully downloaded to `staging_dir`.
///
/// A segment is considered completed if:
/// 1. Its file exists and has size > 0 bytes.
/// 2. No `.aria2` control file exists for it.
pub(crate) async fn is_segment_completed(staging_dir: &Path, filename: &str) -> bool {
    let path = staging_dir.join(filename);
    let aria2_path = staging_dir.join(format!("{filename}.aria2"));
    if tokio::fs::try_exists(&aria2_path).await.unwrap_or(false) {
        return false;
    }
    match tokio::fs::metadata(&path).await {
        Ok(meta) => meta.is_file() && meta.len() > 0,
        Err(_) => false,
    }
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

/// Filters `segments` against `staging_dir`, returning only those that are missing or incomplete.
pub async fn filter_missing_segments(
    staging_dir: &Path,
    segments: &[VodSegment],
) -> Vec<VodSegment> {
    let mut missing = Vec::with_capacity(segments.len());
    for seg in segments {
        if !is_segment_completed(staging_dir, &seg.filename).await {
            missing.push(seg.clone());
        }
    }
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

    cmd.args([
        "-j16",
        "-x16",
        "-s16",
        "-c",
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--summary-interval=1",
        "--dir",
    ]);
    cmd.arg(staging_dir);
    cmd.arg("--input-file");
    cmd.arg(input_file);

    if let Some(cookie) = cookie_header {
        let trimmed = cookie.trim();
        if !trimmed.is_empty() {
            cmd.arg(format!("--header=Cookie: {trimmed}"));
        }
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn aria2c binary: '{aria2c_bin}'"))?;

    let stdout = child.stdout.take();
    let stdout_tx = progress_tx.clone();
    let stdout_handle = tokio::spawn(async move {
        if let Some(pipe) = stdout {
            let mut lines = tokio::io::BufReader::new(pipe).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(ref tx) = stdout_tx {
                    if let Some(speed) = crate::vod::progress::parse_aria2c_speed(&line) {
                        let _ = tx.send(crate::vod::progress::VodVideoProgressUpdate::Speed(speed));
                    }
                }
            }
        }
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
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.tick().await;
            loop {
                interval.tick().await;
                let mut completed = 0;
                let mut bytes = 0;
                if let Ok(mut read_dir) = tokio::fs::read_dir(&staging_dir_buf).await {
                    while let Ok(Some(entry)) = read_dir.next_entry().await {
                        let path = entry.path();
                        if let Some(ext) = path.extension() {
                            if ext == "ts" {
                                let filename =
                                    path.file_name().unwrap_or_default().to_string_lossy();
                                let aria2_path = staging_dir_buf.join(format!("{filename}.aria2"));
                                if !tokio::fs::try_exists(&aria2_path).await.unwrap_or(false)
                                    && let Ok(meta) = entry.metadata().await
                                    && meta.len() > 0
                                {
                                    completed += 1;
                                    bytes += meta.len();
                                }
                            }
                        }
                    }
                }
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
            stdout_handle.abort();
            monitor_handle.abort();
            let status = res.with_context(|| "Failed waiting on aria2c process")?;
            let stderr_logs = stderr_handle.await.unwrap_or_default();
            if !status.success() {
                bail!(
                    "aria2c failed with exit status {status}: {}",
                    stderr_logs.trim()
                );
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
            VodSegment {
                index: 0,
                filename: "segment_00000.ts".to_string(),
                url: "https://example.com/seg0.ts".to_string(),
                duration: 6.0,
            },
            VodSegment {
                index: 1,
                filename: "segment_00001.ts".to_string(),
                url: "https://example.com/seg1.ts".to_string(),
                duration: 6.0,
            },
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
            VodSegment {
                index: 0,
                filename: "seg_00.ts".to_string(),
                url: "https://example.com/00.ts".to_string(),
                duration: 6.0,
            },
            VodSegment {
                index: 1,
                filename: "seg_01.ts".to_string(),
                url: "https://example.com/01.ts".to_string(),
                duration: 6.0,
            },
            VodSegment {
                index: 2,
                filename: "seg_02.ts".to_string(),
                url: "https://example.com/02.ts".to_string(),
                duration: 6.0,
            },
            VodSegment {
                index: 3,
                filename: "seg_03.ts".to_string(),
                url: "https://example.com/03.ts".to_string(),
                duration: 6.0,
            },
        ];

        // Case 1: seg_00.ts is fully completed (file exists, size > 0, no .aria2 file)
        tokio::fs::write(temp_dir.join("seg_00.ts"), b"valid MPEG-TS data")
            .await
            .unwrap();

        // Case 2: seg_01.ts is incomplete (file exists, but seg_01.ts.aria2 control file exists)
        tokio::fs::write(temp_dir.join("seg_01.ts"), b"partial data")
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

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
