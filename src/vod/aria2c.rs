use anyhow::{Context, Result, bail};
use std::path::Path;
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
    cancel_token: CancellationToken,
) -> Result<()> {
    if cancel_token.is_cancelled() {
        bail!("VOD download cancelled by token before aria2c execution");
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
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--summary-interval=1",
        "--dir",
    ]);
    cmd.arg(staging_dir);
    cmd.arg("--input-file");
    cmd.arg(input_file);

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

            if let Some(ref tx) = progress_tx {
                let mut final_bytes = 0;
                for seg in segments {
                    if let Ok(meta) = tokio::fs::metadata(staging_dir.join(&seg.filename)).await {
                        final_bytes += meta.len();
                    }
                }
                let _ = tx.send(crate::vod::progress::VodVideoProgressUpdate::SegmentsDownloaded {
                    segments: segments.len(),
                    bytes: final_bytes,
                });
            }

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
}
