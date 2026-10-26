use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::AsyncBufReadExt;

use crate::config::RcloneConfig;
use crate::uploader::backend::{BoxFuture, ProgressCallback, UploadBackend};

/// Formats a full destination path for rclone operations.
/// Ensures no redundant consecutive slashes are produced.
pub fn format_destination(remote_path: &str, remote_dir: &str, file_name: &str) -> String {
    let trimmed_base = remote_path.trim_end_matches('/');
    let trimmed_dir = remote_dir.trim_matches('/');
    let trimmed_file = file_name.trim_start_matches('/');

    if trimmed_dir.is_empty() {
        format!("{trimmed_base}/{trimmed_file}")
    } else {
        format!("{trimmed_base}/{trimmed_dir}/{trimmed_file}")
    }
}

/// Statistics emitted by rclone in JSON log events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RcloneStats {
    #[serde(default)]
    pub bytes: u64,
    #[serde(default, rename = "totalBytes")]
    pub total_bytes: u64,
    #[serde(default)]
    pub speed: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct RcloneLogEntry {
    #[serde(default)]
    #[allow(dead_code)]
    pub level: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub msg: Option<String>,
    pub stats: Option<RcloneStats>,
}

fn format_rclone_output_error(
    operation: &str,
    target: &str,
    output: &std::process::Output,
) -> String {
    let stderr_str = String::from_utf8_lossy(&output.stderr);
    let trimmed = stderr_str.trim();
    if trimmed.is_empty() {
        format!(
            "rclone {operation} failed for '{target}' with status {}",
            output.status
        )
    } else {
        format!(
            "rclone {operation} failed for '{target}' with status {}: {trimmed}",
            output.status
        )
    }
}

/// Parses an rclone JSON log line containing transfer statistics.
/// Returns `None` if the line is not JSON or does not contain `stats`.
pub fn parse_rclone_log_line(line: &str) -> Option<RcloneStats> {
    let trimmed = line.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return None;
    }
    serde_json::from_str::<RcloneLogEntry>(trimmed)
        .ok()
        .and_then(|entry| entry.stats)
}

/// Storage backend powered by the rclone CLI subprocess.
#[derive(Debug, Clone)]
pub struct RcloneBackend {
    pub config: RcloneConfig,
    custom_bin: Option<String>,
}

impl RcloneBackend {
    /// Creates a new `RcloneBackend` from configuration.
    pub fn new(config: RcloneConfig) -> Self {
        Self {
            config,
            custom_bin: None,
        }
    }

    /// Sets an explicit rclone binary path for this backend instance.
    pub fn with_bin(mut self, bin: impl Into<String>) -> Self {
        self.custom_bin = Some(bin.into());
        self
    }

    /// Resolves the rclone binary to execute based on precedence:
    /// `custom_bin` (via `with_bin`) > `CHZZK_LOAD_RCLONE_BIN` env > `config.rclone_bin` > `"rclone"`.
    pub fn resolve_bin(&self) -> String {
        if let Some(ref custom) = self.custom_bin {
            return custom.clone();
        }
        if let Some(bin) = std::env::var("CHZZK_LOAD_RCLONE_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
        {
            return bin;
        }
        if !self.config.rclone_bin.trim().is_empty() {
            return self.config.rclone_bin.clone();
        }
        "rclone".to_string()
    }

    /// Builds a `tokio::process::Command` configured for `rclone copyto`.
    pub fn build_copy_command(
        &self,
        local_path: &Path,
        remote_dest: &str,
    ) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(self.resolve_bin());
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
        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }
        cmd
    }

    /// Builds a `tokio::process::Command` configured for `rclone lsf --max-depth 1`.
    pub fn build_check_command(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(self.resolve_bin());
        cmd.kill_on_drop(true);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::piped());
        cmd.arg("lsf")
            .arg("--max-depth")
            .arg("1")
            .arg(&self.config.remote_path);
        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }
        cmd
    }

    /// Builds a `tokio::process::Command` configured for `rclone moveto`.
    pub fn build_move_command(
        &self,
        remote_src: &str,
        remote_dst: &str,
    ) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(self.resolve_bin());
        cmd.kill_on_drop(true);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::piped());
        cmd.arg("moveto").arg(remote_src).arg(remote_dst);
        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }
        cmd
    }

    /// Builds a `tokio::process::Command` configured for `rclone deletefile`.
    pub fn build_delete_command(&self, remote_path: &str) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(self.resolve_bin());
        cmd.kill_on_drop(true);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::piped());
        cmd.arg("deletefile").arg(remote_path);
        for arg in &self.config.extra_args {
            cmd.arg(arg);
        }
        cmd
    }

    /// Atomically renames/moves a remote file from `remote_src` to `remote_dst` via `rclone moveto`.
    pub async fn move_file(&self, remote_src: &str, remote_dst: &str) -> anyhow::Result<()> {
        let bin = self.resolve_bin();
        let mut cmd = self.build_move_command(remote_src, remote_dst);
        let output = cmd
            .output()
            .await
            .with_context(|| format!("failed to execute rclone moveto from '{bin}'"))?;
        if output.status.success() {
            Ok(())
        } else {
            let target = format!("{remote_src}' -> '{remote_dst}");
            anyhow::bail!("{}", format_rclone_output_error("moveto", &target, &output));
        }
    }

    /// Deletes a remote file via `rclone deletefile`.
    ///
    /// Exit code 4 in rclone denotes "directory or object not found", which is treated as success.
    pub async fn delete_file(&self, remote_path: &str) -> anyhow::Result<()> {
        let bin = self.resolve_bin();
        let mut cmd = self.build_delete_command(remote_path);
        let output = cmd
            .output()
            .await
            .with_context(|| format!("failed to execute rclone deletefile from '{bin}'"))?;
        if output.status.success() {
            Ok(())
        } else {
            if output.status.code() == Some(4) {
                return Ok(());
            }
            anyhow::bail!(
                "{}",
                format_rclone_output_error("deletefile", remote_path, &output)
            );
        }
    }

    /// Uploads a local file directly to a designated `remote_dest` via `rclone copyto`.
    pub async fn upload_file_to_dest(
        &self,
        local_path: &Path,
        remote_dest: &str,
        on_progress: ProgressCallback,
    ) -> anyhow::Result<u64> {
        let metadata = tokio::fs::metadata(local_path)
            .await
            .with_context(|| format!("failed to read local file metadata: {local_path:?}"))?;
        let file_size = metadata.len();

        let bin = self.resolve_bin();
        let mut cmd = self.build_copy_command(local_path, remote_dest);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn rclone copyto from '{bin}'"))?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to capture stderr from rclone process"))?;

        let mut reader = tokio::io::BufReader::new(stderr).lines();
        let mut last_speed = 0.0;
        let mut stderr_logs = Vec::new();

        while let Ok(Some(line)) = reader.next_line().await {
            if let Some(stats) = parse_rclone_log_line(&line) {
                let total = if stats.total_bytes > 0 {
                    stats.total_bytes
                } else {
                    file_size
                };
                let speed_mb_s = stats.speed / 1_048_576.0;
                last_speed = speed_mb_s;
                on_progress(stats.bytes, total, speed_mb_s);
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
            on_progress(file_size, file_size, last_speed);
            Ok(file_size)
        } else {
            let err_msg = stderr_logs.join("\n");
            let trimmed = err_msg.trim();
            if trimmed.is_empty() {
                anyhow::bail!("rclone failed with status {status}");
            } else {
                anyhow::bail!("rclone failed with status {status}: {trimmed}");
            }
        }
    }
}

impl UploadBackend for RcloneBackend {
    fn upload_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| anyhow::anyhow!("invalid local file path: {local_path:?}"))?;
            let remote_dest = format_destination(&self.config.remote_path, remote_dir, file_name);
            self.upload_file_to_dest(local_path, &remote_dest, on_progress)
                .await
        })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let bin = self.resolve_bin();
            let mut cmd = self.build_check_command();
            let output = cmd
                .output()
                .await
                .with_context(|| format!("failed to execute rclone check from '{bin}'"))?;
            let status = output.status;
            let remote_path = &self.config.remote_path;
            if status.success() {
                Ok(())
            } else {
                let stderr_str = String::from_utf8_lossy(&output.stderr);
                let trimmed = stderr_str.trim();
                if trimmed.is_empty() {
                    Err(anyhow::anyhow!(
                        "rclone connection check failed for '{remote_path}' with status {status}"
                    ))
                } else {
                    Err(anyhow::anyhow!(
                        "rclone connection check failed for '{remote_path}' with status {status}: {trimmed}"
                    ))
                }
            }
        })
    }
}
