use anyhow::Context;
use std::io;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::consolidation::manifest::resolve_rclone_bin;

/// Default timeout for waiting for the rclone loopback server to start and report its port.
pub const DEFAULT_LOOPBACK_STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

/// An ephemeral HTTP loopback server wrapping `rclone serve http`.
///
/// Spawns a background subprocess on loopback (`127.0.0.1:0`), dynamically parses
/// the operating system-assigned port from startup logs, performs a TCP liveness
/// handshake, and ensures the subprocess is killed via an RAII [`Drop`] guard.
#[derive(Debug)]
pub struct EphemeralLoopbackServer {
    child: tokio::process::Child,
    port: u16,
    base_url: String,
    _drain_handle: Option<tokio::task::JoinHandle<()>>,
}

impl EphemeralLoopbackServer {
    /// Starts an ephemeral rclone HTTP loopback server scoped to `remote_base`
    /// using the default startup timeout.
    pub async fn start(remote_base: &str, rclone_bin: Option<&str>) -> anyhow::Result<Self> {
        Self::start_with_timeout(remote_base, rclone_bin, DEFAULT_LOOPBACK_STARTUP_TIMEOUT).await
    }

    /// Starts an ephemeral rclone HTTP loopback server scoped to `remote_base`
    /// with a specified startup timeout.
    pub async fn start_with_timeout(
        remote_base: &str,
        rclone_bin: Option<&str>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let resolved_bin = resolve_rclone_bin(rclone_bin);
        let mut cmd = Command::new(&resolved_bin);
        cmd.kill_on_drop(true);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());
        cmd.arg("serve")
            .arg("http")
            .arg(remote_base)
            .arg("--addr")
            .arg("127.0.0.1:0")
            .arg("--read-only");

        let mut child = cmd.spawn().with_context(|| {
            format!("Failed to spawn rclone serve http process with '{resolved_bin}'")
        })?;

        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to open rclone stderr pipe"))?;

        let mut reader = BufReader::new(stderr).lines();
        let mut logs = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;

        let port = loop {
            let next_line_fut = reader.next_line();
            let line_res = match tokio::time::timeout_at(deadline, next_line_fut).await {
                Ok(res) => res,
                Err(_) => {
                    let _ = child.start_kill();
                    let logs_str = if logs.is_empty() {
                        "no stderr output received".to_string()
                    } else {
                        logs.join("\n")
                    };
                    anyhow::bail!(
                        "Timed out after {:?} waiting for rclone loopback server to start. Captured stderr: {}",
                        timeout,
                        logs_str
                    );
                }
            };

            match line_res {
                Ok(Some(line)) => {
                    if let Some(p) = parse_loopback_port(&line) {
                        break p;
                    }
                    if logs.len() < 50 {
                        logs.push(line);
                    }
                }
                Ok(None) => {
                    let _ = child.start_kill();
                    let logs_str = if logs.is_empty() {
                        "no stderr output received".to_string()
                    } else {
                        logs.join("\n")
                    };
                    anyhow::bail!(
                        "rclone serve http exited prematurely before reporting loopback port: {}",
                        logs_str
                    );
                }
                Err(e) => {
                    let _ = child.start_kill();
                    anyhow::bail!("Failed reading rclone stderr: {e}");
                }
            }
        };

        // TCP liveness check to verify the port is actively listening and accepting connections
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        let mut connected = false;
        for _ in 0..20 {
            if let Ok(Some(exit)) = child.try_wait() {
                let _ = child.start_kill();
                let logs_str = logs.join("\n");
                anyhow::bail!(
                    "rclone serve http exited with status {exit} during liveness check: {logs_str}"
                );
            }
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                connected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if !connected {
            let _ = child.start_kill();
            anyhow::bail!("rclone loopback server on port {port} failed TCP liveness check");
        }

        // Drain remaining stderr in background to prevent OS pipe buffer saturation
        let drain_handle = tokio::spawn(async move {
            let mut reader = reader;
            while let Ok(Some(_)) = reader.next_line().await {}
        });

        Ok(Self {
            child,
            port,
            base_url: format!("http://127.0.0.1:{port}/"),
            _drain_handle: Some(drain_handle),
        })
    }

    /// Returns the dynamically assigned loopback port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the base URL of the loopback server (e.g. `http://127.0.0.1:<port>/`).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Formats a complete loopback HTTP URL for a given chunk name or relative path.
    pub fn chunk_url(&self, chunk_name: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            chunk_name.trim_start_matches('/')
        )
    }

    /// Returns the OS process ID of the rclone child process if available.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Explicitly kills the loopback server subprocess.
    pub async fn kill(&mut self) -> io::Result<()> {
        if let Some(ref handle) = self._drain_handle {
            handle.abort();
        }
        self.child.kill().await
    }
}

impl Drop for EphemeralLoopbackServer {
    fn drop(&mut self) {
        if let Some(ref handle) = self._drain_handle {
            handle.abort();
        }
        let _ = self.child.start_kill();
    }
}

/// Parses the dynamically assigned loopback HTTP port from an rclone startup log line.
///
/// Searches for occurrences of `"127.0.0.1:"` and parses subsequent digits.
/// Returns the first non-zero port found, or `None` if no valid port was located.
pub fn parse_loopback_port(line: &str) -> Option<u16> {
    let key = "127.0.0.1:";
    let mut remainder = line;
    while let Some(idx) = remainder.find(key) {
        let after_ip = &remainder[idx + key.len()..];
        let digits: String = after_ip
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(port) = digits.parse::<u16>() {
            if port > 0 {
                return Some(port);
            }
        }
        remainder = after_ip;
    }
    None
}
