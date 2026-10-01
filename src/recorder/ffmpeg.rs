use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinHandle;

pub fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim();
    let mut result = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => result.push('_'),
            c if c.is_control() => result.push('_'),
            other => result.push(other),
        }
    }
    result
}

pub fn build_ffmpeg_command_with_bin(
    ffmpeg_bin: &str,
    m3u8_url: &str,
    output_pattern: &Path,
    chunk_duration_seconds: u64,
    cookie_header: Option<&str>,
) -> Command {
    let mut cmd = Command::new(ffmpeg_bin);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y");

    let mut headers = String::from(
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n",
    );
    if let Some(cookie) = cookie_header {
        headers.push_str("Cookie: ");
        headers.push_str(cookie);
        headers.push_str("\r\n");
    }
    cmd.arg("-headers").arg(headers);
    cmd.arg("-extension_picky").arg("0");

    cmd.arg("-i")
        .arg(m3u8_url)
        .arg("-c")
        .arg("copy")
        .arg("-f")
        .arg("segment")
        .arg("-segment_time")
        .arg(chunk_duration_seconds.to_string())
        .arg("-segment_format")
        .arg("mpegts")
        .arg("-reset_timestamps")
        .arg("1")
        .arg(output_pattern);

    cmd
}

pub fn build_ffmpeg_command(
    m3u8_url: &str,
    output_pattern: &Path,
    chunk_duration_seconds: u64,
    cookie_header: Option<&str>,
) -> Command {
    let ffmpeg_bin =
        std::env::var("CHZZK_LOAD_FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());
    build_ffmpeg_command_with_bin(
        &ffmpeg_bin,
        m3u8_url,
        output_pattern,
        chunk_duration_seconds,
        cookie_header,
    )
}

/// Detects whether an FFmpeg stderr log line indicates an AES key 403 Forbidden or
/// DRM permission denied error that occurs when recording restricted streams without
/// authenticated Naver credentials.
pub fn is_ffmpeg_key_forbidden_error(line: &str) -> bool {
    let lower = line.trim().to_ascii_lowercase();
    lower.contains("unable to open key file")
        || (lower.contains("403 forbidden")
            && (lower.contains("aes_key") || lower.contains("key file")))
        || (lower.contains("aes_key")
            && (lower.contains("access denied")
                || lower.contains("permission denied")
                || lower.contains("forbidden")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FfmpegEvent {
    Log(String),
    KeyForbidden,
    Exited(std::process::ExitStatus),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FfmpegExit {
    Clean(std::process::ExitStatus),
    Killed(std::process::ExitStatus),
}

/// Encapsulates child process execution, piped I/O, stderr log parsing,
/// AES key error detection, and graceful termination escalation.
pub struct FfmpegSession {
    child: Child,
    stdin: Option<ChildStdin>,
    event_rx: Receiver<FfmpegEvent>,
    _reader_handle: JoinHandle<()>,
    key_forbidden: Arc<AtomicBool>,
    exit_status: Option<std::process::ExitStatus>,
    has_yielded_exited: bool,
}

impl FfmpegSession {
    pub fn spawn(
        m3u8_url: &str,
        output_pattern: &Path,
        chunk_duration_seconds: u64,
        cookie_header: Option<&str>,
        ffmpeg_bin: Option<&str>,
    ) -> std::io::Result<Self> {
        let cmd = match ffmpeg_bin {
            Some(bin) => build_ffmpeg_command_with_bin(
                bin,
                m3u8_url,
                output_pattern,
                chunk_duration_seconds,
                cookie_header,
            ),
            None => build_ffmpeg_command(
                m3u8_url,
                output_pattern,
                chunk_duration_seconds,
                cookie_header,
            ),
        };
        Self::from_command(cmd)
    }

    pub fn from_command(mut cmd: Command) -> std::io::Result<Self> {
        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take();
        let stderr = child.stderr.take();

        let (event_tx, event_rx) = mpsc::channel(64);
        let key_forbidden = Arc::new(AtomicBool::new(false));
        let key_forbidden_clone = key_forbidden.clone();

        let reader_handle = tokio::spawn(async move {
            if let Some(stderr) = stderr {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut reader = BufReader::new(stderr);
                let mut byte_buf = Vec::new();
                while let Ok(n) = reader.read_until(b'\n', &mut byte_buf).await {
                    if n == 0 {
                        break;
                    }
                    let lossy_line = String::from_utf8_lossy(&byte_buf);
                    let trimmed = lossy_line.trim();
                    if !trimmed.is_empty() {
                        let is_key_error = is_ffmpeg_key_forbidden_error(trimmed);
                        if is_key_error {
                            key_forbidden_clone.store(true, Ordering::SeqCst);
                            let _ = event_tx.send(FfmpegEvent::KeyForbidden).await;
                        }
                        if !key_forbidden_clone.load(Ordering::SeqCst) {
                            let _ = event_tx.send(FfmpegEvent::Log(trimmed.to_string())).await;
                        }
                    }
                    byte_buf.clear();
                }
            }
        });

        Ok(Self {
            child,
            stdin,
            event_rx,
            _reader_handle: reader_handle,
            key_forbidden,
            exit_status: None,
            has_yielded_exited: false,
        })
    }

    pub fn child_id(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn is_key_forbidden(&self) -> bool {
        self.key_forbidden.load(Ordering::SeqCst)
    }

    pub async fn stop_graceful(&mut self, timeout: Duration) -> std::io::Result<FfmpegExit> {
        if let Some(status) = self.exit_status {
            return Ok(FfmpegExit::Clean(status));
        }
        if let Some(mut stdin) = self.stdin.take() {
            use tokio::io::AsyncWriteExt;
            let _ = stdin.write_all(b"q\n").await;
            let _ = stdin.flush().await;
            drop(stdin);
        }
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                Ok(FfmpegExit::Clean(status))
            }
            _ => {
                let _ = self.child.kill().await;
                let status = self.child.wait().await?;
                self.exit_status = Some(status);
                Ok(FfmpegExit::Killed(status))
            }
        }
    }

    pub async fn kill(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Some(status) = self.exit_status {
            return Ok(status);
        }
        let _ = self.stdin.take();
        let _ = self.child.kill().await;
        let status = self.child.wait().await?;
        self.exit_status = Some(status);
        Ok(status)
    }

    pub async fn recv_event(&mut self) -> Option<FfmpegEvent> {
        if self.has_yielded_exited {
            return None;
        }
        if let Ok(event) = self.event_rx.try_recv() {
            return Some(event);
        }
        if let Some(status) = self.exit_status {
            if let Some(event) = self.event_rx.recv().await {
                return Some(event);
            }
            self.has_yielded_exited = true;
            return Some(FfmpegEvent::Exited(status));
        }

        tokio::select! {
            biased;
            event = self.event_rx.recv() => {
                match event {
                    Some(ev) => Some(ev),
                    None => {
                        let status = self.child.wait().await.ok()?;
                        self.exit_status = Some(status);
                        self.has_yielded_exited = true;
                        Some(FfmpegEvent::Exited(status))
                    }
                }
            }
            status_res = self.child.wait() => {
                let status = status_res.ok()?;
                self.exit_status = Some(status);
                tokio::select! {
                    biased;
                    event = self.event_rx.recv() => {
                        match event {
                            Some(ev) => Some(ev),
                            None => {
                                self.has_yielded_exited = true;
                                Some(FfmpegEvent::Exited(status))
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {
                        self.has_yielded_exited = true;
                        Some(FfmpegEvent::Exited(status))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename_basic() {
        let input = "a/b\\c:d*e?f\"g<h>i|j";
        let output = sanitize_filename(input);
        assert_eq!(output, "a_b_c_d_e_f_g_h_i_j");
    }

    #[test]
    fn test_sanitize_filename_replaces_question_marks() {
        let input = "Title? With questions? Yes!";
        let output = sanitize_filename(input);
        assert_eq!(output, "Title_ With questions_ Yes!");
    }

    #[test]
    fn test_sanitize_filename_trim() {
        let input = "   hello world   ";
        let output = sanitize_filename(input);
        assert_eq!(output, "hello world");
    }

    #[test]
    fn test_sanitize_filename_control_characters() {
        let input = "title\x00with\x1fcontrol\x07chars";
        let output = sanitize_filename(input);
        assert_eq!(output, "title_with_control_chars");
    }

    #[test]
    fn test_build_ffmpeg_command_args() {
        let path = Path::new("recordings/test/%04d.ts");
        let cmd = build_ffmpeg_command("http://example.com/live.m3u8", path, 10, None);
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|s| s.to_string_lossy().to_string())
            .collect();

        assert_eq!(args[0], "-hide_banner");
        assert_eq!(args[1], "-loglevel");
        assert_eq!(args[2], "warning");
        assert_eq!(args[3], "-y");
        assert_eq!(args[4], "-headers");
        assert!(args[5].contains("User-Agent:"));
        assert!(!args[5].contains("Cookie:"));
        assert_eq!(args[6], "-extension_picky");
        assert_eq!(args[7], "0");
        assert!(!args.iter().any(|a| a.starts_with("-reconnect")));
        assert_eq!(args[8], "-i");
        assert_eq!(args[9], "http://example.com/live.m3u8");
        assert_eq!(args[10], "-c");
        assert_eq!(args[11], "copy");
        assert_eq!(args[12], "-f");
        assert_eq!(args[13], "segment");
        assert_eq!(args[14], "-segment_time");
        assert_eq!(args[15], "10");
        assert_eq!(args[16], "-segment_format");
        assert_eq!(args[17], "mpegts");
        assert_eq!(args[18], "-reset_timestamps");
        assert_eq!(args[19], "1");
    }

    #[test]
    fn test_build_ffmpeg_command_default_program() {
        let path = Path::new("recordings/test/%04d.ts");
        let cmd = build_ffmpeg_command("http://example.com/live.m3u8", path, 10, None);
        let std_cmd = cmd.as_std();
        let expected_bin =
            std::env::var("CHZZK_LOAD_FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());
        assert_eq!(std_cmd.get_program(), expected_bin.as_str());
    }

    #[test]
    fn test_is_ffmpeg_key_forbidden_error() {
        assert!(is_ffmpeg_key_forbidden_error(
            "[crypto @ 0000021c321d2680] Unable to open key file https://example.com/aes_key"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[crypto @ 0000021c321d2680] unable to open key file https://example.com/aes_key"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "\x1b[31m[crypto @ 0000021c321d2680] Unable to open key file https://example.com/aes_key\x1b[0m"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[https @ 0000021c321d3340] HTTP error 403 Forbidden for https://example.com/aes_key"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[https @ 0000021c321d3340] http error 403 forbidden for https://example.com/aes_key"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[https @ 0000021c321d3340] HTTP error 403 Forbidden while reading key file"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[crypto @ 0000021c321d2680] aes_key request failed: access denied"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[crypto @ 0000021c321d2680] aes_key: Permission Denied"
        ));
        assert!(is_ffmpeg_key_forbidden_error(
            "[crypto @ 0000021c321d2680] aes_key request returned forbidden"
        ));
        assert!(!is_ffmpeg_key_forbidden_error(
            "[hls @ 0000021c321d1200] Opening 'chunk_0001.ts' for reading"
        ));
        assert!(!is_ffmpeg_key_forbidden_error(
            "[https @ 0000021c321d3340] HTTP error 403 Forbidden for https://example.com/video_0001.m4v"
        ));
    }
}
