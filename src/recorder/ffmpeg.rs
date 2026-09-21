use std::path::Path;
use tokio::process::Command;

pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            other => other,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

pub fn build_ffmpeg_command(
    m3u8_url: &str,
    output_pattern: &Path,
    chunk_duration_seconds: u64,
    cookie_header: Option<&str>,
) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("warning")
        .arg("-y");

    let mut headers =
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n".to_string();
    if let Some(cookie) = cookie_header {
        headers.push_str(&format!("Cookie: {}\r\n", cookie));
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
}
