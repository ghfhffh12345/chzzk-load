use std::path::Path;
use tokio::process::Command;

pub fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim();
    let mut result = String::with_capacity(trimmed.len());
    for c in trimmed.chars() {
        match c {
            '\\' | '/' | ':' | '*' | '"' | '<' | '>' | '|' => result.push('_'),
            c if c.is_control() => result.push('_'),
            other => result.push(other),
        }
    }
    result
}

pub fn build_ffmpeg_command(
    m3u8_url: &str,
    output_pattern: &Path,
    chunk_duration_seconds: u64,
    cookie_header: Option<&str>,
) -> Command {
    let ffmpeg_bin =
        std::env::var("CHZZK_LOAD_FFMPEG_BIN").unwrap_or_else(|_| "ffmpeg".to_string());
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
    cmd.arg("-reconnect").arg("1");
    cmd.arg("-reconnect_at_eof").arg("1");
    cmd.arg("-reconnect_streamed").arg("1");
    cmd.arg("-reconnect_delay_max").arg("10");

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
        assert_eq!(output, "a_b_c_d_e?f_g_h_i_j");
    }

    #[test]
    fn test_sanitize_filename_preserves_question_marks() {
        let input = "Title? With questions? Yes!";
        let output = sanitize_filename(input);
        assert_eq!(output, "Title? With questions? Yes!");
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
        assert_eq!(args[8], "-reconnect");
        assert_eq!(args[9], "1");
        assert_eq!(args[10], "-reconnect_at_eof");
        assert_eq!(args[11], "1");
        assert_eq!(args[12], "-reconnect_streamed");
        assert_eq!(args[13], "1");
        assert_eq!(args[14], "-reconnect_delay_max");
        assert_eq!(args[15], "10");
        assert_eq!(args[16], "-i");
        assert_eq!(args[17], "http://example.com/live.m3u8");
        assert_eq!(args[18], "-c");
        assert_eq!(args[19], "copy");
        assert_eq!(args[20], "-f");
        assert_eq!(args[21], "segment");
        assert_eq!(args[22], "-segment_time");
        assert_eq!(args[23], "10");
        assert_eq!(args[24], "-segment_format");
        assert_eq!(args[25], "mpegts");
        assert_eq!(args[26], "-reset_timestamps");
        assert_eq!(args[27], "1");
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
}
