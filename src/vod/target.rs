use anyhow::{Context, Result, bail};

/// Parses a video target string (either a full Chzzk video URL or a numeric video ID)
/// into a numeric video identifier.
///
/// Supported input forms:
/// - Pure numeric ID: `"4531238"`
/// - Full URL: `"https://chzzk.naver.com/video/4531238"`
/// - HTTP URL: `"http://chzzk.naver.com/video/4531238"`
/// - Mobile URL: `"https://m.chzzk.naver.com/video/4531238"`
/// - URL with query params: `"https://chzzk.naver.com/video/4531238?other=param"`
/// - URL with trailing slash: `"https://chzzk.naver.com/video/4531238/"`
/// - URL with fragment: `"https://chzzk.naver.com/video/4531238#t=100"`
pub fn parse_video_target(target: &str) -> Result<u64> {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        bail!("Video target cannot be empty: expected numeric video ID or Chzzk video URL");
    }

    // Direct numeric video ID
    if let Ok(id) = trimmed.parse::<u64>() {
        if id == 0 {
            bail!("Invalid video ID '0': expected positive numeric integer");
        }
        return Ok(id);
    }

    // URL path containing "/video/"
    let path = if let Some(idx) = trimmed.find("/video/") {
        &trimmed[idx + "/video/".len()..]
    } else {
        bail!(
            "Invalid video target '{trimmed}': expected numeric video ID (e.g. '123456') or Chzzk video URL (e.g. 'https://chzzk.naver.com/video/123456')"
        );
    };

    let id_str = path.split(['?', '/', '#', '&']).next().unwrap_or("").trim();

    if id_str.is_empty() {
        bail!(
            "Missing video ID in target '{trimmed}': expected positive numeric integer after '/video/'"
        );
    }

    let id = id_str.parse::<u64>().with_context(|| {
        format!(
            "Invalid video ID '{id_str}' in target '{trimmed}': expected positive numeric integer"
        )
    })?;

    if id == 0 {
        bail!("Invalid video ID '0': expected positive numeric integer");
    }

    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_numeric_id() {
        assert_eq!(parse_video_target("123456").unwrap(), 123456);
        assert_eq!(parse_video_target("  4531238  ").unwrap(), 4531238);
    }

    #[test]
    fn test_parse_full_url() {
        assert_eq!(
            parse_video_target("https://chzzk.naver.com/video/4531238").unwrap(),
            4531238
        );
        assert_eq!(
            parse_video_target("http://chzzk.naver.com/video/4531238/").unwrap(),
            4531238
        );
        assert_eq!(
            parse_video_target("https://m.chzzk.naver.com/video/4531238?list=1&index=2").unwrap(),
            4531238
        );
        assert_eq!(
            parse_video_target("chzzk.naver.com/video/4531238#comment").unwrap(),
            4531238
        );
    }

    #[test]
    fn test_parse_invalid_target_fails() {
        assert!(parse_video_target("").is_err());
        assert!(parse_video_target("   ").is_err());
        assert!(parse_video_target("0").is_err());
        assert!(parse_video_target("-123").is_err());
        assert!(parse_video_target("abc").is_err());
        assert!(parse_video_target("https://chzzk.naver.com/live/123456").is_err());
        assert!(parse_video_target("https://chzzk.naver.com/video/").is_err());
        assert!(parse_video_target("https://chzzk.naver.com/video/notanumber").is_err());
    }
}
