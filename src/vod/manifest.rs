use anyhow::{Result, bail};

/// Representation of an HLS variant stream extracted from a master playlist.
#[derive(Debug, Clone, PartialEq)]
pub struct HlsVariant {
    pub url: String,
    pub bandwidth: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub name: Option<String>,
}

/// Representation of an individual media segment (.ts) extracted from a variant playlist.
#[derive(Debug, Clone, PartialEq)]
pub struct VodSegment {
    pub index: usize,
    pub filename: String,
    pub url: String,
    pub duration: f64,
}

/// Resolves a relative URL against a base URL.
pub fn resolve_url(base: &str, relative: &str) -> String {
    let rel = relative.trim();
    if rel.starts_with("http://") || rel.starts_with("https://") {
        return rel.to_string();
    }

    if rel.starts_with('/') {
        // Root-relative path
        if let Some(scheme_end) = base.find("://") {
            let host_part = &base[scheme_end + 3..];
            if let Some(path_start) = host_part.find('/') {
                let host_root = &base[..scheme_end + 3 + path_start];
                return format!("{host_root}{rel}");
            }
            return format!("{base}{rel}");
        }
    }

    if let Some(idx) = base.rfind('/') {
        let base_dir = &base[..=idx];
        format!("{base_dir}{rel}")
    } else {
        rel.to_string()
    }
}

/// Parses an HLS master playlist (`#EXT-X-STREAM-INF`) and extracts all available variants.
pub fn parse_master_playlist(content: &str, base_url: &str) -> Result<Vec<HlsVariant>> {
    let mut variants = Vec::new();
    let mut pending_bandwidth: Option<u64> = None;
    let mut pending_width: Option<u32> = None;
    let mut pending_height: Option<u32> = None;
    let mut pending_name: Option<String> = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(stream_inf) = trimmed.strip_prefix("#EXT-X-STREAM-INF:") {
            // Parse attributes: e.g. BANDWIDTH=8000000,RESOLUTION=1920x1080,NAME="1080p"
            pending_bandwidth = None;
            pending_width = None;
            pending_height = None;
            pending_name = None;

            for attr in stream_inf.split(',') {
                let attr = attr.trim();
                if let Some((k, v)) = attr.split_once('=') {
                    let key = k.trim();
                    let val = v.trim().trim_matches('"');
                    match key {
                        "BANDWIDTH" => {
                            pending_bandwidth = val.parse::<u64>().ok();
                        }
                        "RESOLUTION" => {
                            if let Some((w, h)) = val.split_once('x') {
                                pending_width = w.trim().parse::<u32>().ok();
                                pending_height = h.trim().parse::<u32>().ok();
                            }
                        }
                        "NAME" => {
                            pending_name = Some(val.to_string());
                        }
                        _ => {}
                    }
                }
            }
        } else if !trimmed.starts_with('#') {
            // This is a stream URI line following #EXT-X-STREAM-INF
            let url = resolve_url(base_url, trimmed);
            variants.push(HlsVariant {
                url,
                bandwidth: pending_bandwidth.unwrap_or(0),
                width: pending_width,
                height: pending_height,
                name: pending_name.take(),
            });
            pending_bandwidth = None;
            pending_width = None;
            pending_height = None;
        }
    }

    Ok(variants)
}

/// Selects the best matching variant from a list of HLS variants based on the requested quality.
///
/// Supported quality specifications:
/// - `"best"`: highest resolution and bandwidth (default)
/// - `"worst"`: lowest resolution and bandwidth
/// - `"1080p"` / `"1080"`: 1080p resolution (height 1080)
/// - `"720p"` / `"720"`: 720p resolution (height 720)
/// - `"480p"` / `"480"`: 480p resolution (height 480)
/// - `"360p"` / `"360"`: 360p resolution (height 360)
pub fn select_variant<'a>(
    variants: &'a [HlsVariant],
    requested_quality: &str,
) -> Result<&'a HlsVariant> {
    if variants.is_empty() {
        bail!("Master playlist contains no media variant streams");
    }

    let quality = requested_quality.trim().to_ascii_lowercase();

    if quality == "worst" {
        let mut sorted: Vec<&HlsVariant> = variants.iter().collect();
        sorted.sort_by_key(|v| (v.height.unwrap_or(0), v.bandwidth));
        return Ok(sorted[0]);
    }

    if quality == "best" {
        let mut sorted: Vec<&HlsVariant> = variants.iter().collect();
        sorted.sort_by_key(|v| (v.height.unwrap_or(0), v.bandwidth));
        return Ok(sorted.last().unwrap());
    }

    // Try matching specific resolution (e.g. "1080p" -> 1080)
    let target_height: Option<u32> = quality
        .strip_suffix('p')
        .unwrap_or(&quality)
        .parse::<u32>()
        .ok();

    if let Some(target_h) = target_height {
        // Look for exact height match
        if let Some(matched) = variants.iter().find(|v| v.height == Some(target_h)) {
            return Ok(matched);
        }

        // Look for name match
        let quality_p = format!("{target_h}p");
        if let Some(matched) = variants.iter().find(|v| {
            v.name.as_deref().is_some_and(|n| {
                n.eq_ignore_ascii_case(&quality) || n.eq_ignore_ascii_case(&quality_p)
            })
        }) {
            return Ok(matched);
        }

        // Fallback: pick the closest resolution <= target_height, or highest available
        let mut matching: Vec<&HlsVariant> = variants
            .iter()
            .filter(|v| v.height.is_some_and(|h| h <= target_h))
            .collect();

        if !matching.is_empty() {
            matching.sort_by_key(|v| (v.height.unwrap_or(0), v.bandwidth));
            return Ok(matching.last().unwrap());
        }

        bail!("No matching variant found for requested quality '{requested_quality}'");
    }

    bail!(
        "Unrecognized quality specification '{requested_quality}': expected 'best', 'worst', or resolution like '1080p', '720p', '480p'"
    );
}

/// Parses an HLS variant playlist (`#EXTINF`) and extracts all video segments (.ts).
pub fn parse_variant_playlist(content: &str, variant_url: &str) -> Result<Vec<VodSegment>> {
    let mut segments = Vec::new();
    let mut pending_duration: Option<f64> = None;
    let mut segment_index = 0usize;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(extinf) = trimmed.strip_prefix("#EXTINF:") {
            let dur_str = extinf.split(',').next().unwrap_or("").trim();
            pending_duration = dur_str.parse::<f64>().ok();
        } else if !trimmed.starts_with('#') {
            let url = resolve_url(variant_url, trimmed);
            let filename = format!("segment_{segment_index:05}.ts");
            let duration = pending_duration.take().unwrap_or(0.0);

            segments.push(VodSegment {
                index: segment_index,
                filename,
                url,
                duration,
            });
            segment_index += 1;
        }
    }

    if segments.is_empty() {
        bail!("Variant playlist contains 0 video segments");
    }

    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_MASTER_M3U8: &str = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=1920x1080,FRAME-RATE=60.000,NAME="1080p"
1080p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1280x720,FRAME-RATE=60.000,NAME="720p"
720p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=854x480,FRAME-RATE=30.000,NAME="480p"
480p/index.m3u8
"#;

    const SAMPLE_VARIANT_M3U8: &str = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:6.000,
chunk_0000.ts?token=abc
#EXTINF:6.000,
chunk_0001.ts?token=abc
#EXTINF:4.500,
chunk_0002.ts?token=abc
#EXT-X-ENDLIST
"#;

    #[test]
    fn test_parse_master_playlist() {
        let variants = parse_master_playlist(
            SAMPLE_MASTER_M3U8,
            "https://cdn.example.com/vod/master.m3u8",
        )
        .unwrap();
        assert_eq!(variants.len(), 3);
        assert_eq!(variants[0].bandwidth, 8000000);
        assert_eq!(variants[0].width, Some(1920));
        assert_eq!(variants[0].height, Some(1080));
        assert_eq!(
            variants[0].url,
            "https://cdn.example.com/vod/1080p/index.m3u8"
        );
    }

    #[test]
    fn test_select_variant_quality() {
        let variants = parse_master_playlist(
            SAMPLE_MASTER_M3U8,
            "https://cdn.example.com/vod/master.m3u8",
        )
        .unwrap();

        let best = select_variant(&variants, "best").unwrap();
        assert_eq!(best.height, Some(1080));

        let v720 = select_variant(&variants, "720p").unwrap();
        assert_eq!(v720.height, Some(720));

        let worst = select_variant(&variants, "worst").unwrap();
        assert_eq!(worst.height, Some(480));

        assert!(select_variant(&variants, "invalid_quality").is_err());
    }

    #[test]
    fn test_parse_variant_playlist() {
        let segments = parse_variant_playlist(
            SAMPLE_VARIANT_M3U8,
            "https://cdn.example.com/vod/1080p/index.m3u8",
        )
        .unwrap();

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].filename, "segment_00000.ts");
        assert_eq!(
            segments[0].url,
            "https://cdn.example.com/vod/1080p/chunk_0000.ts?token=abc"
        );
        assert_eq!(segments[0].duration, 6.0);
        assert_eq!(segments[2].filename, "segment_00002.ts");
        assert_eq!(segments[2].duration, 4.5);
    }
}
