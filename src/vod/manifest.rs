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

/// Resolves a relative URL against a base URL, preserving query parameters
/// from the base URL if the relative URL does not provide its own.
pub fn resolve_url(base: &str, relative: &str) -> String {
    let rel = relative.trim();
    if rel.starts_with("http://") || rel.starts_with("https://") {
        return rel.to_string();
    }

    let (base_no_query, base_query) = match base.split_once('?') {
        Some((path, q)) => (path, Some(q)),
        None => (base, None),
    };

    let resolved_path = if rel.starts_with('/') {
        // Root-relative path
        if let Some(scheme_end) = base_no_query.find("://") {
            let host_part = &base_no_query[scheme_end + 3..];
            if let Some(path_start) = host_part.find('/') {
                let host_root = &base_no_query[..scheme_end + 3 + path_start];
                format!("{host_root}{rel}")
            } else {
                format!("{base_no_query}{rel}")
            }
        } else {
            rel.to_string()
        }
    } else if let Some(idx) = base_no_query.rfind('/') {
        let base_dir = &base_no_query[..=idx];
        format!("{base_dir}{rel}")
    } else {
        rel.to_string()
    };

    if let Some(query) = base_query {
        if !rel.contains('?') && !resolved_path.contains('?') {
            return format!("{resolved_path}?{query}");
        }
    }

    resolved_path
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

/// Representation of an HLS variant playlist including optional fMP4 init segment and media segments.
#[derive(Debug, Clone, PartialEq)]
pub struct VodVariantManifest {
    pub init_segment: Option<VodSegment>,
    pub segments: Vec<VodSegment>,
}

/// Parses an HLS variant playlist (`#EXTINF` and optional `#EXT-X-MAP`) and extracts all video segments.
pub fn parse_variant_manifest(content: &str, variant_url: &str) -> Result<VodVariantManifest> {
    let mut segments = Vec::new();
    let mut init_segment = None;
    let mut pending_duration: Option<f64> = None;
    let mut segment_index = 0usize;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(map_attr) = trimmed.strip_prefix("#EXT-X-MAP:") {
            for attr in map_attr.split(',') {
                let attr = attr.trim();
                if let Some((k, v)) = attr.split_once('=') {
                    if k.trim().eq_ignore_ascii_case("URI") {
                        let uri_val = v.trim().trim_matches('"');
                        let url = resolve_url(variant_url, uri_val);
                        init_segment = Some(VodSegment {
                            index: 0,
                            filename: "init.mp4".to_string(),
                            url,
                            duration: 0.0,
                        });
                    }
                }
            }
        } else if let Some(extinf) = trimmed.strip_prefix("#EXTINF:") {
            let dur_str = extinf.split(',').next().unwrap_or("").trim();
            pending_duration = dur_str.parse::<f64>().ok();
        } else if !trimmed.starts_with('#') {
            let url = resolve_url(variant_url, trimmed);
            let is_fmp4 = init_segment.is_some()
                || trimmed.contains(".m4v")
                || trimmed.contains(".m4s")
                || trimmed.contains(".mp4");
            let ext = if is_fmp4 { "mp4" } else { "ts" };
            let filename = format!("segment_{segment_index:05}.{ext}");
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

    Ok(VodVariantManifest {
        init_segment,
        segments,
    })
}

/// Parses an HLS variant playlist (`#EXTINF`) and extracts all video segments (.ts).
pub fn parse_variant_playlist(content: &str, variant_url: &str) -> Result<Vec<VodSegment>> {
    let manifest = parse_variant_manifest(content, variant_url)?;
    Ok(manifest.segments)
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

    #[test]
    fn test_parse_variant_manifest_fmp4_with_map() {
        let content = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="720p_init.m4s?token=xyz"
#EXTINF:4.166667,
720p_chunk_00.m4v
#EXTINF:4.166667,
720p_chunk_01.m4v
#EXT-X-ENDLIST
"#;
        let manifest = parse_variant_manifest(
            content,
            "https://cdn.example.com/chzzk/720p/vod_chunklist.m3u8?token=xyz",
        )
        .unwrap();

        assert!(manifest.init_segment.is_some());
        let init = manifest.init_segment.unwrap();
        assert_eq!(init.filename, "init.mp4");
        assert_eq!(
            init.url,
            "https://cdn.example.com/chzzk/720p/720p_init.m4s?token=xyz"
        );
        assert_eq!(init.duration, 0.0);

        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(manifest.segments[0].filename, "segment_00000.mp4");
        assert_eq!(
            manifest.segments[0].url,
            "https://cdn.example.com/chzzk/720p/720p_chunk_00.m4v?token=xyz"
        );
        assert_eq!(manifest.segments[0].duration, 4.166667);
        assert_eq!(manifest.segments[1].filename, "segment_00001.mp4");
    }

    #[test]
    fn test_resolve_url_query_inheritance() {
        // Query param inherited on relative path without query
        assert_eq!(
            resolve_url(
                "https://cdn.example.com/hls/master.m3u8?token=123&env=real",
                "variant_1080.m3u8"
            ),
            "https://cdn.example.com/hls/variant_1080.m3u8?token=123&env=real"
        );

        // Segment URL inheriting token from variant URL
        assert_eq!(
            resolve_url(
                "https://cdn.example.com/hls/variant_1080.m3u8?token=123",
                "segment_00000.ts"
            ),
            "https://cdn.example.com/hls/segment_00000.ts?token=123"
        );

        // Relative path with its own query param does not get clobbered
        assert_eq!(
            resolve_url(
                "https://cdn.example.com/hls/master.m3u8?token=123",
                "variant.m3u8?custom_token=456"
            ),
            "https://cdn.example.com/hls/variant.m3u8?custom_token=456"
        );

        // Absolute URL remains unchanged
        assert_eq!(
            resolve_url(
                "https://cdn.example.com/hls/master.m3u8?token=123",
                "https://othercdn.example.com/seg.ts"
            ),
            "https://othercdn.example.com/seg.ts"
        );

        // Base without query param behaves normally
        assert_eq!(
            resolve_url(
                "https://cdn.example.com/hls/master.m3u8",
                "variant_720.m3u8"
            ),
            "https://cdn.example.com/hls/variant_720.m3u8"
        );
    }
}
