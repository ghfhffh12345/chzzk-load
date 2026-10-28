use std::path::{Path, PathBuf};

use crate::consolidation::manifest::TargetLocation;

/// Computes a deterministic 64-bit FNV-1a hash of a target string, returning a 16-hex-digit string.
pub fn compute_target_hash(target_str: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in target_str.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// Generates the isolated subfolder name for temporary staging: `chzzk-load-consolidation-<hash>`.
pub fn staging_folder_name(target_str: &str) -> String {
    format!(
        "chzzk-load-consolidation-{}",
        compute_target_hash(target_str)
    )
}

/// Resolves the staging workspace directory for post-recording consolidation.
///
/// If `custom_temp_dir` is provided, resolves directly to that path.
/// Otherwise, defaults to an isolated deterministic directory under the system temporary path:
/// `std::env::temp_dir() / chzzk-load-consolidation-<hash_of_target>`.
pub fn resolve_staging_dir<P: AsRef<Path>>(
    target: &TargetLocation,
    custom_temp_dir: Option<P>,
) -> PathBuf {
    if let Some(custom) = custom_temp_dir {
        custom.as_ref().to_path_buf()
    } else {
        std::env::temp_dir().join(staging_folder_name(target.raw()))
    }
}

/// Required disk space headroom multiplier (1.15x) for post-recording consolidation.
pub const STAGING_HEADROOM_MULTIPLIER: f64 = 1.15;

/// Calculates the required staging disk space in bytes for a video stream of `total_video_bytes`.
///
/// Applies the 1.15x headroom multiplier, rounding up to the nearest byte.
pub fn calculate_required_staging_bytes(total_video_bytes: u64) -> u64 {
    (total_video_bytes as f64 * STAGING_HEADROOM_MULTIPLIER).ceil() as u64
}

/// Verifies whether the provided `available_bytes` meets or exceeds 1.15x `total_video_bytes`
/// for staging files at `staging_dir`.
///
/// Emits an actionable diagnostic error detailing required vs. available bytes and recommending
/// `--temp-dir <PATH>` when space is insufficient.
pub fn verify_staging_capacity_with_space(
    staging_dir: &Path,
    available_bytes: u64,
    total_video_bytes: u64,
) -> anyhow::Result<()> {
    let required_bytes = calculate_required_staging_bytes(total_video_bytes);
    if available_bytes < required_bytes {
        let req_gb = required_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let avail_gb = available_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let vid_gb = total_video_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        anyhow::bail!(
            "Insufficient disk space for staging consolidation at '{}': \
             required {:.2} GB ({} bytes, 1.15x of {:.2} GB video stream), \
             available {:.2} GB ({} bytes). \
             Specify a secondary drive with adequate free space using '--temp-dir <PATH>'.",
            staging_dir.display(),
            req_gb,
            required_bytes,
            vid_gb,
            avail_gb,
            available_bytes
        );
    }
    Ok(())
}

/// Pre-flight disk capacity verification that queries available filesystem bytes at the staging
/// location against 1.15x total video bytes discovered in the manifest.
///
/// If available space is insufficient, aborts with an actionable diagnostic error.
pub fn verify_staging_capacity(staging_dir: &Path, total_video_bytes: u64) -> anyhow::Result<()> {
    let disk_space = crate::disk::get_disk_space(staging_dir).map_err(|e| {
        anyhow::anyhow!(
            "Failed to query available disk space at staging path '{}': {e}",
            staging_dir.display()
        )
    })?;
    verify_staging_capacity_with_space(staging_dir, disk_space.available_bytes, total_video_bytes)
}

/// Recursively removes a directory with bounded exponential backoff retries handling
/// transient Windows sharing violations (32), access denied (5), and pending unlink latency (145),
/// treating `ErrorKind::NotFound` as success.
pub async fn remove_dir_all_with_retry(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut remove_res = tokio::fs::remove_dir_all(path).await;
    let mut attempts = 0;
    while let Err(ref e) = remove_res {
        if attempts >= 10 || e.kind() == std::io::ErrorKind::NotFound {
            break;
        }
        let raw_os = e.raw_os_error();
        let is_transient_lock = raw_os == Some(145) // ERROR_DIR_NOT_EMPTY
            || raw_os == Some(32) // ERROR_SHARING_VIOLATION
            || raw_os == Some(5)  // ERROR_ACCESS_DENIED
            || e.kind() == std::io::ErrorKind::PermissionDenied;

        if is_transient_lock {
            attempts += 1;
            tokio::time::sleep(std::time::Duration::from_millis(std::cmp::min(
                200,
                20 * (1 << (attempts - 1)),
            )))
            .await;
            remove_res = tokio::fs::remove_dir_all(path).await;
        } else {
            break;
        }
    }

    match remove_res {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Structural metadata extracted during MP4 container integrity verification.
#[derive(Debug, Clone, PartialEq)]
pub struct Mp4ContainerInfo {
    pub file_size: u64,
    pub moov_offset: u64,
    pub moov_size: u64,
    pub mdat_offset: u64,
    pub mdat_size: u64,
    pub is_faststart: bool,
    pub track_chunk_offsets: Vec<u64>,
    pub duration_seconds: Option<f64>,
}

fn parse_mvhd_duration<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    payload_start: u64,
    payload_end: u64,
) -> Option<f64> {
    use std::io::SeekFrom;

    if payload_end.saturating_sub(payload_start) < 20 {
        return None;
    }
    if reader.seek(SeekFrom::Start(payload_start)).is_err() {
        return None;
    }
    let mut buf = [0u8; 32];
    if reader.read_exact(&mut buf).is_err() {
        return None;
    }
    let version = buf[0];
    if version == 1 {
        let timescale = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
        let duration = u64::from_be_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);
        if timescale > 0 {
            Some(duration as f64 / timescale as f64)
        } else {
            None
        }
    } else if version == 0 {
        let timescale = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let duration = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
        if timescale > 0 {
            Some(duration as f64 / timescale as f64)
        } else {
            None
        }
    } else {
        None
    }
}

fn parse_chunk_offsets<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    payload_start: u64,
    payload_end: u64,
    is_64bit: bool,
    chunk_offsets: &mut Vec<u64>,
) {
    use std::io::SeekFrom;

    if payload_end.saturating_sub(payload_start) < 8 {
        return;
    }
    if reader.seek(SeekFrom::Start(payload_start)).is_err() {
        return;
    }
    let mut header = [0u8; 8];
    if reader.read_exact(&mut header).is_err() {
        return;
    }
    let entry_count = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    if entry_count == 0 {
        return;
    }

    if is_64bit {
        if payload_end.saturating_sub(payload_start) >= 16 {
            let mut offset_buf = [0u8; 8];
            if reader.read_exact(&mut offset_buf).is_ok() {
                chunk_offsets.push(u64::from_be_bytes(offset_buf));
            }
        }
    } else if payload_end.saturating_sub(payload_start) >= 12 {
        let mut offset_buf = [0u8; 4];
        if reader.read_exact(&mut offset_buf).is_ok() {
            chunk_offsets.push(u32::from_be_bytes(offset_buf) as u64);
        }
    }
}

/// Recursively scans container boxes within `moov` (`trak`, `mdia`, `minf`, `stbl`)
/// extracting sample chunk offsets from `stco` (32-bit) or `co64` (64-bit) boxes,
/// and parses duration from `mvhd`.
fn scan_moov_chunk_offsets<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
    chunk_offsets: &mut Vec<u64>,
    duration_seconds: &mut Option<f64>,
) {
    use std::io::SeekFrom;

    let mut offset = start;
    let mut header = [0u8; 8];

    while offset.saturating_add(8) <= end {
        if reader.seek(SeekFrom::Start(offset)).is_err() {
            break;
        }
        if reader.read_exact(&mut header).is_err() {
            break;
        }

        let size_u32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        let (header_size, box_size) = if size_u32 == 1 {
            let mut ext = [0u8; 8];
            if reader.read_exact(&mut ext).is_err() {
                break;
            }
            let ext_size = u64::from_be_bytes(ext);
            if ext_size < 16 {
                break;
            }
            (16u64, ext_size)
        } else if size_u32 == 0 {
            (8u64, end.saturating_sub(offset))
        } else if size_u32 < 8 {
            break;
        } else {
            (8u64, size_u32 as u64)
        };

        let next_offset = match offset.checked_add(box_size) {
            Some(n) if n <= end => n,
            _ => break,
        };

        let payload_start = offset + header_size;
        let payload_end = next_offset;

        match box_type {
            b"mvhd" => {
                if duration_seconds.is_none() {
                    *duration_seconds = parse_mvhd_duration(reader, payload_start, payload_end);
                }
            }
            b"trak" | b"mdia" | b"minf" | b"stbl" => {
                scan_moov_chunk_offsets(
                    reader,
                    payload_start,
                    payload_end,
                    chunk_offsets,
                    duration_seconds,
                );
            }
            b"stco" => {
                parse_chunk_offsets(reader, payload_start, payload_end, false, chunk_offsets);
            }
            b"co64" => {
                parse_chunk_offsets(reader, payload_start, payload_end, true, chunk_offsets);
            }
            _ => {}
        }

        offset = next_offset;
    }
}

/// Validates the structural integrity of an MP4 container file:
/// 1. Verifies that top-level boxes cleanly tile the entire file with zero trailing unindexed bytes.
/// 2. Ensures exactly one `moov` atom header is present (rejects files with duplicate `moov` boxes).
/// 3. Ensures at least one `mdat` atom is present with valid payload boundaries.
/// 4. Inspects sample chunk offsets (`stco`/`co64`) inside `moov`, ensuring they point directly
///    within the `mdat` media payload range rather than header metadata or unaligned offsets.
pub fn validate_mp4_container_integrity(path: &Path) -> anyhow::Result<Mp4ContainerInfo> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open MP4 file '{}': {e}", path.display()))?;

    let file_len = file
        .metadata()
        .map_err(|e| anyhow::anyhow!("Failed to read metadata for '{}': {e}", path.display()))?
        .len();

    if file_len < 16 {
        anyhow::bail!(
            "File '{}' is too small to be a valid MP4 container ({} bytes)",
            path.display(),
            file_len
        );
    }

    let mut offset: u64 = 0;
    let mut header = [0u8; 8];

    let mut moov_info: Option<(u64, u64, u64)> = None; // (offset, header_size, size)
    let mut moov_count: usize = 0;
    let mut mdat_ranges: Vec<(u64, u64)> = Vec::new(); // [(payload_start, payload_end)]
    let mut first_mdat_info: Option<(u64, u64)> = None; // (offset, size)

    while offset.saturating_add(8) <= file_len {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut header)?;

        let size_u32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        let (header_size, box_size) = if size_u32 == 1 {
            let mut ext = [0u8; 8];
            file.read_exact(&mut ext)?;
            let ext_size = u64::from_be_bytes(ext);
            if ext_size < 16 {
                anyhow::bail!(
                    "Invalid 64-bit box size {ext_size} for atom '{:?}' at offset {offset}",
                    String::from_utf8_lossy(box_type)
                );
            }
            (16u64, ext_size)
        } else if size_u32 == 0 {
            // Box extends to end of file
            (8u64, file_len.saturating_sub(offset))
        } else if size_u32 < 8 {
            anyhow::bail!(
                "Invalid 32-bit box size {size_u32} for atom '{:?}' at offset {offset}",
                String::from_utf8_lossy(box_type)
            );
        } else {
            (8u64, size_u32 as u64)
        };

        let next_offset = match offset.checked_add(box_size) {
            Some(n) if n <= file_len => n,
            _ => anyhow::bail!(
                "Atom '{:?}' at offset {offset} with size {box_size} exceeds file length {file_len}",
                String::from_utf8_lossy(box_type)
            ),
        };

        if box_type == b"moov" {
            moov_count += 1;
            if moov_count > 1 {
                anyhow::bail!(
                    "Corrupted MP4 container in '{}': duplicate 'moov' atom detected at offset {offset} (previous at offset {})",
                    path.display(),
                    moov_info.map(|(o, _, _)| o).unwrap_or(0)
                );
            }
            if box_size < 16 {
                anyhow::bail!(
                    "Corrupted MP4 container in '{}': 'moov' atom at offset {offset} is too small ({box_size} bytes)",
                    path.display()
                );
            }
            moov_info = Some((offset, header_size, box_size));
        } else if box_type == b"mdat" {
            if first_mdat_info.is_none() {
                first_mdat_info = Some((offset, box_size));
            }
            let payload_start = offset + header_size;
            let payload_end = next_offset;
            mdat_ranges.push((payload_start, payload_end));
        }

        offset = next_offset;
    }

    if offset != file_len {
        anyhow::bail!(
            "Corrupted MP4 container in '{}': trailing unindexed data ({} bytes unparsed at end of file, offset {offset} vs file length {file_len})",
            path.display(),
            file_len - offset
        );
    }

    let (moov_offset, moov_hdr_size, moov_size) = moov_info.ok_or_else(|| {
        anyhow::anyhow!(
            "Corrupted MP4 container in '{}': missing required 'moov' atom",
            path.display()
        )
    })?;

    let (mdat_offset, mdat_size) = first_mdat_info.ok_or_else(|| {
        anyhow::anyhow!(
            "Corrupted MP4 container in '{}': missing required 'mdat' atom",
            path.display()
        )
    })?;

    // Scan chunk offsets in moov and verify they point inside mdat payload ranges
    let mut chunk_offsets = Vec::new();
    let mut duration_seconds = None;
    scan_moov_chunk_offsets(
        &mut file,
        moov_offset + moov_hdr_size,
        moov_offset + moov_size,
        &mut chunk_offsets,
        &mut duration_seconds,
    );

    for &chunk_offset in &chunk_offsets {
        let inside_any_mdat = mdat_ranges
            .iter()
            .any(|&(start, end)| chunk_offset >= start && chunk_offset < end);
        if !inside_any_mdat {
            anyhow::bail!(
                "Corrupted MP4 container in '{}': sample chunk offset {chunk_offset} does not fall within any 'mdat' media payload range",
                path.display()
            );
        }
    }

    let is_faststart = moov_offset < mdat_offset;

    Ok(Mp4ContainerInfo {
        file_size: file_len,
        moov_offset,
        moov_size,
        mdat_offset,
        mdat_size,
        is_faststart,
        track_chunk_offsets: chunk_offsets,
        duration_seconds,
    })
}

/// Inspects an MP4 file on disk for container sanity by validating all container integrity invariants.
///
/// Returns `true` if the file exists, has non-zero size, and passes full container integrity verification.
pub fn probe_moov_atom(path: &Path) -> bool {
    validate_mp4_container_integrity(path).is_ok()
}

/// Validates the staging workspace media files (`live_stream.mp4` and `live_chat.jsonl`).
///
/// If `has_video` is true, `live_stream.mp4` must exist, be non-empty, and pass full MP4 container integrity verification.
/// If `has_chat` is true, `live_chat.jsonl` must exist and be non-empty (>0 bytes).
/// If neither video nor chat is requested, returns an error.
pub fn validate_staging_workspace(
    staging_dir: &Path,
    has_video: bool,
    has_chat: bool,
) -> anyhow::Result<()> {
    if !has_video && !has_chat {
        anyhow::bail!(
            "No media selected for staging workspace validation in '{}'",
            staging_dir.display()
        );
    }

    if has_video {
        let mp4_path = staging_dir.join("live_stream.mp4");
        if !mp4_path.exists() {
            anyhow::bail!(
                "Staging workspace video file does not exist: '{}'",
                mp4_path.display()
            );
        }
        validate_mp4_container_integrity(&mp4_path)?;
    }

    if has_chat {
        let chat_path = staging_dir.join("live_chat.jsonl");
        let meta = std::fs::metadata(&chat_path).map_err(|e| {
            anyhow::anyhow!(
                "Staging workspace chat file does not exist or cannot be read at '{}': {e}",
                chat_path.display()
            )
        })?;
        if meta.len() == 0 {
            anyhow::bail!(
                "Staging workspace chat file is empty (0 bytes): '{}'",
                chat_path.display()
            );
        }
    }

    Ok(())
}

/// Checks whether existing staging files in `staging_dir` satisfy all integrity criteria
/// to resume cloud upload directly without re-executing FFmpeg or chat deduplication.
///
/// Supports canonical snake-case identifiers (`live_stream.mp4`, `live_chat.jsonl`)
/// and backwards-compatible legacy identifiers (`livestream.mp4`, `livechat.jsonl`).
pub fn check_staging_resume_readiness(staging_dir: &Path, has_video: bool, has_chat: bool) -> bool {
    if !has_video && !has_chat {
        return false;
    }

    if has_video {
        let canonical_mp4 = staging_dir.join("live_stream.mp4");
        let legacy_mp4 = staging_dir.join("livestream.mp4");
        let valid = (canonical_mp4.exists() && probe_moov_atom(&canonical_mp4))
            || (legacy_mp4.exists() && probe_moov_atom(&legacy_mp4));
        if !valid {
            return false;
        }
    }

    if has_chat {
        let canonical_chat = staging_dir.join("live_chat.jsonl");
        let legacy_chat = staging_dir.join("livechat.jsonl");
        let valid = matches!(std::fs::metadata(&canonical_chat), Ok(m) if m.len() > 0)
            || matches!(std::fs::metadata(&legacy_chat), Ok(m) if m.len() > 0);
        if !valid {
            return false;
        }
    }

    true
}

/// Purges temporary staged media part files (`live_stream.mp4.part` and `live_chat.jsonl.part`)
/// from `staging_dir` using retry-backed unlink operations to absorb Windows file locking latencies.
pub async fn cleanup_staged_media(staging_dir: &Path) -> std::io::Result<()> {
    let part_files = [
        "live_stream.mp4.part",
        "live_chat.jsonl.part",
        "livestream.mp4.part",
        "livechat.jsonl.part",
    ];
    for file in part_files {
        let p = staging_dir.join(file);
        if let Err(e) = crate::uploader::unlink_local_file_with_retry(&p).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Invalidates and removes any existing consolidation staging files in `staging_dir`
/// (`live_stream.mp4`, `live_chat.jsonl`, `live_stream.mp4.part`, `live_chat.jsonl.part`,
/// as well as legacy `livestream.mp4`, `livechat.jsonl`, `livestream.mp4.part`, `livechat.jsonl.part`)
/// using retry-backed unlink operations to absorb Windows file locking latencies.
pub async fn invalidate_staging_files(staging_dir: &Path) -> std::io::Result<()> {
    let files = [
        "live_stream.mp4",
        "live_chat.jsonl",
        "live_stream.mp4.part",
        "live_chat.jsonl.part",
        "livestream.mp4",
        "livechat.jsonl",
        "livestream.mp4.part",
        "livechat.jsonl.part",
    ];
    for file in files {
        let p = staging_dir.join(file);
        if let Err(e) = crate::uploader::unlink_local_file_with_retry(&p).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e);
            }
        }
    }
    Ok(())
}
