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

/// Inspects an MP4 file on disk for container sanity by scanning top-level boxes for a valid `moov` atom header.
///
/// Uses bounded seek operations without buffering media payloads into RAM, running in O(1) time
/// regardless of video container size. Returns `true` if a non-zero-sized file contains a valid `moov` box header.
pub fn probe_moov_atom(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    let file_len = match file.metadata() {
        Ok(m) => m.len(),
        Err(_) => return false,
    };

    if file_len < 8 {
        return false;
    }

    let mut offset: u64 = 0;
    let mut header = [0u8; 8];

    while offset.saturating_add(8) <= file_len {
        if file.seek(SeekFrom::Start(offset)).is_err() {
            return false;
        }
        if file.read_exact(&mut header).is_err() {
            return false;
        }

        let size_u32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        let box_size: u64 = if size_u32 == 1 {
            let mut ext_buf = [0u8; 8];
            if file.read_exact(&mut ext_buf).is_err() {
                return false;
            }
            let ext_size = u64::from_be_bytes(ext_buf);
            if ext_size < 16 {
                return false;
            }
            ext_size
        } else if size_u32 == 0 {
            file_len.saturating_sub(offset)
        } else if size_u32 < 8 {
            return false;
        } else {
            size_u32 as u64
        };

        if box_type == b"moov" {
            // A valid moov atom must have payload child boxes (at least 16 bytes total)
            return box_size >= 16 && offset.saturating_add(box_size) <= file_len;
        }

        match offset.checked_add(box_size) {
            Some(next_offset) => {
                if next_offset <= offset || next_offset > file_len {
                    return false;
                }
                offset = next_offset;
            }
            None => return false,
        }
    }

    false
}

/// Checks whether existing staging files in `staging_dir` satisfy all integrity criteria
/// to resume cloud upload directly without re-executing FFmpeg or chat deduplication.
///
/// If `has_video` is true, `stream.mp4` must exist with non-zero size and a valid `moov` atom header.
/// If `has_chat` is true, `chat.jsonl` must exist with non-zero size.
pub fn check_staging_resume_readiness(staging_dir: &Path, has_video: bool, has_chat: bool) -> bool {
    if !has_video && !has_chat {
        return false;
    }

    if has_video {
        let mp4_path = staging_dir.join("stream.mp4");
        if !probe_moov_atom(&mp4_path) {
            return false;
        }
    }

    if has_chat {
        let jsonl_path = staging_dir.join("chat.jsonl");
        match std::fs::metadata(&jsonl_path) {
            Ok(m) if m.len() > 0 => {}
            _ => return false,
        }
    }

    true
}

/// Invalidates and removes any existing consolidation staging files in `staging_dir`
/// (`stream.mp4`, `chat.jsonl`, `stream.mp4.part`, `chat.jsonl.part`)
/// using retry-backed unlink operations to absorb Windows file locking latencies.
pub async fn invalidate_staging_files(staging_dir: &Path) -> std::io::Result<()> {
    let files = [
        "stream.mp4",
        "chat.jsonl",
        "stream.mp4.part",
        "chat.jsonl.part",
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
