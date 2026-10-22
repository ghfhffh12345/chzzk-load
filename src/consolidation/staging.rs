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
