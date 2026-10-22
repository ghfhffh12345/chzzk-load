use std::path::{Path, PathBuf};

use chzzk_load::consolidation::manifest::TargetLocation;
use chzzk_load::consolidation::staging::{
    STAGING_HEADROOM_MULTIPLIER, calculate_required_staging_bytes, compute_target_hash,
    resolve_staging_dir, staging_folder_name, verify_staging_capacity,
    verify_staging_capacity_with_space,
};

#[test]
fn test_resolve_staging_dir_with_custom_temp_dir() {
    let target = TargetLocation::parse("remote:bucket/session_1");
    let custom = PathBuf::from("/mnt/fast_nvme/consolidation_staging");

    let resolved = resolve_staging_dir(&target, Some(&custom));
    assert_eq!(resolved, custom);
}

#[test]
fn test_resolve_staging_dir_without_temp_dir_defaults_to_hashed_temp_subfolder() {
    let target = TargetLocation::parse("remote:bucket/session_1");

    let resolved = resolve_staging_dir(&target, None::<&Path>);
    let expected_folder_name = staging_folder_name(target.raw());

    assert_eq!(resolved, std::env::temp_dir().join(&expected_folder_name));
    assert!(
        expected_folder_name.starts_with("chzzk-load-consolidation-"),
        "folder name must begin with 'chzzk-load-consolidation-', got: {expected_folder_name}"
    );

    // Verify determinism: repeated calls return identical paths
    let resolved_second = resolve_staging_dir(&target, None::<&Path>);
    assert_eq!(resolved, resolved_second);
}

#[test]
fn test_resolve_staging_dir_different_targets_produce_distinct_subfolders() {
    let target_a = TargetLocation::parse("remote:bucket/stream_a");
    let target_b = TargetLocation::parse("remote:bucket/stream_b");

    let dir_a = resolve_staging_dir(&target_a, None::<&Path>);
    let dir_b = resolve_staging_dir(&target_b, None::<&Path>);

    assert_ne!(dir_a, dir_b);
    assert_ne!(
        compute_target_hash(target_a.raw()),
        compute_target_hash(target_b.raw())
    );
}

#[test]
fn test_calculate_required_staging_bytes() {
    assert_eq!(calculate_required_staging_bytes(0), 0);
    assert_eq!(calculate_required_staging_bytes(100), 115);
    assert_eq!(
        calculate_required_staging_bytes(1_000_000_000),
        1_150_000_000
    );
    assert_eq!(STAGING_HEADROOM_MULTIPLIER, 1.15);
}

#[test]
fn test_verify_staging_capacity_with_space_sufficient_headroom() {
    let staging_dir = Path::new("/dummy/staging/workspace");

    // Exact 1.15x boundary
    assert!(
        verify_staging_capacity_with_space(staging_dir, 1150, 1000).is_ok(),
        "exact 1.15x boundary must succeed"
    );

    // Ample headroom
    assert!(
        verify_staging_capacity_with_space(staging_dir, 10_000_000, 1000).is_ok(),
        "abundant available space must succeed"
    );

    // Zero bytes payload
    assert!(
        verify_staging_capacity_with_space(staging_dir, 0, 0).is_ok(),
        "zero bytes payload must succeed"
    );
}

#[test]
fn test_verify_staging_capacity_with_space_insufficient_headroom() {
    let staging_dir = Path::new("/volumes/fast_disk/staging");

    // 1 byte less than 1.15x required
    let res = verify_staging_capacity_with_space(staging_dir, 1149, 1000);
    assert!(res.is_err(), "1149 available vs 1150 required must fail");

    let err = res.unwrap_err();
    let err_msg = format!("{err:#}");

    assert!(
        err_msg.contains("Insufficient disk space"),
        "error must mention 'Insufficient disk space': {err_msg}"
    );
    assert!(
        err_msg.contains("required"),
        "error must mention required space: {err_msg}"
    );
    assert!(
        err_msg.contains("available"),
        "error must mention available space: {err_msg}"
    );
    assert!(
        err_msg.contains("1.15x"),
        "error must mention 1.15x headroom: {err_msg}"
    );
    assert!(
        err_msg.contains("--temp-dir"),
        "error must recommend '--temp-dir': {err_msg}"
    );
    assert!(
        err_msg.contains("/volumes/fast_disk/staging"),
        "error must include staging path: {err_msg}"
    );
}

#[test]
fn test_verify_staging_capacity_real_filesystem_success() {
    let temp_dir = std::env::temp_dir().join(format!("test_cap_succ_{}", rand::random::<u32>()));
    // Small payload (1 KB) easily satisfies modern drive free space
    let res = verify_staging_capacity(&temp_dir, 1024);
    assert!(
        res.is_ok(),
        "pre-flight check for 1024 bytes against temp dir should succeed, got: {res:?}"
    );
}

#[test]
fn test_verify_staging_capacity_real_filesystem_insufficient_space() {
    let temp_dir = std::env::temp_dir().join(format!("test_cap_fail_{}", rand::random::<u32>()));
    // Exorbitantly huge stream (e.g. 50 PB) must fail pre-flight capacity check
    let huge_bytes = 50_000_000_000_000_000u64;
    let res = verify_staging_capacity(&temp_dir, huge_bytes);
    assert!(
        res.is_err(),
        "50 PB video stream capacity check must fail on standard hardware"
    );

    let err_msg = format!("{:#}", res.unwrap_err());
    assert!(
        err_msg.contains("Insufficient disk space"),
        "error must mention insufficient space: {err_msg}"
    );
    assert!(
        err_msg.contains("--temp-dir"),
        "error must recommend '--temp-dir': {err_msg}"
    );
}
