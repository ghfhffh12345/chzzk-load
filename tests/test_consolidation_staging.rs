use std::path::{Path, PathBuf};

mod common;

use chzzk_load::consolidation::manifest::TargetLocation;
use chzzk_load::consolidation::staging::{
    STAGING_HEADROOM_MULTIPLIER, calculate_required_staging_bytes, check_staging_resume_readiness,
    compute_target_hash, invalidate_staging_files, probe_moov_atom, resolve_staging_dir,
    staging_folder_name, verify_staging_capacity, verify_staging_capacity_with_space,
};

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).unwrap();
    common::register_mock_temp_dir(dir.clone());
    dir
}

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

#[test]
fn test_probe_moov_atom_nonexistent_and_empty_file() {
    let temp_dir = create_temp_test_dir("test_probe_empty");
    let nonexistent = temp_dir.join("nonexistent.mp4");
    assert!(!probe_moov_atom(&nonexistent));

    let empty = temp_dir.join("empty.mp4");
    std::fs::write(&empty, b"").unwrap();
    assert!(!probe_moov_atom(&empty));

    let too_short = temp_dir.join("short.mp4");
    std::fs::write(&too_short, b"1234567").unwrap();
    assert!(!probe_moov_atom(&too_short));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_probe_moov_atom_corrupted_and_missing_moov() {
    let temp_dir = create_temp_test_dir("test_probe_corrupt");

    // Random garbage
    let garbage = temp_dir.join("garbage.mp4");
    std::fs::write(&garbage, b"random garbage payload that is not mp4").unwrap();
    assert!(!probe_moov_atom(&garbage));

    // Valid ftyp box only, no moov
    let ftyp_only = temp_dir.join("ftyp_only.mp4");
    let mut ftyp_bytes = vec![0x00, 0x00, 0x00, 0x18];
    ftyp_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    std::fs::write(&ftyp_only, &ftyp_bytes).unwrap();
    assert!(!probe_moov_atom(&ftyp_only));

    // Empty 8-byte moov box without payload must be rejected
    let empty_moov = temp_dir.join("empty_moov.mp4");
    let mut em_zero = ftyp_bytes.clone();
    em_zero.extend_from_slice(&[0x00, 0x00, 0x00, 0x08]);
    em_zero.extend_from_slice(b"moov");
    std::fs::write(&empty_moov, &em_zero).unwrap();
    assert!(
        !probe_moov_atom(&empty_moov),
        "Empty 8-byte moov box without metadata payload must be rejected"
    );

    // ftyp + mdat only, no moov
    let ftyp_mdat = temp_dir.join("ftyp_mdat.mp4");
    let mut data = ftyp_bytes.clone();
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    data.extend_from_slice(b"mdat12345678");
    std::fs::write(&ftyp_mdat, &data).unwrap();
    assert!(!probe_moov_atom(&ftyp_mdat));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_probe_moov_atom_valid_faststart_and_end_moov() {
    let temp_dir = create_temp_test_dir("test_probe_valid");

    // Faststart MP4: ftyp (24 bytes) + moov (16 bytes) + mdat (16 bytes)
    let faststart = temp_dir.join("faststart.mp4");
    let mut fs_bytes = vec![0x00, 0x00, 0x00, 0x18];
    fs_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"moovmvhd1234");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"mdat12345678");
    std::fs::write(&faststart, &fs_bytes).unwrap();
    assert!(probe_moov_atom(&faststart));

    // moov at end: ftyp (24 bytes) + mdat (16 bytes) + moov (16 bytes)
    let end_moov = temp_dir.join("end_moov.mp4");
    let mut em_bytes = vec![0x00, 0x00, 0x00, 0x18];
    em_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    em_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    em_bytes.extend_from_slice(b"mdat12345678");
    em_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    em_bytes.extend_from_slice(b"moovmvhd1234");
    std::fs::write(&end_moov, &em_bytes).unwrap();
    assert!(probe_moov_atom(&end_moov));

    // 64-bit largesize mdat followed by moov
    let large_box = temp_dir.join("large_box.mp4");
    let mut lb_bytes = vec![0x00, 0x00, 0x00, 0x18];
    lb_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    // size=1 (indicates 64-bit largesize), type="mdat", largesize=24 (16 header + 8 data)
    lb_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    lb_bytes.extend_from_slice(b"mdat");
    lb_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18]);
    lb_bytes.extend_from_slice(b"12345678"); // 8 bytes data
    lb_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    lb_bytes.extend_from_slice(b"moovmvhd1234");
    std::fs::write(&large_box, &lb_bytes).unwrap();
    assert!(probe_moov_atom(&large_box));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_check_staging_resume_readiness() {
    let temp_dir = create_temp_test_dir("test_staging_resume");

    let mp4_path = temp_dir.join("consolidated.mp4");
    let jsonl_path = temp_dir.join("consolidated.jsonl");

    // Case 1: Empty staging dir
    assert!(!check_staging_resume_readiness(&temp_dir, true, true));
    assert!(!check_staging_resume_readiness(&temp_dir, true, false));
    assert!(!check_staging_resume_readiness(&temp_dir, false, true));

    // Create valid mp4
    let mut fs_bytes = vec![0x00, 0x00, 0x00, 0x18];
    fs_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"moovmvhd1234");
    std::fs::write(&mp4_path, &fs_bytes).unwrap();

    // Valid video only: has_video=true, has_chat=false -> true
    assert!(check_staging_resume_readiness(&temp_dir, true, false));
    // But has_chat=true expects chat too -> false
    assert!(!check_staging_resume_readiness(&temp_dir, true, true));

    // Create 0-byte chat
    std::fs::write(&jsonl_path, b"").unwrap();
    assert!(!check_staging_resume_readiness(&temp_dir, true, true));

    // Create valid non-empty chat
    std::fs::write(&jsonl_path, b"{\"content\":\"hello\"}\n").unwrap();
    // Now both valid -> true
    assert!(check_staging_resume_readiness(&temp_dir, true, true));
    // Chat only -> true
    assert!(check_staging_resume_readiness(&temp_dir, false, true));

    // Corrupt mp4
    std::fs::write(&mp4_path, b"corrupted bytes").unwrap();
    assert!(!check_staging_resume_readiness(&temp_dir, true, true));
    assert!(!check_staging_resume_readiness(&temp_dir, true, false));
    // Chat only still valid
    assert!(check_staging_resume_readiness(&temp_dir, false, true));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_invalidate_staging_files() {
    let temp_dir = create_temp_test_dir("test_invalidate");

    let mp4 = temp_dir.join("consolidated.mp4");
    let jsonl = temp_dir.join("consolidated.jsonl");
    let mp4_part = temp_dir.join("consolidated.mp4.part");
    let jsonl_part = temp_dir.join("consolidated.jsonl.part");
    let other_file = temp_dir.join("keep_this.txt");

    std::fs::write(&mp4, b"mp4").unwrap();
    std::fs::write(&jsonl, b"jsonl").unwrap();
    std::fs::write(&mp4_part, b"part1").unwrap();
    std::fs::write(&jsonl_part, b"part2").unwrap();
    std::fs::write(&other_file, b"keep").unwrap();

    let res = invalidate_staging_files(&temp_dir).await;
    assert!(res.is_ok());

    assert!(!mp4.exists(), "consolidated.mp4 must be removed");
    assert!(!jsonl.exists(), "consolidated.jsonl must be removed");
    assert!(!mp4_part.exists(), "consolidated.mp4.part must be removed");
    assert!(
        !jsonl_part.exists(),
        "consolidated.jsonl.part must be removed"
    );
    assert!(other_file.exists(), "other files must remain untouched");

    let _ = std::fs::remove_dir_all(&temp_dir);
}
