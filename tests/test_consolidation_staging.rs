use std::path::{Path, PathBuf};

mod common;

use chzzk_load::consolidation::manifest::TargetLocation;
use chzzk_load::consolidation::staging::{
    Mp4ContainerInfo, STAGING_HEADROOM_MULTIPLIER, calculate_required_staging_bytes,
    check_staging_resume_readiness, cleanup_staged_media, compute_target_hash,
    invalidate_staging_files, probe_moov_atom, resolve_staging_dir, staging_folder_name,
    validate_mp4_container_integrity, validate_staging_workspace, verify_staging_capacity,
    verify_staging_capacity_with_space,
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

    let mp4_path = temp_dir.join("livestream.mp4");
    let jsonl_path = temp_dir.join("livechat.jsonl");

    // Case 1: Empty staging dir
    assert!(!check_staging_resume_readiness(&temp_dir, true, true));
    assert!(!check_staging_resume_readiness(&temp_dir, true, false));
    assert!(!check_staging_resume_readiness(&temp_dir, false, true));

    // Create valid mp4: ftyp (24 bytes) + moov (16 bytes) + mdat (16 bytes)
    let mut fs_bytes = vec![0x00, 0x00, 0x00, 0x18];
    fs_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"moovmvhd1234");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"mdat12345678");
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

    let mp4 = temp_dir.join("livestream.mp4");
    let jsonl = temp_dir.join("livechat.jsonl");
    let mp4_part = temp_dir.join("livestream.mp4.part");
    let jsonl_part = temp_dir.join("livechat.jsonl.part");
    let other_file = temp_dir.join("keep_this.txt");

    std::fs::write(&mp4, b"mp4").unwrap();
    std::fs::write(&jsonl, b"jsonl").unwrap();
    std::fs::write(&mp4_part, b"part1").unwrap();
    std::fs::write(&jsonl_part, b"part2").unwrap();
    std::fs::write(&other_file, b"keep").unwrap();

    let res = invalidate_staging_files(&temp_dir).await;
    assert!(res.is_ok());

    assert!(!mp4.exists(), "livestream.mp4 must be removed");
    assert!(!jsonl.exists(), "livechat.jsonl must be removed");
    assert!(!mp4_part.exists(), "livestream.mp4.part must be removed");
    assert!(!jsonl_part.exists(), "livechat.jsonl.part must be removed");
    assert!(other_file.exists(), "other files must remain untouched");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_cleanup_staged_media_purges_parts_only() {
    let temp_dir = create_temp_test_dir("test_cleanup_staged_media");

    let live_stream_mp4 = temp_dir.join("live_stream.mp4");
    let live_chat_jsonl = temp_dir.join("live_chat.jsonl");
    let video_part = temp_dir.join("live_stream.mp4.part");
    let chat_part = temp_dir.join("live_chat.jsonl.part");
    let keep_file = temp_dir.join("other_data.bin");

    std::fs::write(&live_stream_mp4, b"final mp4").unwrap();
    std::fs::write(&live_chat_jsonl, b"final chat").unwrap();
    std::fs::write(&video_part, b"partial video").unwrap();
    std::fs::write(&chat_part, b"partial chat").unwrap();
    std::fs::write(&keep_file, b"keep me").unwrap();

    let res = cleanup_staged_media(&temp_dir).await;
    assert!(res.is_ok());

    // .part files MUST be deleted
    assert!(
        !video_part.exists(),
        "live_stream.mp4.part must be removed by cleanup_staged_media"
    );
    assert!(
        !chat_part.exists(),
        "live_chat.jsonl.part must be removed by cleanup_staged_media"
    );

    // Final files and other files MUST remain untouched
    assert!(
        live_stream_mp4.exists(),
        "final live_stream.mp4 must NOT be removed by cleanup_staged_media"
    );
    assert!(
        live_chat_jsonl.exists(),
        "final live_chat.jsonl must NOT be removed by cleanup_staged_media"
    );
    assert!(
        keep_file.exists(),
        "unrelated files must NOT be removed by cleanup_staged_media"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_validate_staging_workspace_valid_and_invalid_scenarios() {
    let temp_dir = create_temp_test_dir("test_validate_workspace");

    let mp4_path = temp_dir.join("live_stream.mp4");
    let chat_path = temp_dir.join("live_chat.jsonl");

    // 1. Both false -> error
    let err_no_media = validate_staging_workspace(&temp_dir, false, false).unwrap_err();
    assert!(
        err_no_media.to_string().contains("No media selected"),
        "error message must cite no media: {err_no_media}"
    );

    // 2. Both requested, but files missing -> error
    let err_missing = validate_staging_workspace(&temp_dir, true, true).unwrap_err();
    assert!(
        err_missing.to_string().contains("does not exist"),
        "error message must cite missing file: {err_missing}"
    );

    // 3. Create valid faststart MP4
    let mut fs_bytes = vec![0x00, 0x00, 0x00, 0x18];
    fs_bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"moovmvhd1234");
    fs_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    fs_bytes.extend_from_slice(b"mdat12345678");
    std::fs::write(&mp4_path, &fs_bytes).unwrap();

    // Video only requested -> Ok
    assert!(validate_staging_workspace(&temp_dir, true, false).is_ok());

    // Both requested, but chat still missing -> error
    let err_chat_missing = validate_staging_workspace(&temp_dir, true, true).unwrap_err();
    assert!(
        err_chat_missing.to_string().contains("does not exist")
            || err_chat_missing.to_string().contains("cannot be read"),
        "error must cite missing chat: {err_chat_missing}"
    );

    // 4. Create empty (0-byte) chat file -> error
    std::fs::write(&chat_path, b"").unwrap();
    let err_empty_chat = validate_staging_workspace(&temp_dir, true, true).unwrap_err();
    assert!(
        err_empty_chat.to_string().contains("empty (0 bytes)"),
        "error must cite empty chat: {err_empty_chat}"
    );

    // 5. Create valid non-empty chat file -> Ok
    std::fs::write(
        &chat_path,
        b"{\"time_ms\":1000,\"nickname\":\"user\",\"content\":\"hi\"}\n",
    )
    .unwrap();
    assert!(validate_staging_workspace(&temp_dir, true, true).is_ok());

    // Chat only requested -> Ok
    assert!(validate_staging_workspace(&temp_dir, false, true).is_ok());

    // 6. Corrupt video container -> error
    std::fs::write(&mp4_path, b"not an mp4 file").unwrap();
    let err_corrupt_mp4 = validate_staging_workspace(&temp_dir, true, true).unwrap_err();
    assert!(
        err_corrupt_mp4.to_string().contains("too small")
            || err_corrupt_mp4.to_string().contains("Corrupted MP4"),
        "error must cite container corruption: {err_corrupt_mp4}"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_probe_moov_atom_rejects_duplicate_moov_and_trailing_garbage() {
    let temp_dir = create_temp_test_dir("test_probe_corrupted_layout");

    // Replicate Rakoko corrupted MP4 layout:
    // ftyp (24 bytes) + moov (16 bytes) + mdat (16 bytes) + duplicate moov (16 bytes) + trailing garbage (16 bytes)
    let corrupt_mp4 = temp_dir.join("livestream.mp4");
    let mut bytes = vec![0x00, 0x00, 0x00, 0x18];
    bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    bytes.extend_from_slice(b"moovmvhd1234");
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    bytes.extend_from_slice(b"mdat12345678");
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    bytes.extend_from_slice(b"moovmvhd1234"); // Duplicate moov!
    bytes.extend_from_slice(b"UNINDEXED_GARBAGE!"); // Trailing garbage!
    std::fs::write(&corrupt_mp4, &bytes).unwrap();

    // MUST be rejected because of duplicate moov and trailing unindexed garbage!
    assert!(
        !probe_moov_atom(&corrupt_mp4),
        "MP4 container with duplicate moov atom and trailing garbage must be rejected"
    );
    assert!(
        !check_staging_resume_readiness(&temp_dir, true, false),
        "check_staging_resume_readiness must reject corrupt staging video"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

fn build_mp4_with_co64(target_chunk_offset: u64) -> Vec<u8> {
    // Structure:
    // ftyp: 24 bytes (0..24)
    // moov: 64 bytes (24..88)
    //   trak: 56 bytes (32..88)
    //     mdia: 48 bytes (40..88)
    //       minf: 40 bytes (48..88)
    //         stbl: 32 bytes (56..88)
    //           co64: 24 bytes (64..88)
    // mdat: 24 bytes (88..112)
    //   header: 8 bytes (88..96)
    //   payload: 16 bytes (96..112)
    let mut bytes = Vec::new();

    // ftyp
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x18]);
    bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");

    // moov (size 64)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x40]);
    bytes.extend_from_slice(b"moov");

    // trak (size 56)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x38]);
    bytes.extend_from_slice(b"trak");

    // mdia (size 48)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x30]);
    bytes.extend_from_slice(b"mdia");

    // minf (size 40)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x28]);
    bytes.extend_from_slice(b"minf");

    // stbl (size 32)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]);
    bytes.extend_from_slice(b"stbl");

    // co64 (size 24): version/flags (4), entry_count=1 (4), chunk_offset (8)
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x18]);
    bytes.extend_from_slice(b"co64");
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    bytes.extend_from_slice(&target_chunk_offset.to_be_bytes());

    // mdat (size 24): header 8, payload 16
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x18]);
    bytes.extend_from_slice(b"mdat");
    bytes.extend_from_slice(b"1234567812345678");

    bytes
}

#[test]
fn test_validate_mp4_container_integrity_valid_faststart_co64() {
    let temp_dir = create_temp_test_dir("test_validate_valid_faststart");
    let mp4_path = temp_dir.join("livestream.mp4");

    // Valid offset: 96 is inside mdat payload [96, 112)
    let bytes = build_mp4_with_co64(96);
    std::fs::write(&mp4_path, &bytes).unwrap();

    let info: Mp4ContainerInfo =
        validate_mp4_container_integrity(&mp4_path).expect("valid container must pass");
    assert_eq!(info.file_size, 112);
    assert_eq!(info.moov_offset, 24);
    assert_eq!(info.moov_size, 64);
    assert_eq!(info.mdat_offset, 88);
    assert_eq!(info.mdat_size, 24);
    assert!(info.is_faststart);
    assert_eq!(info.track_chunk_offsets, vec![96]);

    assert!(probe_moov_atom(&mp4_path));
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_validate_mp4_container_integrity_rejects_offset_pointing_to_moov() {
    let temp_dir = create_temp_test_dir("test_validate_desync_offset");
    let mp4_path = temp_dir.join("livestream.mp4");

    // Desync offset: 40 points inside moov (not mdat)
    let bytes = build_mp4_with_co64(40);
    std::fs::write(&mp4_path, &bytes).unwrap();

    let err = validate_mp4_container_integrity(&mp4_path).expect_err("desync offset must fail");
    assert!(
        err.to_string()
            .contains("sample chunk offset 40 does not fall within any 'mdat' media payload range"),
        "error message must detail offset desync: {err}"
    );

    assert!(!probe_moov_atom(&mp4_path));
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_validate_mp4_container_integrity_rejects_missing_mdat() {
    let temp_dir = create_temp_test_dir("test_validate_missing_mdat");
    let mp4_path = temp_dir.join("livestream.mp4");

    // ftyp (24 bytes) + moov (16 bytes) only, no mdat
    let mut bytes = vec![0x00, 0x00, 0x00, 0x18];
    bytes.extend_from_slice(b"ftypisom\0\0\x02\0isomiso2");
    bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
    bytes.extend_from_slice(b"moovmvhd1234");
    std::fs::write(&mp4_path, &bytes).unwrap();

    let err = validate_mp4_container_integrity(&mp4_path).expect_err("missing mdat must fail");
    assert!(
        err.to_string().contains("missing required 'mdat' atom"),
        "error message must indicate missing mdat: {err}"
    );

    assert!(!probe_moov_atom(&mp4_path));
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_validate_mp4_container_integrity_rejects_trailing_garbage() {
    let temp_dir = create_temp_test_dir("test_validate_trailing_garbage");
    let mp4_path = temp_dir.join("livestream.mp4");

    // Case 1: unindexed trailing bytes less than atom header size (5 bytes)
    let mut bytes_sub = build_mp4_with_co64(96);
    bytes_sub.extend_from_slice(b"12345");
    std::fs::write(&mp4_path, &bytes_sub).unwrap();

    let err = validate_mp4_container_integrity(&mp4_path)
        .expect_err("trailing sub-header garbage must fail");
    assert!(
        err.to_string().contains("trailing unindexed data"),
        "error message must indicate trailing unindexed data: {err}"
    );
    assert!(!probe_moov_atom(&mp4_path));

    // Case 2: corrupted trailing atom payload exceeding file length
    let mut bytes_corrupt_atom = build_mp4_with_co64(96);
    bytes_corrupt_atom.extend_from_slice(b"UNINDEXED_GARBAGE_PAYLOAD");
    std::fs::write(&mp4_path, &bytes_corrupt_atom).unwrap();

    let err_atom =
        validate_mp4_container_integrity(&mp4_path).expect_err("corrupted trailing atom must fail");
    assert!(
        err_atom.to_string().contains("exceeds file length")
            || err_atom.to_string().contains("trailing unindexed data"),
        "error message must detail corruption: {err_atom}"
    );
    assert!(!probe_moov_atom(&mp4_path));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
