use chzzk_load::disk::{get_disk_space, has_sufficient_disk_space};
use std::env;

#[test]
fn test_get_disk_space_temp_dir() {
    let temp = env::temp_dir();
    let space = get_disk_space(&temp).expect("Failed to get disk space for temp_dir");
    assert!(space.available_bytes > 0, "available_bytes should be > 0");
    assert!(space.total_bytes > 0, "total_bytes should be > 0");
    assert!(
        space.available_bytes <= space.total_bytes,
        "available_bytes should be <= total_bytes"
    );
    assert!(space.available_gb() > 0.0, "available_gb() should be > 0.0");
    assert!(space.total_gb() > 0.0, "total_gb() should be > 0.0");
    assert!(
        space.available_gb() <= space.total_gb(),
        "available_gb() should be <= total_gb()"
    );
}

#[test]
fn test_get_disk_space_non_existent_subdir() {
    let temp = env::temp_dir();
    let non_existent = temp
        .join(format!(
            "chzzk_non_existent_dir_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .join("nested_subdir");

    assert!(!non_existent.exists(), "Target test path should not exist");

    let space =
        get_disk_space(&non_existent).expect("Failed to get disk space for non-existent subdir");
    assert!(space.available_bytes > 0, "available_bytes should be > 0");
    assert!(space.total_bytes > 0, "total_bytes should be > 0");
    assert!(space.available_gb() > 0.0, "available_gb() should be > 0.0");
}

#[test]
fn test_has_sufficient_disk_space_small_or_zero_threshold() {
    let temp = env::temp_dir();

    // Threshold 0.0 or negative returns true
    assert!(
        has_sufficient_disk_space(&temp, 0.0),
        "0.0 threshold should return true"
    );
    assert!(
        has_sufficient_disk_space(&temp, -1.0),
        "negative threshold should return true"
    );

    // Very small threshold returns true
    assert!(
        has_sufficient_disk_space(&temp, 0.0001),
        "very small threshold (0.0001 GB) should return true"
    );
}

#[test]
fn test_has_sufficient_disk_space_impossibly_large_threshold() {
    let temp = env::temp_dir();

    // Impossibly large threshold (1,000,000 GB) returns false
    assert!(
        !has_sufficient_disk_space(&temp, 1_000_000.0),
        "impossibly large threshold should return false"
    );
}
