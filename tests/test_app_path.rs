use chzzk_load::app_path::{get_exe_dir, resolve_path};
use std::path::{Path, PathBuf};

#[test]
fn test_resolve_relative_path() {
    let exe_dir = get_exe_dir();
    let rel = Path::new("settings.json");
    let resolved = resolve_path(rel);
    assert_eq!(resolved, exe_dir.join("settings.json"));
}

#[test]
fn test_resolve_absolute_path() {
    #[cfg(windows)]
    let abs = PathBuf::from("C:\\custom\\settings.json");
    #[cfg(not(windows))]
    let abs = PathBuf::from("/custom/settings.json");

    let resolved = resolve_path(&abs);
    assert_eq!(resolved, abs);
}
