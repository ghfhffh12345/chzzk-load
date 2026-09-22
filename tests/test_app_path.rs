use chzzk_load::app_path::{resolve_path, resolve_path_with};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

static ENV_MUTEX: Mutex<()> = Mutex::new(());

#[test]
fn test_resolve_relative_path_prefers_cwd_when_exists() {
    let temp_root =
        std::env::temp_dir().join(format!("chzzk_path_test_1_{}", rand::random::<u32>()));
    let mock_cwd = temp_root.join("cwd");
    let mock_exe = temp_root.join("bin");
    fs::create_dir_all(&mock_cwd).unwrap();
    fs::create_dir_all(&mock_exe).unwrap();

    let cwd_file = mock_cwd.join("settings.json");
    let exe_file = mock_exe.join("settings.json");
    fs::write(&cwd_file, "cwd content").unwrap();
    fs::write(&exe_file, "exe content").unwrap();

    let resolved = resolve_path_with(Path::new("settings.json"), &mock_cwd, &mock_exe);
    assert_eq!(
        resolved, cwd_file,
        "Should prefer settings.json in CWD over exe dir"
    );

    let _ = fs::remove_dir_all(&temp_root);
}

#[test]
fn test_resolve_relative_path_defaults_to_cwd_when_missing() {
    let temp_root =
        std::env::temp_dir().join(format!("chzzk_path_test_2_{}", rand::random::<u32>()));
    let mock_cwd = temp_root.join("cwd");
    let mock_exe = temp_root.join("bin");
    fs::create_dir_all(&mock_cwd).unwrap();
    fs::create_dir_all(&mock_exe).unwrap();

    let resolved = resolve_path_with(Path::new("settings.json"), &mock_cwd, &mock_exe);
    assert_eq!(
        resolved,
        mock_cwd.join("settings.json"),
        "Missing file should default to CWD for creation"
    );

    let _ = fs::remove_dir_all(&temp_root);
}

#[test]
fn test_resolve_relative_path_falls_back_to_exe_dir_when_not_in_cwd() {
    let _guard = ENV_MUTEX.lock().unwrap();
    let temp_root =
        std::env::temp_dir().join(format!("chzzk_path_test_3_{}", rand::random::<u32>()));
    let mock_cwd = temp_root.join("cwd");
    let mock_exe = temp_root.join("bin");
    fs::create_dir_all(&mock_cwd).unwrap();
    fs::create_dir_all(&mock_exe).unwrap();

    let exe_file = mock_exe.join("settings.json");
    fs::write(&exe_file, "portable settings").unwrap();

    let resolved = resolve_path_with(Path::new("settings.json"), &mock_cwd, &mock_exe);
    assert_eq!(
        resolved, exe_file,
        "Should fall back to portable exe dir when file exists there and not in CWD"
    );

    let _ = fs::remove_dir_all(&temp_root);
}

#[test]
fn test_resolve_relative_path_ignores_node_modules_exe_dir() {
    let _guard = ENV_MUTEX.lock().unwrap();
    let temp_root =
        std::env::temp_dir().join(format!("chzzk_path_test_4_{}", rand::random::<u32>()));
    let mock_cwd = temp_root.join("my-project");
    let mock_exe = temp_root
        .join("node_modules")
        .join("chzzk-load-windows-x64")
        .join("bin");
    fs::create_dir_all(&mock_cwd).unwrap();
    fs::create_dir_all(&mock_exe).unwrap();

    let exe_file = mock_exe.join("settings.json");
    fs::write(&exe_file, "stale node_modules settings").unwrap();

    let resolved = resolve_path_with(Path::new("settings.json"), &mock_cwd, &mock_exe);
    assert_eq!(
        resolved,
        mock_cwd.join("settings.json"),
        "Should NEVER fall back to settings inside node_modules"
    );

    let _ = fs::remove_dir_all(&temp_root);
}

#[test]
fn test_resolve_relative_path_ignores_exe_dir_when_npm_env_set() {
    let _guard = ENV_MUTEX.lock().unwrap();
    let temp_root =
        std::env::temp_dir().join(format!("chzzk_path_test_5_{}", rand::random::<u32>()));
    let mock_cwd = temp_root.join("cwd");
    let mock_exe = temp_root.join("bin");
    fs::create_dir_all(&mock_cwd).unwrap();
    fs::create_dir_all(&mock_exe).unwrap();

    let exe_file = mock_exe.join("settings.json");
    fs::write(&exe_file, "exe settings").unwrap();

    // Set CHZZK_LOAD_NPM environment variable
    unsafe {
        std::env::set_var("CHZZK_LOAD_NPM", "1");
    }

    let resolved = resolve_path_with(Path::new("settings.json"), &mock_cwd, &mock_exe);

    unsafe {
        std::env::remove_var("CHZZK_LOAD_NPM");
    }

    assert_eq!(
        resolved,
        mock_cwd.join("settings.json"),
        "When CHZZK_LOAD_NPM is set, should always resolve to CWD"
    );

    let _ = fs::remove_dir_all(&temp_root);
}

#[test]
fn test_resolve_path_resolves_to_cwd_in_repo() {
    let cwd = std::env::current_dir().unwrap();
    let rel = Path::new("settings.json");
    let resolved = resolve_path(rel);
    assert_eq!(
        resolved,
        cwd.join("settings.json"),
        "resolve_path should resolve existing settings.json in CWD"
    );
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
