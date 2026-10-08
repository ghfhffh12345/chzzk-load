#![allow(dead_code)]

pub mod mock_ffmpeg;
pub mod mock_rclone;
pub mod mock_source;
pub mod observability;

use std::path::PathBuf;
use std::sync::Mutex;

static CLEANUP_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

unsafe extern "C" {
    fn atexit(cb: extern "C" fn()) -> std::ffi::c_int;
}

extern "C" fn cleanup_mock_dirs() {
    if let Ok(dirs) = CLEANUP_DIRS.lock() {
        for dir in dirs.iter() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Registers a temporary directory used by subprocess mocks to be deleted upon test process exit.
pub fn register_mock_temp_dir(dir: PathBuf) {
    static ATEXIT_REGISTERED: std::sync::Once = std::sync::Once::new();
    ATEXIT_REGISTERED.call_once(|| unsafe {
        atexit(cleanup_mock_dirs);
    });
    if let Ok(mut dirs) = CLEANUP_DIRS.lock() {
        dirs.push(dir);
    }
}
