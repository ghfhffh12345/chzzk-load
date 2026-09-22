use std::path::{Path, PathBuf};

pub fn get_exe_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

fn is_npm_context(exe_dir: &Path) -> bool {
    if std::env::var("CHZZK_LOAD_NPM")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return true;
    }
    exe_dir
        .components()
        .any(|c| c.as_os_str() == "node_modules")
}

pub fn resolve_path_with(path: &Path, cwd: &Path, exe_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let cwd_candidate = cwd.join(path);
    if cwd_candidate.exists() {
        return cwd_candidate;
    }

    if !is_npm_context(exe_dir) {
        let exe_candidate = exe_dir.join(path);
        if exe_candidate.exists() {
            return exe_candidate;
        }
    }

    cwd_candidate
}

pub fn resolve_path(path: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    resolve_path_with(path, &cwd, &get_exe_dir())
}
