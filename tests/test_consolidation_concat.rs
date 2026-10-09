use chzzk_load::consolidation::{
    ConcatScriptGuard, create_temp_concat_script, create_temp_concat_script_in,
    create_temp_concat_script_in_sync, create_temp_concat_script_sync, escape_concat_path,
    generate_concat_script,
};
use std::fs;
use std::path::PathBuf;

fn create_temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{}_{}", prefix, rand::random::<u32>()));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn test_escape_concat_path_windows_separators() {
    let input = r"C:\recordings\session_1\chunk_0000.ts";
    let expected = "C:/recordings/session_1/chunk_0000.ts";
    assert_eq!(escape_concat_path(input), expected);
}

#[test]
fn test_escape_concat_path_single_quotes() {
    let input = "session_'special'_name/chunk_0001.ts";
    let expected = r"session_'\''special'\''_name/chunk_0001.ts";
    assert_eq!(escape_concat_path(input), expected);
}

#[test]
fn test_escape_concat_path_windows_with_quotes() {
    let input = r"C:\recordings\It's Streamer\chunk_0002.ts";
    let expected = r"C:/recordings/It'\''s Streamer/chunk_0002.ts";
    assert_eq!(escape_concat_path(input), expected);
}

#[test]
fn test_escape_concat_path_unicode_and_korean() {
    let input = r"D:\녹화\[2026-10-09] [스트리머] 방송 [1080p]\chunk_0003.ts";
    let expected = "D:/녹화/[2026-10-09] [스트리머] 방송 [1080p]/chunk_0003.ts";
    assert_eq!(escape_concat_path(input), expected);
}

#[test]
fn test_escape_concat_path_http_url() {
    let input = "http://127.0.0.1:28522/chunk_0000.ts";
    assert_eq!(escape_concat_path(input), input);
}

#[test]
fn test_generate_concat_script_format() {
    // Empty entries
    let empty: Vec<String> = vec![];
    assert_eq!(generate_concat_script(&empty), "");

    // Single entry
    let single = vec!["chunk_0000.ts".to_string()];
    assert_eq!(generate_concat_script(&single), "file 'chunk_0000.ts'\n");

    // Multiple entries with escaping
    let entries = vec![
        r"C:\recordings\session\chunk_0000.ts".to_string(),
        r"C:\recordings\don't_stop\chunk_0001.ts".to_string(),
        "http://127.0.0.1:8080/chunk_0002.ts".to_string(),
    ];
    let expected = "file 'C:/recordings/session/chunk_0000.ts'\n\
                    file 'C:/recordings/don'\\''t_stop/chunk_0001.ts'\n\
                    file 'http://127.0.0.1:8080/chunk_0002.ts'\n";
    assert_eq!(generate_concat_script(&entries), expected);
}

#[tokio::test]
async fn test_create_temp_concat_script_valid_utf8_without_bom() {
    let entries = vec![
        r"C:\녹화\[스트리머] 방송\chunk_0000.ts".to_string(),
        r"C:\recordings\streamer's channel\chunk_0001.ts".to_string(),
    ];

    let guard = create_temp_concat_script(&entries)
        .await
        .expect("create_temp_concat_script should succeed");

    let script_path = guard.path().to_path_buf();
    assert!(
        script_path.exists(),
        "Temporary script file must exist on disk"
    );

    // Read raw bytes to verify strictly UTF-8 without BOM
    let bytes = fs::read(&script_path).expect("read temp script bytes");

    // UTF-8 BOM is [0xEF, 0xBB, 0xBF]
    assert!(bytes.len() >= 3, "Generated script must have content");
    assert!(
        !bytes.starts_with(&[0xEF, 0xBB, 0xBF]),
        "FFmpeg concat demuxer rejects UTF-8 BOM; file must NOT start with BOM"
    );

    // Verify content string matches expected format
    let content = String::from_utf8(bytes).expect("file content must be valid UTF-8");
    let expected = generate_concat_script(&entries);
    assert_eq!(content, expected);

    // Explicit drop should delete file
    drop(guard);
    assert!(
        !script_path.exists(),
        "Temporary script file must be cleaned up on drop"
    );
}

#[test]
fn test_concat_script_guard_raii_cleanup_on_drop() {
    let temp_dir = create_temp_test_dir("test_guard_drop");
    let dummy_file = temp_dir.join("dummy_manifest.txt");
    fs::write(&dummy_file, b"file 'test.ts'\n").expect("write dummy file");
    assert!(dummy_file.exists());

    {
        let guard = ConcatScriptGuard::new(dummy_file.clone());
        assert_eq!(guard.path(), dummy_file.as_path());
        assert!(guard.path().exists());
    } // guard drops here

    assert!(
        !dummy_file.exists(),
        "ConcatScriptGuard must remove file on drop"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_concat_script_guard_drop_tolerates_transient_lock() {
    let temp_dir = create_temp_test_dir("test_guard_transient_lock");
    let dummy_file = temp_dir.join("locked_manifest.txt");
    fs::write(&dummy_file, b"file 'chunk_0000.ts'\n").expect("write dummy file");
    assert!(dummy_file.exists());

    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        opts.share_mode(1); // FILE_SHARE_READ only
    }
    let lock_file = opts.open(&dummy_file).expect("open lock file");

    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(lock_file);
    });

    {
        let _guard = ConcatScriptGuard::new(dummy_file.clone());
    } // guard drops here, invoking unlink_local_file_with_retry_sync

    assert!(
        !dummy_file.exists(),
        "ConcatScriptGuard must remove file on drop even if transiently locked"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_concat_script_guard_into_path_defuses_drop() {
    let temp_dir = create_temp_test_dir("test_guard_defuse");
    let dummy_file = temp_dir.join("preserved_manifest.txt");
    fs::write(&dummy_file, b"file 'preserved.ts'\n").expect("write dummy file");
    assert!(dummy_file.exists());

    {
        let guard = ConcatScriptGuard::new(dummy_file.clone());
        let preserved = guard.into_path();
        assert_eq!(preserved, dummy_file);
    } // guard would have dropped here

    assert!(
        dummy_file.exists(),
        "into_path() must defuse drop so file is preserved"
    );

    let _ = fs::remove_file(&dummy_file);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_create_temp_concat_script_in_custom_dir() {
    let temp_dir = create_temp_test_dir("test_custom_dir");
    let entries = vec!["chunk_0000.ts".to_string(), "chunk_0001.ts".to_string()];

    let guard = create_temp_concat_script_in(&temp_dir, &entries)
        .await
        .expect("create_temp_concat_script_in should succeed");

    assert!(guard.path().starts_with(&temp_dir));
    assert!(guard.path().exists());

    let content = fs::read_to_string(guard.path()).expect("read script content");
    assert_eq!(content, generate_concat_script(&entries));

    drop(guard);
    assert!(!temp_dir.join("concat_").exists());
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_create_temp_concat_script_sync_helpers() {
    let temp_dir = create_temp_test_dir("test_sync_helpers");
    let entries = vec!["chunk_0000.ts".to_string()];

    // Test in-dir sync helper
    let guard_in = create_temp_concat_script_in_sync(&temp_dir, &entries)
        .expect("create_temp_concat_script_in_sync should succeed");
    assert!(guard_in.path().exists());
    drop(guard_in);

    // Test global temp_dir sync helper
    let guard_global = create_temp_concat_script_sync(&entries)
        .expect("create_temp_concat_script_sync should succeed");
    let global_path = guard_global.path().to_path_buf();
    assert!(global_path.exists());
    drop(guard_global);
    assert!(!global_path.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_concat_script_guard_traits_and_display() {
    let path = PathBuf::from("/tmp/test_manifest.txt");
    let guard = ConcatScriptGuard::new(path.clone());

    // Deref to &Path
    assert_eq!(&*guard, path.as_path());
    // AsRef<Path>
    assert_eq!(guard.as_ref(), path.as_path());
    // Display
    assert_eq!(format!("{guard}"), path.display().to_string());
    // Debug
    let debug_str = format!("{guard:?}");
    assert!(debug_str.contains("ConcatScriptGuard"));

    // Defuse so it doesn't attempt to unlink nonexistent /tmp path
    let _ = guard.into_path();
}

#[tokio::test]
async fn test_create_temp_concat_script_in_nonexistent_directory_fails_without_leak() {
    let nonexistent_dir = PathBuf::from("Z:\\nonexistent_dir_12345\\impossible");
    let entries = vec!["chunk_0000.ts".to_string()];

    let result = create_temp_concat_script_in(&nonexistent_dir, &entries).await;
    assert!(
        result.is_err(),
        "Writing to a nonexistent directory must fail"
    );
}

#[test]
fn test_concat_script_guard_drop_tolerates_not_found() {
    let temp_dir = create_temp_test_dir("test_guard_not_found");
    let missing_file = temp_dir.join("nonexistent_manifest.txt");
    assert!(!missing_file.exists());

    {
        let _guard = ConcatScriptGuard::new(missing_file.clone());
    } // drops here without panic

    assert!(!missing_file.exists());
    let _ = fs::remove_dir_all(&temp_dir);
}
