use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

/// Checks if a session directory is strictly empty (0 entries).
/// If so, removes the directory with bounded retry handling Windows file-locking latency.
/// Returns `Ok(true)` if the directory was removed, `Ok(false)` if it contained any files
/// or was preserved, or `Err(e)` on I/O error.
pub async fn cleanup_session_dir_if_empty(session_dir: &Path) -> std::io::Result<bool> {
    if !session_dir.exists() || !session_dir.is_dir() {
        return Ok(false);
    }

    let mut sub_entries = match tokio::fs::read_dir(session_dir).await {
        Ok(rd) => rd,
        Err(e) => return Err(e),
    };

    if sub_entries.next_entry().await?.is_some() {
        // Directory contains at least one entry - preserve it strictly.
        return Ok(false);
    }

    let mut remove_dir_res = tokio::fs::remove_dir(session_dir).await;
    let mut attempts = 0;
    while let Err(ref e) = remove_dir_res {
        if attempts >= 5 || e.kind() == std::io::ErrorKind::NotFound {
            break;
        }
        let raw_os = e.raw_os_error();
        let is_transient_lock = raw_os == Some(145) // ERROR_DIR_NOT_EMPTY (pending unlinks)
            || raw_os == Some(32) // ERROR_SHARING_VIOLATION
            || raw_os == Some(5)  // ERROR_ACCESS_DENIED
            || e.kind() == std::io::ErrorKind::PermissionDenied;

        if is_transient_lock {
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(20 * attempts)).await;
            remove_dir_res = tokio::fs::remove_dir(session_dir).await;
        } else {
            break;
        }
    }

    if remove_dir_res.is_ok() || !session_dir.exists() {
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Cleans up strictly empty session directories inside the recordings directory,
/// strictly excluding any currently active session directory names or channel prefixes.
pub async fn cleanup_empty_session_dirs_excluding(
    recordings_dir: &Path,
    active_channels: &HashSet<String>,
) -> std::io::Result<usize> {
    if !recordings_dir.exists() || !recordings_dir.is_dir() {
        return Ok(0);
    }

    let mut removed_count = 0;
    let mut entries = tokio::fs::read_dir(recordings_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let is_dir = match entry.file_type().await {
            Ok(ft) => ft.is_dir(),
            Err(_) => entry.path().is_dir(),
        };
        if is_dir {
            let path = entry.path();
            let dir_name = entry.file_name();
            let dir_name_str = dir_name.to_string_lossy();

            let is_active = active_channels.iter().any(|item| {
                dir_name_str == item.as_str() || dir_name_str.starts_with(&format!("{item}_"))
            });
            if is_active {
                continue;
            }

            if let Ok(true) = cleanup_session_dir_if_empty(&path).await {
                removed_count += 1;
            }
        }
    }

    Ok(removed_count)
}

/// Cleans up all empty or metadata-only session directories inside the recordings directory.
pub async fn cleanup_empty_session_dirs(recordings_dir: &Path) -> std::io::Result<usize> {
    cleanup_empty_session_dirs_excluding(recordings_dir, &HashSet::new()).await
}

/// Cleans up empty session directories bounded by a maximum duration.
pub async fn cleanup_empty_session_dirs_bounded(
    recordings_dir: &Path,
    timeout: Duration,
) -> std::io::Result<usize> {
    match tokio::time::timeout(timeout, cleanup_empty_session_dirs(recordings_dir)).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "cleanup empty session directories timed out",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cleanup_session_dir_if_empty_preserves_dir_with_metadata_jsonl_and_purges_when_empty()
     {
        let temp_dir =
            std::env::temp_dir().join(format!("test_cleanup_meta_{}", rand::random::<u32>()));
        let session_dir = temp_dir.join("session_123");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        let meta_file = session_dir.join("metadata.jsonl");
        tokio::fs::write(&meta_file, b"{\"event\":\"INITIAL_STATE\"}\n")
            .await
            .unwrap();

        assert!(session_dir.exists());
        assert!(meta_file.exists());

        // Under strict emptiness, presence of metadata.jsonl must preserve the directory
        let removed_with_meta = cleanup_session_dir_if_empty(&session_dir).await.unwrap();
        assert!(
            !removed_with_meta,
            "session directory with metadata.jsonl must NOT be removed under strict emptiness"
        );
        assert!(session_dir.exists());
        assert!(meta_file.exists());

        // Once metadata.jsonl is removed (simulating final cloud upload unlinking), the directory is strictly empty
        tokio::fs::remove_file(&meta_file).await.unwrap();
        let removed_when_empty = cleanup_session_dir_if_empty(&session_dir).await.unwrap();
        assert!(
            removed_when_empty,
            "strictly empty session directory must be removed"
        );
        assert!(!session_dir.exists());

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_cleanup_session_dir_if_empty_preserves_dir_with_chunks() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_cleanup_preserve_{}", rand::random::<u32>()));
        let session_dir = temp_dir.join("session_123");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        let chunk_file = session_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_file, b"video data").await.unwrap();

        let removed = cleanup_session_dir_if_empty(&session_dir).await.unwrap();
        assert!(!removed);
        assert!(session_dir.exists());
        assert!(chunk_file.exists());

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
