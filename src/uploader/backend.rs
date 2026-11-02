use futures_util::stream::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ProgressCallback = Box<dyn Fn(u64, u64, f64) + Send + Sync + 'static>;

/// Trait defining the cloud storage upload operations.
pub trait UploadBackend: Send + Sync {
    /// Uploads a file at `local_path` directly to `remote_dest` with optional progress tracking.
    fn upload_file_to_dest<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dest: &'a str,
        on_progress: Option<ProgressCallback>,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;

    /// Atomically moves/renames a remote file from `remote_src` to `remote_dst`.
    fn move_file<'a>(
        &'a self,
        _remote_src: &'a str,
        _remote_dst: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { anyhow::bail!("move_file is not supported by this storage backend") })
    }

    /// Idempotently deletes a remote file at `remote_path`.
    /// Returns Ok(()) if the file was deleted or was already absent.
    fn delete_file<'a>(&'a self, _remote_path: &'a str) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(
            async move { anyhow::bail!("delete_file is not supported by this storage backend") },
        )
    }

    /// Deletes multiple remote files concurrently under a bounded concurrency limit.
    fn delete_files<'a>(&'a self, remote_paths: &'a [String]) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if remote_paths.is_empty() {
                return Ok(());
            }
            let concurrency = 16.min(remote_paths.len()).max(1);
            let mut futures = Vec::with_capacity(remote_paths.len());
            for path in remote_paths {
                futures.push(self.delete_file(path));
            }
            let results = futures_util::stream::iter(futures)
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await;

            let mut errors = Vec::new();
            for res in results {
                if let Err(e) = res {
                    errors.push(e);
                }
            }

            if !errors.is_empty() {
                let count = errors.len();
                let sample: Vec<String> = errors.iter().take(5).map(|e| format!("{e:#}")).collect();
                anyhow::bail!(
                    "Failed to delete {count} remote file(s):\n  - {}",
                    sample.join("\n  - ")
                );
            }
            Ok(())
        })
    }

    /// Verifies connectivity to the storage backend.
    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>>;

    /// Downloads a remote file at `remote_src` directly to `local_dest`.
    fn download_file<'a>(
        &'a self,
        _remote_src: &'a str,
        _local_dest: &'a Path,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(
            async move { anyhow::bail!("download_file is not supported by this storage backend") },
        )
    }

    /// Convenience default method wrapping `upload_file_to_dest`.
    fn upload_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| anyhow::anyhow!("invalid local file path: {local_path:?}"))?;
            let remote_dest = if remote_dir.is_empty() {
                file_name.to_string()
            } else {
                format!("{}/{}", remote_dir.trim_end_matches('/'), file_name)
            };
            self.upload_file_to_dest(local_path, &remote_dest, Some(on_progress))
                .await
        })
    }
}

/// In-memory mock upload backend for deterministic unit and integration tests.
#[derive(Default, Clone)]
pub struct MockUploadBackend {
    pub uploads: Arc<Mutex<Vec<(PathBuf, String)>>>,
    pub files: Arc<Mutex<HashMap<String, u64>>>,
    pub should_fail: Arc<AtomicBool>,
    pub fail_patterns: Arc<Mutex<Vec<String>>>,
}

impl MockUploadBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UploadBackend for MockUploadBackend {
    fn upload_file_to_dest<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dest: &'a str,
        on_progress: Option<ProgressCallback>,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }

            {
                let patterns = self.fail_patterns.lock().await;
                for pat in patterns.iter() {
                    if remote_dest.contains(pat) || local_path.to_string_lossy().contains(pat) {
                        return Err(anyhow::anyhow!("simulated error for pattern: {pat}"));
                    }
                }
            }

            let len = if local_path.exists() {
                tokio::fs::metadata(local_path).await?.len()
            } else {
                1024
            };
            self.uploads
                .lock()
                .await
                .push((local_path.to_path_buf(), remote_dest.to_string()));
            self.files.lock().await.insert(remote_dest.to_string(), len);
            if let Some(cb) = on_progress {
                cb(len, len, 10.0);
            }
            Ok(len)
        })
    }

    fn move_file<'a>(
        &'a self,
        remote_src: &'a str,
        remote_dst: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }
            let mut files = self.files.lock().await;
            if let Some(len) = files.remove(remote_src) {
                files.insert(remote_dst.to_string(), len);
                Ok(())
            } else {
                Err(anyhow::anyhow!("source file not found: {remote_src}"))
            }
        })
    }

    fn delete_file<'a>(&'a self, remote_path: &'a str) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }
            let mut files = self.files.lock().await;
            files.remove(remote_path);
            Ok(())
        })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }
            Ok(())
        })
    }

    fn download_file<'a>(
        &'a self,
        remote_src: &'a str,
        local_dest: &'a Path,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }
            {
                let patterns = self.fail_patterns.lock().await;
                for pat in patterns.iter() {
                    if remote_src.contains(pat) || local_dest.to_string_lossy().contains(pat) {
                        return Err(anyhow::anyhow!("simulated error for pattern: {pat}"));
                    }
                }
            }
            if let Some(parent) = local_dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let path_str = if let Some((_, rest)) = remote_src.split_once(':') {
                rest
            } else {
                remote_src
            };
            let p = Path::new(path_str);
            if p.exists() {
                let content = tokio::fs::read(p).await?;
                tokio::fs::write(local_dest, content).await?;
                Ok(())
            } else {
                anyhow::bail!("MockUploadBackend: remote source not found on disk: {remote_src}")
            }
        })
    }

    fn upload_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            if self.should_fail.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("simulated error"));
            }

            let len = if local_path.exists() {
                tokio::fs::metadata(local_path).await?.len()
            } else {
                1024
            };
            self.uploads
                .lock()
                .await
                .push((local_path.to_path_buf(), remote_dir.to_string()));

            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file");
            let remote_dest = if remote_dir.is_empty() {
                file_name.to_string()
            } else {
                format!("{}/{}", remote_dir.trim_end_matches('/'), file_name)
            };
            self.files.lock().await.insert(remote_dest, len);

            on_progress(len, len, 10.0);
            Ok(len)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_mock_backend_upload_file() {
        let temp_dir = std::env::temp_dir().join(format!("test_mock_up_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let file_path = temp_dir.join("test_chunk.ts");
        tokio::fs::write(&file_path, b"test MPEG-TS video content data")
            .await
            .unwrap();
        let file_len = tokio::fs::metadata(&file_path).await.unwrap().len();

        let mock = MockUploadBackend::default();
        let progress_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let progress_called_clone = progress_called.clone();

        let uploaded_len = mock
            .upload_file(
                &file_path,
                "remote_dir_1",
                Box::new(move |uploaded, total, speed| {
                    assert_eq!(uploaded, file_len);
                    assert_eq!(total, file_len);
                    assert!(speed > 0.0);
                    progress_called_clone.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .expect("upload should succeed");

        assert_eq!(uploaded_len, file_len);
        assert!(progress_called.load(Ordering::SeqCst));
        assert!(
            file_path.exists(),
            "local file must remain intact upon confirmed upload"
        );

        let uploads = mock.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0], (file_path, "remote_dir_1".to_string()));

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_mock_backend_simulated_error() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_mock_err_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let file_path = temp_dir.join("test_chunk_err.ts");
        tokio::fs::write(&file_path, b"some content").await.unwrap();

        let mock = MockUploadBackend::default();
        mock.should_fail.store(true, Ordering::SeqCst);

        let res = mock
            .upload_file(&file_path, "remote_dir_err", Box::new(|_, _, _| {}))
            .await;

        assert!(res.is_err(), "upload should fail when should_fail is true");
        assert!(
            file_path.exists(),
            "local file must remain intact on upload failure"
        );
        assert!(mock.uploads.lock().await.is_empty());

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_mock_backend_check_connection() {
        let mock = MockUploadBackend::default();
        assert!(mock.check_connection().await.is_ok());

        mock.should_fail.store(true, Ordering::SeqCst);
        assert!(mock.check_connection().await.is_err());
    }

    #[tokio::test]
    async fn test_upload_backend_object_safety() {
        let mock = MockUploadBackend::default();
        let backend: Arc<dyn UploadBackend> = Arc::new(mock);
        assert!(backend.check_connection().await.is_ok());
    }
}
