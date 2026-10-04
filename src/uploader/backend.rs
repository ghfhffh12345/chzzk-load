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
    /// Uploads a file at `local_path` to `remote_dir` and reports progress.
    /// Does not delete the local file.
    fn upload_file<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>>;

    /// Verifies connectivity to the storage backend.
    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>>;
}

/// In-memory mock upload backend for deterministic unit and integration tests.
#[derive(Default, Clone)]
pub struct MockUploadBackend {
    pub uploads: Arc<Mutex<Vec<(PathBuf, String)>>>,
    pub should_fail: Arc<AtomicBool>,
}

impl MockUploadBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UploadBackend for MockUploadBackend {
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
            on_progress(len, len, 10.0);
            Ok(len)
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
