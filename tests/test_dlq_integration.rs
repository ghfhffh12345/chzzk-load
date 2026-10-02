use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chzzk_load::config::RcloneConfig;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::backend::{BoxFuture, ProgressCallback, UploadBackend};
use chzzk_load::uploader::rclone::RcloneBackend;
use chzzk_load::uploader::{DlqConfig, UploadTask, UploadWorker};

/// RAII helper to create and automatically clean up temporary test directories.
struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "{prefix}_{}_{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::create_dir_all(&path).expect("failed to create temporary test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Checks if a functioning rclone binary is available.
fn ensure_rclone_available() -> bool {
    let bin = std::env::var("CHZZK_LOAD_RCLONE_BIN").unwrap_or_else(|_| "rclone".to_string());
    std::process::Command::new(&bin)
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Helper to configure an `RcloneBackend` pointing to a local filesystem remote.
fn create_local_rclone_backend(remote_dir: &Path) -> RcloneBackend {
    let remote_path = remote_dir.to_string_lossy().replace('\\', "/");
    let config = RcloneConfig {
        remote_path,
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
        skip_connection_check: false,
    };
    RcloneBackend::new(config)
}

type FailPredicate = Arc<dyn Fn(&Path, usize) -> bool + Send + Sync>;

/// Wrapper around `RcloneBackend` that allows injecting mock upload failures
/// on specific attempts before delegating to the genuine `RcloneBackend` subprocess.
struct ControlledRcloneBackend {
    inner: RcloneBackend,
    attempts: Arc<Mutex<HashMap<String, usize>>>,
    fail_predicate: FailPredicate,
}

impl ControlledRcloneBackend {
    fn new<F>(inner: RcloneBackend, fail_predicate: F) -> Self
    where
        F: Fn(&Path, usize) -> bool + Send + Sync + 'static,
    {
        Self {
            inner,
            attempts: Arc::new(Mutex::new(HashMap::new())),
            fail_predicate: Arc::new(fail_predicate),
        }
    }

    fn attempts_for(&self, file_name: &str) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .get(file_name)
            .copied()
            .unwrap_or(0)
    }
}

impl UploadBackend for ControlledRcloneBackend {
    fn upload_file_and_delete<'a>(
        &'a self,
        local_path: &'a Path,
        remote_dir: &'a str,
        on_progress: ProgressCallback,
    ) -> BoxFuture<'a, anyhow::Result<u64>> {
        Box::pin(async move {
            let file_name = local_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let count = attempts.entry(file_name.clone()).or_insert(0);
                *count += 1;
                *count
            };

            if (self.fail_predicate)(local_path, attempt) {
                anyhow::bail!("Mock injected failure on attempt {attempt} for {file_name}");
            }

            self.inner
                .upload_file_and_delete(local_path, remote_dir, on_progress)
                .await
        })
    }

    fn upload_text<'a>(
        &'a self,
        remote_dir: &'a str,
        file_name: &'a str,
        content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        self.inner.upload_text(remote_dir, file_name, content)
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        self.inner.check_connection()
    }
}

#[tokio::test]
async fn test_real_rclone_local_remote_basic_upload() {
    if !ensure_rclone_available() {
        eprintln!("Skipping rclone integration test: rclone binary not available");
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_basic_src");
    let remote_guard = TempDirGuard::new("test_rclone_basic_dst");

    let chunk_path = local_guard.path().join("chunk_0000.ts");
    let payload = b"video-stream-data-chunk-0";
    std::fs::write(&chunk_path, payload).expect("failed to write local chunk");

    let task = UploadTask {
        channel_id: "ch_stream1".to_string(),
        session_folder_id: "session_20261002".to_string(),
        remote_dir: "session_20261002".to_string(),
        chunk_path: chunk_path.clone(),
        chunk_name: "chunk_0000.ts".to_string(),
        streamer_name: "StreamerA".to_string(),
    };

    let backend = Arc::new(create_local_rclone_backend(remote_guard.path()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let worker_handle = UploadWorker::spawn_with_concurrency(Some(backend), event_tx, upload_rx, 1);

    upload_tx.send(task).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify local file was removed by RcloneBackend upon upload confirmation
    assert!(
        !chunk_path.exists(),
        "Local chunk must be deleted after confirmed rclone upload"
    );

    // Verify remote file was created by real rclone subprocess and contents match
    let remote_dest = remote_guard
        .path()
        .join("session_20261002")
        .join("chunk_0000.ts");
    assert!(
        remote_dest.exists(),
        "Remote destination file must exist in local filesystem remote: {:?}",
        remote_dest
    );
    let uploaded_bytes = std::fs::read(&remote_dest).expect("failed to read remote file");
    assert_eq!(
        uploaded_bytes, payload,
        "Remote uploaded file contents must match payload exactly"
    );

    // Verify UploadCompleted event was received
    let mut saw_completed = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::UploadCompleted { chunk_name, .. } = ev {
            if chunk_name == "chunk_0000.ts" {
                saw_completed = true;
            }
        }
    }
    assert!(
        saw_completed,
        "UploadCompleted event must be emitted for chunk_0000.ts"
    );
}

#[tokio::test]
async fn test_real_rclone_dlq_infinite_retry_eventually_succeeds() {
    if !ensure_rclone_available() {
        eprintln!("Skipping rclone integration test: rclone binary not available");
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_dlq_retry_src");
    let remote_guard = TempDirGuard::new("test_rclone_dlq_retry_dst");

    let chunk_path = local_guard.path().join("chunk_0000.ts");
    let payload = b"retryable-chunk-data";
    std::fs::write(&chunk_path, payload).expect("failed to write local chunk");

    let task = UploadTask {
        channel_id: "ch_stream1".to_string(),
        session_folder_id: "session_retry".to_string(),
        remote_dir: "session_retry".to_string(),
        chunk_path: chunk_path.clone(),
        chunk_name: "chunk_0000.ts".to_string(),
        streamer_name: "StreamerA".to_string(),
    };

    // Configure backend to fail attempts 1, 2, and 3 (mock failure),
    // and then resolve on attempt 4 (retry 3) to execute genuine rclone upload.
    let real_rclone = create_local_rclone_backend(remote_guard.path());
    let backend = Arc::new(ControlledRcloneBackend::new(
        real_rclone,
        |_path, attempt| attempt < 4,
    ));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(20),
        circuit_breaker_failures: 20, // Keep breaker open during retry loop
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    upload_tx.send(task).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify 4 attempts were made (1 primary + 3 DLQ retries)
    assert_eq!(
        backend.attempts_for("chunk_0000.ts"),
        4,
        "Worker must retry until mock failure resolves on attempt 4"
    );

    // Verify local file was removed after successful upload
    assert!(
        !chunk_path.exists(),
        "Local file must be deleted after DLQ retry succeeds with real rclone"
    );

    // Verify remote file was created by genuine rclone and contents match
    let remote_dest = remote_guard
        .path()
        .join("session_retry")
        .join("chunk_0000.ts");
    assert!(
        remote_dest.exists(),
        "Remote destination file must exist in local remote directory"
    );
    let uploaded_bytes = std::fs::read(&remote_dest).expect("failed to read remote file");
    assert_eq!(
        uploaded_bytes, payload,
        "Remote file content must match payload"
    );

    // Verify DLQ retry logs and completion event
    let mut saw_dlq_transfer = false;
    let mut saw_dlq_success = false;
    let mut saw_completed = false;

    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::UploadCompleted { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                saw_completed = true;
            }
            AppEvent::Log(entry) => {
                if entry
                    .message
                    .contains("[DLQ] Transferred chunk_0000.ts to DLQ")
                {
                    saw_dlq_transfer = true;
                }
                if entry
                    .message
                    .contains("[DLQ] Successfully uploaded chunk_0000.ts on retry 3")
                {
                    saw_dlq_success = true;
                }
            }
            _ => {}
        }
    }

    assert!(saw_dlq_transfer, "DLQ transfer log must be recorded");
    assert!(
        saw_dlq_success,
        "[DLQ] Successfully uploaded on retry 3 log must be recorded"
    );
    assert!(
        saw_completed,
        "UploadCompleted event must be recorded for chunk_0000.ts"
    );
}

#[tokio::test]
async fn test_real_rclone_dlq_disk_aware_eviction_drops_oldest_pair() {
    if !ensure_rclone_available() {
        eprintln!("Skipping rclone integration test: rclone binary not available");
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_disk_evict_src");
    let remote_guard = TempDirGuard::new("test_rclone_disk_evict_dst");

    // Chunk 0 pair: chunk_0000.ts + chat_0000.jsonl (oldest)
    let chunk0_ts = local_guard.path().join("chunk_0000.ts");
    let chunk0_chat = local_guard.path().join("chat_0000.jsonl");
    std::fs::write(&chunk0_ts, b"video-0000").expect("failed to write chunk0 ts");
    std::fs::write(&chunk0_chat, b"chat-0000").expect("failed to write chunk0 chat");

    // Chunk 1 pair: chunk_0001.ts + chat_0001.jsonl (newer)
    let chunk1_ts = local_guard.path().join("chunk_0001.ts");
    let chunk1_chat = local_guard.path().join("chat_0001.jsonl");
    std::fs::write(&chunk1_ts, b"video-0001").expect("failed to write chunk1 ts");
    std::fs::write(&chunk1_chat, b"chat-0001").expect("failed to write chunk1 chat");

    let task0 = UploadTask {
        channel_id: "ch_stream1".to_string(),
        session_folder_id: "session_evict".to_string(),
        remote_dir: "session_evict".to_string(),
        chunk_path: chunk0_ts.clone(),
        chunk_name: "chunk_0000.ts".to_string(),
        streamer_name: "StreamerA".to_string(),
    };

    let task1 = UploadTask {
        channel_id: "ch_stream1".to_string(),
        session_folder_id: "session_evict".to_string(),
        remote_dir: "session_evict".to_string(),
        chunk_path: chunk1_ts.clone(),
        chunk_name: "chunk_0001.ts".to_string(),
        streamer_name: "StreamerA".to_string(),
    };

    // Both tasks fail on attempt 1 to enter DLQ.
    // On attempt 2, mock failure is resolved, delegating to real rclone.
    let real_rclone = create_local_rclone_backend(remote_guard.path());
    let backend = Arc::new(ControlledRcloneBackend::new(
        real_rclone,
        |_path, attempt| attempt == 1,
    ));

    // Injected disk space provider: returns 0.5 GB on first check (< 2.0 GB threshold),
    // triggering eviction of the oldest pair, then 10.0 GB on subsequent checks.
    let disk_check_count = Arc::new(AtomicUsize::new(0));
    let disk_check_count_clone = disk_check_count.clone();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(20),
        min_free_disk_gb: 2.0,
        circuit_breaker_failures: 20,
        ..Default::default()
    }
    .with_disk_space_provider(move |_path| {
        let count = disk_check_count_clone.fetch_add(1, Ordering::SeqCst);
        if count == 0 { 0.5 } else { 10.0 }
    });

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    // Send task0 and task1
    upload_tx.send(task0).await.unwrap();
    upload_tx.send(task1).await.unwrap();
    drop(upload_tx);

    worker_handle.await.unwrap();

    // Verify Chunk 0 pair was permanently deleted from disk by disk-aware eviction
    assert!(
        !chunk0_ts.exists(),
        "Oldest chunk_0000.ts must be deleted from disk by disk eviction"
    );
    assert!(
        !chunk0_chat.exists(),
        "Coupled chat_0000.jsonl must be deleted from disk by disk eviction"
    );

    // Verify Chunk 0 was never uploaded to remote
    let remote_chunk0 = remote_guard
        .path()
        .join("session_evict")
        .join("chunk_0000.ts");
    assert!(
        !remote_chunk0.exists(),
        "Evicted chunk_0000.ts must not exist on remote"
    );

    // Verify Chunk 1 was spared, retried via real rclone, and deleted locally upon success
    assert!(
        !chunk1_ts.exists(),
        "Spared chunk_0001.ts must be deleted locally after confirmed rclone upload"
    );
    let remote_chunk1 = remote_guard
        .path()
        .join("session_evict")
        .join("chunk_0001.ts");
    assert!(
        remote_chunk1.exists(),
        "Spared chunk_0001.ts must exist on remote after successful rclone upload"
    );
    let uploaded_chunk1 = std::fs::read(&remote_chunk1).expect("failed to read remote chunk 1");
    assert_eq!(
        uploaded_chunk1, b"video-0001",
        "Uploaded chunk 1 content must match payload"
    );

    // Verify chat_0001.jsonl was not deleted (only chunk 0 pair was evicted)
    assert!(
        chunk1_chat.exists(),
        "chat_0001.jsonl was not evicted and must still exist"
    );

    // Verify event logs
    let mut saw_disk_eviction_log = false;
    let mut saw_chunk1_retry_success = false;

    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            if entry
                .message
                .contains("[DLQ] Disk space critically low (0.50 GB < 2.00 GB). Evicted oldest chunk chunk_0000.ts")
            {
                saw_disk_eviction_log = true;
            }
            if entry
                .message
                .contains("[DLQ] Successfully uploaded chunk_0001.ts on retry 1")
            {
                saw_chunk1_retry_success = true;
            }
        }
    }

    assert!(
        saw_disk_eviction_log,
        "Disk space critically low eviction log must be emitted for chunk_0000.ts"
    );
    assert!(
        saw_chunk1_retry_success,
        "Retry 1 success log must be emitted for spared chunk_0001.ts"
    );
}

#[tokio::test]
async fn test_real_rclone_subprocess_failure_enters_dlq() {
    if !ensure_rclone_available() {
        eprintln!("Skipping rclone integration test: rclone binary not available");
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_real_fail_src");
    let remote_guard = TempDirGuard::new("test_rclone_real_fail_dst");

    let chunk_path = local_guard.path().join("chunk_fail.ts");
    std::fs::write(&chunk_path, b"data-that-will-fail-on-rclone").expect("failed to write chunk");

    // Create a regular file at the path where rclone would try to create a directory.
    // In Windows / POSIX, creating a directory inside or under an existing file fails.
    let blocked_dir = remote_guard.path().join("blocked_session");
    std::fs::write(&blocked_dir, b"blocking-file-not-a-directory")
        .expect("failed to write blocker");

    let task = UploadTask {
        channel_id: "ch_stream1".to_string(),
        session_folder_id: "blocked_session".to_string(),
        remote_dir: "blocked_session".to_string(),
        chunk_path: chunk_path.clone(),
        chunk_name: "chunk_fail.ts".to_string(),
        streamer_name: "StreamerA".to_string(),
    };

    let backend = Arc::new(create_local_rclone_backend(remote_guard.path()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    // Set circuit breaker to 1 so the failure immediately pauses retries,
    // allowing the worker to exit cleanly when upload_tx is dropped.
    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(50),
        circuit_breaker_failures: 1,
        circuit_breaker_cooldown: Duration::from_secs(60),
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend), event_tx, upload_rx, 1, dlq_config);

    upload_tx.send(task).await.unwrap();

    // Wait until UploadFailed event is observed from the real rclone failure
    let mut saw_upload_failed = false;
    let mut saw_dlq_transferred = false;

    while let Some(ev) = event_rx.recv().await {
        match ev {
            AppEvent::UploadFailed { chunk_name, .. } if chunk_name == "chunk_fail.ts" => {
                saw_upload_failed = true;
            }
            AppEvent::Log(entry)
                if entry
                    .message
                    .contains("[DLQ] Transferred chunk_fail.ts to DLQ") =>
            {
                saw_dlq_transferred = true;
                // Task has successfully entered DLQ, we can close the queue and exit
                break;
            }
            _ => {}
        }
    }

    drop(upload_tx);
    worker_handle.abort();

    assert!(
        saw_upload_failed,
        "UploadFailed event must be emitted when real rclone subprocess fails"
    );
    assert!(
        saw_dlq_transferred,
        "Task must be transferred to DLQ upon real rclone subprocess failure"
    );
    assert!(
        chunk_path.exists(),
        "Local file must not be deleted when rclone fails"
    );
}
