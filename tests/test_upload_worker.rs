use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::backend::{BoxFuture, ProgressCallback, UploadBackend};
use chzzk_load::uploader::{DlqConfig, UploadTask, UploadWorker};

/// RAII helper to clean up temporary test directories.
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

type FailPredicate = Arc<dyn Fn(&Path, usize) -> bool + Send + Sync>;

/// In-memory mock upload backend configurable with custom failure decisions per chunk and attempt.
struct FlexibleMockBackend {
    uploads: Arc<Mutex<Vec<(PathBuf, String)>>>,
    attempts: Arc<Mutex<HashMap<String, usize>>>,
    fail_predicate: FailPredicate,
    connection_ok: Arc<AtomicBool>,
}

impl FlexibleMockBackend {
    fn new<F>(fail_predicate: F) -> Self
    where
        F: Fn(&Path, usize) -> bool + Send + Sync + 'static,
    {
        Self {
            uploads: Arc::new(Mutex::new(Vec::new())),
            attempts: Arc::new(Mutex::new(HashMap::new())),
            fail_predicate: Arc::new(fail_predicate),
            connection_ok: Arc::new(AtomicBool::new(true)),
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

    fn is_uploaded(&self, path: &Path) -> bool {
        self.uploads.lock().unwrap().iter().any(|(p, _)| p == path)
    }
}

impl UploadBackend for FlexibleMockBackend {
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
                anyhow::bail!("Simulated failure for {file_name} on attempt {attempt}");
            }

            let len = if local_path.exists() {
                let bytes = std::fs::metadata(local_path).map(|m| m.len()).unwrap_or(0);
                on_progress(bytes, bytes, 10.0);
                let _ = std::fs::remove_file(local_path);
                bytes
            } else {
                1024
            };

            self.uploads
                .lock()
                .unwrap()
                .push((local_path.to_path_buf(), remote_dir.to_string()));

            Ok(len)
        })
    }

    fn upload_text<'a>(
        &'a self,
        _remote_dir: &'a str,
        _file_name: &'a str,
        _content: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.connection_ok.load(Ordering::SeqCst) {
                Ok(())
            } else {
                anyhow::bail!("Remote connection check failed");
            }
        })
    }
}

fn create_chunk_file(dir: &Path, file_name: &str, content: &[u8]) -> (UploadTask, PathBuf) {
    let path = dir.join(file_name);
    std::fs::write(&path, content).expect("failed to write test chunk");
    let task = UploadTask {
        channel_id: "ch_test".to_string(),
        session_folder_id: "session_test".to_string(),
        remote_dir: "session_test".to_string(),
        chunk_path: path.clone(),
        chunk_name: file_name.to_string(),
        streamer_name: "TestStreamer".to_string(),
    };
    (task, path)
}

#[tokio::test]
async fn test_dlq_non_blocking_primary_queue_advances() {
    let temp_guard = TempDirGuard::new("test_dlq_advance");
    let (task0, path0) = create_chunk_file(temp_guard.path(), "chunk_0000.ts", b"video-0000");
    let (task1, path1) = create_chunk_file(temp_guard.path(), "chunk_0001.ts", b"video-0001");

    // Configure backend: chunk_0000.ts fails on attempt 1, but chunk_0001.ts succeeds on attempt 1.
    // On attempt 2, chunk_0000.ts succeeds so the worker can eventually terminate cleanly.
    let backend = Arc::new(FlexibleMockBackend::new(|path, attempt| {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        name == "chunk_0000.ts" && attempt == 1
    }));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(50),
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    // Send chunk_0000.ts and chunk_0001.ts for the same channel.
    upload_tx.send(task0).await.unwrap();
    upload_tx.send(task1).await.unwrap();

    // Drop upload_tx so worker exits once all tasks (primary and DLQ) complete.
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify chunk_0001.ts was uploaded and deleted immediately by the primary queue.
    assert!(
        !path1.exists(),
        "chunk_0001.ts must be deleted upon confirmed upload"
    );
    assert!(
        backend.is_uploaded(&path1),
        "chunk_0001.ts must be marked uploaded in backend"
    );

    // Also verify chunk_0000.ts eventually succeeded on DLQ retry and was deleted.
    assert!(
        !path0.exists(),
        "chunk_0000.ts must be deleted after DLQ retry succeeds"
    );
    assert_eq!(backend.attempts_for("chunk_0000.ts"), 2);
    assert_eq!(backend.attempts_for("chunk_0001.ts"), 1);

    // Collect events and verify ordering: chunk_0001.ts completed BEFORE chunk_0000.ts retry completed.
    let mut completion_order = Vec::new();
    let mut saw_dlq_transfer_0 = false;
    let mut saw_dlq_success_0 = false;

    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::UploadCompleted { chunk_name, .. } => {
                completion_order.push(chunk_name);
            }
            AppEvent::Log(entry) => {
                if entry
                    .message
                    .contains("[DLQ] Transferred chunk_0000.ts to DLQ")
                {
                    saw_dlq_transfer_0 = true;
                }
                if entry
                    .message
                    .contains("[DLQ] Successfully uploaded chunk_0000.ts on retry 1")
                {
                    saw_dlq_success_0 = true;
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_dlq_transfer_0,
        "chunk_0000.ts must be transferred to DLQ upon failure"
    );
    assert!(saw_dlq_success_0, "chunk_0000.ts must log successful retry");
    assert_eq!(
        completion_order,
        vec!["chunk_0001.ts".to_string(), "chunk_0000.ts".to_string()],
        "chunk_0001.ts must complete on the primary queue before chunk_0000.ts DLQ retry"
    );
}

#[tokio::test]
async fn test_dlq_retries_and_succeeds() {
    let temp_guard = TempDirGuard::new("test_dlq_retry");
    let (task0, path0) = create_chunk_file(temp_guard.path(), "chunk_0000.ts", b"video-0000");

    // Backend fails chunk_0000.ts on attempt 1, but succeeds on attempt 2 (DLQ retry).
    let backend = Arc::new(FlexibleMockBackend::new(|_path, attempt| attempt == 1));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(15),
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    upload_tx.send(task0).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify chunk_0000.ts was retried and deleted upon success.
    assert!(
        !path0.exists(),
        "chunk_0000.ts must be deleted after DLQ retry succeeds"
    );
    assert_eq!(backend.attempts_for("chunk_0000.ts"), 2);
    assert!(backend.is_uploaded(&path0));

    // Verify UploadCompleted and [DLQ] Successfully uploaded logs were emitted.
    let mut saw_completed = false;
    let mut saw_dlq_success = false;

    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::UploadCompleted { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                saw_completed = true;
            }
            AppEvent::Log(entry)
                if entry
                    .message
                    .contains("[DLQ] Successfully uploaded chunk_0000.ts on retry 1") =>
            {
                saw_dlq_success = true;
            }
            _ => {}
        }
    }

    assert!(
        saw_completed,
        "UploadCompleted event must be emitted for chunk_0000.ts"
    );
    assert!(
        saw_dlq_success,
        "[DLQ] Successfully uploaded log must be emitted"
    );
}

#[tokio::test(start_paused = true)]
async fn test_dlq_infinite_retry_does_not_drop_tasks() {
    let temp_guard = TempDirGuard::new("test_dlq_infinite");
    let (task_fail, path_fail) =
        create_chunk_file(temp_guard.path(), "chunk_fail.ts", b"video-infinite");

    // Fails attempts 1 through 5 (1 primary + 4 DLQ retries), then succeeds on attempt 6 (retry 5).
    // In the old implementation with max_retries: 3, it would have been dropped after attempt 4 (retry 3).
    let backend = Arc::new(FlexibleMockBackend::new(|_path, attempt| attempt < 6));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(10),
        circuit_breaker_failures: 20, // Prevent circuit breaker from tripping during retry progression
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    upload_tx.send(task_fail).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify chunk_fail.ts was uploaded on attempt 6 and deleted after success.
    assert!(
        !path_fail.exists(),
        "chunk_fail.ts must be deleted after DLQ retry succeeds on attempt 6"
    );
    assert_eq!(backend.attempts_for("chunk_fail.ts"), 6);

    let mut saw_retry_5_success = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev
            && entry
                .message
                .contains("[DLQ] Successfully uploaded chunk_fail.ts on retry 5")
        {
            saw_retry_5_success = true;
        }
    }

    assert!(
        saw_retry_5_success,
        "Success log on retry 5 must be emitted, proving retries loop beyond the old 3-attempt limit"
    );
}

#[tokio::test(start_paused = true)]
async fn test_dlq_backoff_capped_at_maximum() {
    let temp_guard = TempDirGuard::new("test_dlq_cap");
    let (task, path) = create_chunk_file(temp_guard.path(), "chunk_cap.ts", b"video-cap");

    // Fail 4 attempts (1 primary + 3 DLQ retries), then succeed on attempt 5 (retry 4).
    let backend = Arc::new(FlexibleMockBackend::new(|_path, attempt| attempt < 5));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    // Initial delay is 1s. Exponential progression: 1s, 2s, 4s, 8s...
    // With max_backoff = 3s:
    // Primary fail -> retry 1 in 1s
    // Retry 1 fail -> retry 2 in 2s
    // Retry 2 fail -> retry 3 in min(4s, 3s) = 3s
    // Retry 3 fail -> retry 4 in min(8s, 3s) = 3s
    let dlq_config = DlqConfig {
        initial_delay: Duration::from_secs(1),
        max_backoff: Duration::from_secs(3),
        circuit_breaker_failures: 20,
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    upload_tx.send(task).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    assert!(!path.exists());
    assert_eq!(backend.attempts_for("chunk_cap.ts"), 5);

    let mut logs = Vec::new();
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            logs.push(entry.message);
        }
    }

    // Check logs for scheduled retries
    assert!(
        logs.iter()
            .any(|m| m.contains("[DLQ] Transferred chunk_cap.ts to DLQ (retry 1 in 1s)")),
        "Expected retry 1 in 1s log, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|m| m.contains("[DLQ] Retry 1 failed for chunk_cap.ts. Scheduled retry 2 in 2s")),
        "Expected retry 2 in 2s log, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|m| m.contains("[DLQ] Retry 2 failed for chunk_cap.ts. Scheduled retry 3 in 3s")),
        "Expected retry 3 capped at 3s log, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|m| m.contains("[DLQ] Retry 3 failed for chunk_cap.ts. Scheduled retry 4 in 3s")),
        "Expected retry 4 capped at 3s log, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|m| m.contains("[DLQ] Successfully uploaded chunk_cap.ts on retry 4")),
        "Expected retry 4 success log, got: {logs:?}"
    );
}

#[tokio::test]
async fn test_circuit_breaker_trips_on_consecutive_failures() {
    let temp_guard = TempDirGuard::new("test_dlq_breaker");

    // Backend fails chunk on attempt 1, but succeeds on retry (attempt 2).
    let backend = Arc::new(FlexibleMockBackend::new(|_path, attempt| attempt == 1));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        circuit_breaker_failures: 5,
        circuit_breaker_cooldown: Duration::from_millis(50),
        initial_delay: Duration::from_millis(10),
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    // Send 5 tasks across 5 distinct channels so 5 consecutive failures occur.
    for i in 0..5 {
        let chunk_name = format!("chunk_{i:04}.ts");
        let chunk_path = temp_guard.path().join(&chunk_name);
        std::fs::write(&chunk_path, b"test-data").unwrap();

        upload_tx
            .send(UploadTask {
                channel_id: format!("ch_{i}"),
                session_folder_id: format!("session_{i}"),
                remote_dir: format!("remote_{i}"),
                chunk_path,
                chunk_name,
                streamer_name: format!("Streamer_{i}"),
            })
            .await
            .unwrap();
    }

    drop(upload_tx);
    worker_handle.await.unwrap();

    let mut saw_breaker_trip = false;
    let mut saw_breaker_recovery = false;

    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            if entry
                .message
                .contains("[CIRCUIT BREAKER] 5 consecutive upload failures detected")
            {
                saw_breaker_trip = true;
            }
            if entry
                .message
                .contains("[CIRCUIT BREAKER] Remote health check succeeded. Resuming upload queue.")
            {
                saw_breaker_recovery = true;
            }
        }
    }

    assert!(
        saw_breaker_trip,
        "Circuit breaker trip warning must be emitted on 5 consecutive failures"
    );
    assert!(
        saw_breaker_recovery,
        "Circuit breaker recovery info log must be emitted after cooldown"
    );
}

#[tokio::test]
async fn test_dlq_capacity_eviction_drops_oldest_task() {
    let temp_guard = TempDirGuard::new("test_dlq_capacity");
    let (task0, _path0) = create_chunk_file(temp_guard.path(), "chunk_0000.ts", b"video-0");
    let (task1, _path1) = create_chunk_file(temp_guard.path(), "chunk_0001.ts", b"video-1");
    let (task2, _path2) = create_chunk_file(temp_guard.path(), "chunk_0002.ts", b"video-2");

    // Backend fails on attempt 1, but succeeds on attempt 2 (retry).
    let backend = Arc::new(FlexibleMockBackend::new(|_path, attempt| attempt == 1));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(20),
        max_tasks_per_channel: 2, // Cap at 2 tasks per channel
        circuit_breaker_failures: 10,
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend.clone()), event_tx, upload_rx, 1, dlq_config);

    // Send 3 tasks for the same channel "ch_test"
    upload_tx.send(task0).await.unwrap();
    upload_tx.send(task1).await.unwrap();
    upload_tx.send(task2).await.unwrap();

    drop(upload_tx);
    worker_handle.await.unwrap();

    let mut saw_eviction = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev
            && entry.message.contains("[DLQ] DLQ capacity reached (2) for channel ch_test. Evicting oldest task chunk_0000.ts.")
        {
            saw_eviction = true;
        }
    }

    assert!(
        saw_eviction,
        "Eviction log for oldest task chunk_0000.ts must be emitted when DLQ capacity is reached"
    );
}
