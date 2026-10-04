use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::backend::{BoxFuture, ProgressCallback, UploadBackend};
use chzzk_load::uploader::worker::resolve_paired_paths;
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
    let task = UploadTask::chunk(
        "ch_test",
        "session_test",
        "session_test",
        path.clone(),
        file_name,
        "TestStreamer",
    );
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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

    // Send 5 tasks across 5 distinct channels so 5 consecutive failures occur.
    for i in 0..5 {
        let chunk_name = format!("chunk_{i:04}.ts");
        let chunk_path = temp_guard.path().join(&chunk_name);
        std::fs::write(&chunk_path, b"test-data").unwrap();

        upload_tx
            .send(UploadTask::chunk(
                format!("ch_{i}"),
                format!("session_{i}"),
                format!("remote_{i}"),
                chunk_path,
                chunk_name,
                format!("Streamer_{i}"),
            ))
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

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

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

#[test]
fn test_resolve_paired_paths() {
    let p1 = Path::new("/recordings/session1/chunk_0042.ts");
    let paired1 = resolve_paired_paths(p1);
    assert_eq!(
        paired1.ts_path,
        Path::new("/recordings/session1/chunk_0042.ts")
    );
    assert_eq!(
        paired1.jsonl_path,
        Path::new("/recordings/session1/chat_0042.jsonl")
    );

    let p2 = Path::new("/recordings/session1/chat_0042.jsonl");
    let paired2 = resolve_paired_paths(p2);
    assert_eq!(
        paired2.ts_path,
        Path::new("/recordings/session1/chunk_0042.ts")
    );
    assert_eq!(
        paired2.jsonl_path,
        Path::new("/recordings/session1/chat_0042.jsonl")
    );

    let p3 = Path::new("/recordings/session1/custom.ts");
    let paired3 = resolve_paired_paths(p3);
    assert_eq!(paired3.ts_path, Path::new("/recordings/session1/custom.ts"));
    assert_eq!(
        paired3.jsonl_path,
        Path::new("/recordings/session1/custom.jsonl")
    );
}

#[tokio::test(start_paused = true)]
async fn test_dlq_disk_aware_eviction_deletes_ts_and_jsonl_when_disk_low() {
    let temp_guard = TempDirGuard::new("test_dlq_disk_evict");
    let (task_ts, path_ts) = create_chunk_file(temp_guard.path(), "chunk_0000.ts", b"video-0000");
    let (task_chat, path_chat) =
        create_chunk_file(temp_guard.path(), "chat_0000.jsonl", b"chat-0000");

    // Backend always fails uploads so tasks divert to DLQ
    let backend = Arc::new(FlexibleMockBackend::new(|_path, _attempt| true));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    // Mock free disk space provider returning 0.5 GB, below the 2.0 GB threshold
    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(10),
        min_free_disk_gb: 2.0,
        ..Default::default()
    }
    .with_free_disk_gb(0.5);

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

    upload_tx.send(task_ts).await.unwrap();
    upload_tx.send(task_chat).await.unwrap();
    drop(upload_tx);

    worker_handle.await.unwrap();

    // Verify both files are permanently deleted from disk
    assert!(
        !path_ts.exists(),
        "Physical .ts file must be deleted upon disk-aware eviction"
    );
    assert!(
        !path_chat.exists(),
        "Coupled .jsonl chat log must be deleted upon disk-aware eviction"
    );

    // Verify warning log was emitted
    let mut saw_disk_eviction_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev
            && entry
                .message
                .contains("[DLQ] Disk space critically low (0.50 GB < 2.00 GB). Evicted oldest chunk chunk_0000.ts")
        {
            saw_disk_eviction_log = true;
        }
    }

    assert!(
        saw_disk_eviction_log,
        "DLQ disk eviction log must be emitted when disk space is below min_free_disk_gb"
    );
}

#[tokio::test(start_paused = true)]
async fn test_dlq_disk_aware_eviction_globally_evicts_oldest_first() {
    let temp_guard = TempDirGuard::new("test_dlq_global_oldest");
    let (task0_ts, path0_ts) = create_chunk_file(temp_guard.path(), "chunk_0000.ts", b"video-0000");
    let (_task0_chat, path0_chat) =
        create_chunk_file(temp_guard.path(), "chat_0000.jsonl", b"chat-0000");

    let (task1_ts, path1_ts) = create_chunk_file(temp_guard.path(), "chunk_0001.ts", b"video-0001");
    let (_task1_chat, _path1_chat) =
        create_chunk_file(temp_guard.path(), "chat_0001.jsonl", b"chat-0001");

    // Fail attempt 1 so tasks enter DLQ. On retry (attempt 2), allow chunk_0001.ts to succeed.
    let backend = Arc::new(FlexibleMockBackend::new(|path, attempt| {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        !(name == "chunk_0001.ts" && attempt >= 2)
    }));

    // Injected disk space provider: returns 0.5 GB while path0_ts remains on disk (< 2.0 GB threshold),
    // triggering eviction of chunk_0000, then 5.0 GB once chunk0 is evicted (recovering, so chunk_0001 is spared).
    let path0_ts_check = path0_ts.clone();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(10),
        min_free_disk_gb: 2.0,
        circuit_breaker_failures: 20,
        ..Default::default()
    }
    .with_disk_space_provider(move |_path| if path0_ts_check.exists() { 0.5 } else { 5.0 });

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

    upload_tx.send(task0_ts).await.unwrap();
    upload_tx.send(task1_ts).await.unwrap();
    drop(upload_tx);

    worker_handle.await.unwrap();

    // Chunk 0000 and coupled chat were evicted due to disk pressure
    assert!(
        !path0_ts.exists(),
        "Oldest chunk_0000.ts must be evicted under disk pressure"
    );
    assert!(
        !path0_chat.exists(),
        "Oldest chat_0000.jsonl must be evicted under disk pressure"
    );

    // Chunk 0001 was uploaded on retry 2 and deleted after successful upload
    assert!(
        !path1_ts.exists(),
        "Spared chunk_0001.ts must be deleted after successful retry upload"
    );

    // Chunk 0001 was NOT evicted by disk pressure; it succeeded on retry 2 and was deleted by backend
    let mut saw_chunk0_eviction = false;
    let mut saw_chunk1_retry_success = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            if entry.message.contains("[DLQ] Disk space critically low")
                && entry.message.contains("chunk_0000.ts")
            {
                saw_chunk0_eviction = true;
            }
            if entry
                .message
                .contains("[DLQ] Successfully uploaded chunk_0001.ts on retry 1")
            {
                saw_chunk1_retry_success = true;
            }
        }
    }

    assert!(saw_chunk0_eviction, "Chunk 0 eviction log must be emitted");
    assert!(
        saw_chunk1_retry_success,
        "Chunk 1 must successfully retry after disk space recovers, not be falsely evicted"
    );
}

#[tokio::test]
async fn test_upload_worker_metadata_snapshot_retention_leaves_file_intact_and_logs_synced() {
    let temp_guard = TempDirGuard::new("test_worker_retention");
    let meta_path = temp_guard.path().join("metadata.jsonl");
    std::fs::write(&meta_path, b"{\"event\":\"INITIAL_STATE\"}\n").unwrap();

    let backend = Arc::new(FlexibleMockBackend::new(|_, _| false));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(50);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(10);

    let worker_handle = UploadWorker::spawn(Some(backend.clone()), event_tx, upload_rx);

    let task = UploadTask::metadata(
        "chan_ret",
        "session_ret",
        "session_ret",
        meta_path.clone(),
        "StreamerRet",
        false, // delete_on_success = false (retention)
    );

    upload_tx.send(task).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify local file is intact
    assert!(
        meta_path.exists(),
        "metadata.jsonl must remain on disk when delete_on_success is false"
    );

    // Verify backend received the upload
    assert!(backend.is_uploaded(&meta_path));

    // Verify telemetry logs and events
    let mut saw_synced_log = false;
    let mut saw_upload_completed = false;
    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::Log(entry) => {
                if entry
                    .message
                    .contains("[StreamerRet] Uploaded metadata.jsonl (synced)")
                {
                    saw_synced_log = true;
                }
            }
            AppEvent::UploadCompleted {
                channel_id,
                chunk_name,
                reclaimed_bytes,
            } => {
                assert_eq!(channel_id, "chan_ret");
                assert_eq!(chunk_name, "metadata.jsonl");
                assert_eq!(
                    reclaimed_bytes, 0,
                    "reclaimed_bytes must be 0 for retained files"
                );
                saw_upload_completed = true;
            }
            _ => {}
        }
    }

    assert!(
        saw_synced_log,
        "Must emit '[StreamerRet] Uploaded metadata.jsonl (synced)'"
    );
    assert!(
        saw_upload_completed,
        "Must emit UploadCompleted with 0 reclaimed bytes"
    );
}

#[tokio::test]
async fn test_upload_worker_chunk_deletion_unlinks_file_and_logs_reclaimed() {
    let temp_guard = TempDirGuard::new("test_worker_deletion");
    let chunk_path = temp_guard.path().join("chunk_0001.ts");
    let data = vec![0u8; 1024 * 1024]; // 1 MB
    std::fs::write(&chunk_path, &data).unwrap();

    let backend = Arc::new(FlexibleMockBackend::new(|_, _| false));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(50);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(10);

    let worker_handle = UploadWorker::spawn(Some(backend.clone()), event_tx, upload_rx);

    let task = UploadTask::chunk(
        "chan_del",
        "session_del",
        "session_del",
        chunk_path.clone(),
        "chunk_0001.ts",
        "StreamerDel",
    );

    upload_tx.send(task).await.unwrap();
    drop(upload_tx);
    worker_handle.await.unwrap();

    // Verify local file was unlinked
    assert!(
        !chunk_path.exists(),
        "chunk file must be unlinked when delete_on_success is true"
    );

    // Verify backend received the upload
    assert!(backend.is_uploaded(&chunk_path));

    // Verify telemetry logs and events
    let mut saw_reclaimed_log = false;
    let mut saw_upload_completed = false;
    while let Ok(ev) = event_rx.try_recv() {
        match ev {
            AppEvent::Log(entry) => {
                if entry
                    .message
                    .contains("[StreamerDel] Uploaded & deleted chunk_0001.ts (reclaimed 1.0 MB)")
                {
                    saw_reclaimed_log = true;
                }
            }
            AppEvent::UploadCompleted {
                channel_id,
                chunk_name,
                reclaimed_bytes,
            } => {
                assert_eq!(channel_id, "chan_del");
                assert_eq!(chunk_name, "chunk_0001.ts");
                assert_eq!(reclaimed_bytes, 1024 * 1024);
                saw_upload_completed = true;
            }
            _ => {}
        }
    }

    assert!(
        saw_reclaimed_log,
        "Must emit '[StreamerDel] Uploaded & deleted chunk_0001.ts (reclaimed 1.0 MB)'"
    );
    assert!(
        saw_upload_completed,
        "Must emit UploadCompleted with reclaimed bytes"
    );
}

#[tokio::test(start_paused = true)]
async fn test_dlq_disk_aware_eviction_strictly_preserves_metadata_tasks() {
    let temp_guard = TempDirGuard::new("test_dlq_meta_immunity");
    let meta_path = temp_guard.path().join("metadata.jsonl");
    std::fs::write(&meta_path, b"{\"event\":\"INITIAL_STATE\"}\n").unwrap();

    let (task_ts, path_ts) = create_chunk_file(temp_guard.path(), "chunk_0001.ts", b"video-0001");
    let task_meta = UploadTask::metadata(
        "ch_meta",
        "session_meta",
        "session_meta",
        meta_path.clone(),
        "StreamerMeta",
        false,
    );

    // Fail attempt 1 so tasks enter DLQ. On retry (attempt 2), allow metadata.jsonl to succeed.
    let backend = Arc::new(FlexibleMockBackend::new(|path, attempt| {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        !(name == "metadata.jsonl" && attempt >= 2)
    }));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    // Critically low disk: 0.5 GB < 2.0 GB
    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(10),
        min_free_disk_gb: 2.0,
        ..Default::default()
    }
    .with_free_disk_gb(0.5);

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

    // Send metadata task first (older), then chunk task
    upload_tx.send(task_meta).await.unwrap();
    upload_tx.send(task_ts).await.unwrap();

    drop(upload_tx);
    worker_handle.await.unwrap();

    // Chunk should be evicted due to disk pressure
    assert!(
        !path_ts.exists(),
        "chunk_0001.ts should be evicted under low disk pressure"
    );

    // metadata.jsonl MUST NOT be evicted or deleted despite low disk pressure and being older
    assert!(
        meta_path.exists(),
        "metadata.jsonl must strictly be immune from DLQ disk eviction"
    );

    let mut saw_chunk_eviction = false;
    let mut saw_meta_eviction = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            if entry.message.contains("[DLQ] Disk space critically low") {
                if entry.message.contains("chunk_0001.ts") {
                    saw_chunk_eviction = true;
                }
                if entry.message.contains("metadata.jsonl") {
                    saw_meta_eviction = true;
                }
            }
        }
    }

    assert!(
        saw_chunk_eviction,
        "chunk_0001.ts eviction log must be emitted"
    );
    assert!(
        !saw_meta_eviction,
        "metadata.jsonl must NEVER be logged as evicted"
    );
}
