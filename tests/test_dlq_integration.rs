use std::collections::HashMap;
use std::path::{Path, PathBuf};
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

/// Helper to create a standard `UploadTask` for testing.
fn make_test_task(session_id: &str, chunk_path: PathBuf, chunk_name: &str) -> UploadTask {
    UploadTask::chunk(
        "ch_stream1",
        session_id,
        session_id,
        chunk_path,
        chunk_name,
        "StreamerA",
    )
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
                anyhow::bail!("Mock injected failure on attempt {attempt} for {file_name}");
            }

            self.inner
                .upload_file(local_path, remote_dir, on_progress)
                .await
        })
    }

    fn check_connection<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<()>> {
        self.inner.check_connection()
    }
}

#[tokio::test]
async fn test_real_rclone_local_remote_basic_upload() {
    if !ensure_rclone_available() {
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_basic_src");
    let remote_guard = TempDirGuard::new("test_rclone_basic_dst");

    let chunk_path = local_guard.path().join("chunk_0000.ts");
    let payload = b"video-stream-data-chunk-0";
    std::fs::write(&chunk_path, payload).expect("failed to write local chunk");

    let task = make_test_task("session_20261002", chunk_path.clone(), "chunk_0000.ts");

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
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_dlq_retry_src");
    let remote_guard = TempDirGuard::new("test_rclone_dlq_retry_dst");

    let chunk_path = local_guard.path().join("chunk_0000.ts");
    let payload = b"retryable-chunk-data";
    std::fs::write(&chunk_path, payload).expect("failed to write local chunk");

    let task = make_test_task("session_retry", chunk_path.clone(), "chunk_0000.ts");

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
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_disk_evict_src");
    let remote_guard = TempDirGuard::new("test_rclone_disk_evict_dst");

    // Metadata task (oldest, enqueued first): should strictly be immune to disk eviction
    let meta_path = local_guard.path().join("metadata.jsonl");
    let meta_payload = b"{\"version\":2,\"event\":\"INITIAL_STATE\",\"stream_offset_ms\":0}\n";
    std::fs::write(&meta_path, meta_payload).expect("failed to write meta");
    let task_meta = UploadTask::metadata(
        "ch_stream1",
        "session_evict",
        "session_evict",
        meta_path.clone(),
        "StreamerA",
        false,
    );

    // Chunk 0 pair: chunk_0000.ts + chat_0000.jsonl (oldest media pair)
    let chunk0_ts = local_guard.path().join("chunk_0000.ts");
    let chunk0_chat = local_guard.path().join("chat_0000.jsonl");
    std::fs::write(&chunk0_ts, b"video-0000").expect("failed to write chunk0 ts");
    std::fs::write(&chunk0_chat, b"chat-0000").expect("failed to write chunk0 chat");

    // Chunk 1 pair: chunk_0001.ts + chat_0001.jsonl (newer media pair)
    let chunk1_ts = local_guard.path().join("chunk_0001.ts");
    let chunk1_chat = local_guard.path().join("chat_0001.jsonl");
    std::fs::write(&chunk1_ts, b"video-0001").expect("failed to write chunk1 ts");
    std::fs::write(&chunk1_chat, b"chat-0001").expect("failed to write chunk1 chat");

    let task0_ts = make_test_task("session_evict", chunk0_ts.clone(), "chunk_0000.ts");
    let task0_chat = make_test_task("session_evict", chunk0_chat.clone(), "chat_0000.jsonl");
    let task1_ts = make_test_task("session_evict", chunk1_ts.clone(), "chunk_0001.ts");
    let task1_chat = make_test_task("session_evict", chunk1_chat.clone(), "chat_0001.jsonl");

    // All tasks fail on attempt 1 to enter DLQ.
    // On attempt 2, mock failure is resolved, delegating to real rclone.
    let real_rclone = create_local_rclone_backend(remote_guard.path());
    let backend = Arc::new(ControlledRcloneBackend::new(
        real_rclone,
        |_path, attempt| attempt == 1,
    ));

    // Injected disk space provider: returns 0.5 GB while chunk0_ts remains on disk (< 2.0 GB threshold),
    // triggering eviction of the oldest pair, then 10.0 GB once chunk0 is evicted.
    let chunk0_ts_check = chunk0_ts.clone();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(20),
        min_free_disk_gb: 2.0,
        circuit_breaker_failures: 20,
        ..Default::default()
    }
    .with_disk_space_provider(
        move |_path| {
            if chunk0_ts_check.exists() { 0.5 } else { 10.0 }
        },
    );

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend.clone()),
        event_tx,
        upload_rx,
        1,
        dlq_config,
        None,
    );

    // Enqueue metadata first, then video and coupled chat tasks for both chunks
    upload_tx.send(task_meta).await.unwrap();
    upload_tx.send(task0_ts).await.unwrap();
    upload_tx.send(task0_chat).await.unwrap();
    upload_tx.send(task1_ts).await.unwrap();
    upload_tx.send(task1_chat).await.unwrap();
    drop(upload_tx);

    worker_handle.await.unwrap();

    // Verify metadata was spared from eviction, uploaded via real rclone, and retained locally
    assert!(
        meta_path.exists(),
        "metadata.jsonl must strictly be immune from DLQ eviction and remain on disk"
    );
    let remote_meta = remote_guard
        .path()
        .join("session_evict")
        .join("metadata.jsonl");
    assert!(
        remote_meta.exists(),
        "metadata.jsonl must exist on remote after real rclone upload"
    );
    assert_eq!(
        std::fs::read(&remote_meta).expect("read remote meta"),
        meta_payload
    );

    // Verify Chunk 0 pair was permanently deleted from disk by disk-aware eviction
    assert!(
        !chunk0_ts.exists(),
        "Oldest chunk_0000.ts must be deleted from disk by disk eviction"
    );
    assert!(
        !chunk0_chat.exists(),
        "Coupled chat_0000.jsonl must be deleted from disk by disk eviction"
    );

    // Verify Chunk 0 pair was never uploaded to remote
    let remote_chunk0_ts = remote_guard
        .path()
        .join("session_evict")
        .join("chunk_0000.ts");
    let remote_chunk0_chat = remote_guard
        .path()
        .join("session_evict")
        .join("chat_0000.jsonl");
    assert!(
        !remote_chunk0_ts.exists(),
        "Evicted chunk_0000.ts must not exist on remote"
    );
    assert!(
        !remote_chunk0_chat.exists(),
        "Evicted chat_0000.jsonl must not exist on remote"
    );

    // Verify Chunk 1 pair was spared, retried via real rclone, and deleted locally upon success
    assert!(
        !chunk1_ts.exists(),
        "Spared chunk_0001.ts must be deleted locally after confirmed rclone upload"
    );
    assert!(
        !chunk1_chat.exists(),
        "Spared chat_0001.jsonl must be deleted locally after confirmed rclone upload"
    );

    let remote_chunk1_ts = remote_guard
        .path()
        .join("session_evict")
        .join("chunk_0001.ts");
    let remote_chunk1_chat = remote_guard
        .path()
        .join("session_evict")
        .join("chat_0001.jsonl");

    assert!(
        remote_chunk1_ts.exists(),
        "Spared chunk_0001.ts must exist on remote after successful rclone upload"
    );
    assert!(
        remote_chunk1_chat.exists(),
        "Spared chat_0001.jsonl must exist on remote after successful rclone upload"
    );

    let uploaded_chunk1_ts =
        std::fs::read(&remote_chunk1_ts).expect("failed to read remote chunk 1 ts");
    assert_eq!(
        uploaded_chunk1_ts, b"video-0001",
        "Uploaded chunk 1 ts content must match payload"
    );
    let uploaded_chunk1_chat =
        std::fs::read(&remote_chunk1_chat).expect("failed to read remote chunk 1 chat");
    assert_eq!(
        uploaded_chunk1_chat, b"chat-0001",
        "Uploaded chunk 1 chat content must match payload"
    );

    // Verify event logs
    let mut saw_disk_eviction_log = false;
    let mut saw_chunk1_retry_success = false;
    let mut saw_meta_retry_success = false;

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
            if entry
                .message
                .contains("[DLQ] Successfully uploaded metadata.jsonl on retry 1")
            {
                saw_meta_retry_success = true;
            }
            assert!(
                !entry
                    .message
                    .contains("Evicted oldest chunk metadata.jsonl"),
                "metadata.jsonl must strictly be immune from DLQ eviction"
            );
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
    assert!(
        saw_meta_retry_success,
        "Retry 1 success log must be emitted for spared metadata.jsonl"
    );
}

#[tokio::test]
async fn test_real_rclone_metadata_snapshot_live_sync_and_teardown_retention() {
    if !ensure_rclone_available() {
        return;
    }

    let local_guard = TempDirGuard::new("test_rclone_meta_src");
    let remote_guard = TempDirGuard::new("test_rclone_meta_dst");

    let meta_path = local_guard.path().join("metadata.jsonl");
    let payload_initial = b"{\"version\":2,\"event\":\"INITIAL_STATE\",\"stream_offset_ms\":0}\n";
    std::fs::write(&meta_path, payload_initial).expect("failed to write initial metadata");

    let task_initial = UploadTask::metadata(
        "ch_meta_test",
        "session_meta_1",
        "session_meta_1",
        meta_path.clone(),
        "MetaStreamer",
        false, // live sync snapshot: retain on disk
    );

    let backend = Arc::new(create_local_rclone_backend(remote_guard.path()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);
    let drain_notify = Arc::new(tokio::sync::Notify::new());

    let worker_handle = UploadWorker::spawn_with_options(
        Some(backend),
        event_tx,
        upload_rx,
        1,
        DlqConfig::default(),
        Some(drain_notify.clone()),
    );

    // 1. Send live sync metadata snapshot
    upload_tx.send(task_initial).await.unwrap();

    // Wait until upload completed event or synced log
    let mut saw_synced_log = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(AppEvent::Log(entry))) =
            tokio::time::timeout(Duration::from_millis(200), event_rx.recv()).await
        {
            if entry
                .message
                .contains("[MetaStreamer] Uploaded metadata.jsonl (synced)")
            {
                saw_synced_log = true;
                break;
            }
        }
    }
    assert!(saw_synced_log, "Uploaded (synced) log must be recorded");

    // Local metadata.jsonl must remain on disk
    assert!(
        meta_path.exists(),
        "metadata.jsonl must remain on disk after live sync snapshot"
    );

    // Remote file must exist and match payload
    let remote_meta = remote_guard
        .path()
        .join("session_meta_1")
        .join("metadata.jsonl");
    assert!(
        remote_meta.exists(),
        "Remote metadata.jsonl must exist after real rclone upload"
    );
    assert_eq!(
        std::fs::read(&remote_meta).expect("read remote meta"),
        payload_initial
    );

    // 2. Append teardown event to metadata.jsonl
    let payload_teardown =
        b"{\"version\":2,\"event\":\"METADATA_CHANGED\",\"stream_offset_ms\":5000}\n";
    let mut full_payload = payload_initial.to_vec();
    full_payload.extend_from_slice(payload_teardown);
    std::fs::write(&meta_path, &full_payload).expect("write updated metadata");

    let task_final = UploadTask::metadata(
        "ch_meta_test",
        "session_meta_1",
        "session_meta_1",
        meta_path.clone(),
        "MetaStreamer",
        true, // final teardown snapshot: delete on success
    );

    upload_tx.send(task_final).await.unwrap();
    drop(upload_tx);

    worker_handle.await.unwrap();

    // Local metadata.jsonl must be unlinked after confirmed teardown upload
    assert!(
        !meta_path.exists(),
        "metadata.jsonl must be deleted locally after final teardown upload"
    );

    // Remote file must match updated payload
    assert_eq!(
        std::fs::read(&remote_meta).expect("read remote meta final"),
        full_payload,
        "Remote metadata.jsonl must reflect updated content"
    );

    let mut saw_deleted_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev {
            if entry
                .message
                .contains("[MetaStreamer] Uploaded & deleted metadata.jsonl")
            {
                saw_deleted_log = true;
            }
        }
    }
    assert!(saw_deleted_log, "Uploaded & deleted log must be recorded");
}

#[tokio::test]
async fn test_real_rclone_subprocess_failure_enters_dlq() {
    if !ensure_rclone_available() {
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

    let task = make_test_task("blocked_session", chunk_path.clone(), "chunk_fail.ts");

    let backend = Arc::new(create_local_rclone_backend(remote_guard.path()));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

    // Configure circuit breaker to pause retries after the first failure.
    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(50),
        circuit_breaker_failures: 1,
        circuit_breaker_cooldown: Duration::from_secs(60),
        ..Default::default()
    };

    let worker_handle =
        UploadWorker::spawn_with_options(Some(backend), event_tx, upload_rx, 1, dlq_config, None);

    upload_tx.send(task).await.unwrap();

    // Wait until UploadFailed and DLQ transfer events are observed from the real rclone failure
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
                // Once DLQ transfer is observed, the test goal is satisfied.
                break;
            }
            _ => {}
        }
    }

    drop(upload_tx);
    // Abort the worker since the unresolvable task remains queued in DLQ
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
