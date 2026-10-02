use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;

use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::engine::reconciliation::reconcile_orphaned_sessions;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::{DlqConfig, MockUploadBackend, UploadTask, UploadWorker};

/// RAII guard to create and clean up an isolated temporary directory within `std::env::temp_dir()`.
struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "{prefix}_{}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            rand::random::<u32>()
        ));
        fs::create_dir_all(&path).expect("failed to create temporary test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[tokio::test]
async fn test_reconciliation_enqueues_sealed_and_quarantines_tail() {
    let guard = TempDirGuard::new("test_reconcile_sealed");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    let chunk0 = session_dir.join("chunk_0000.ts");
    let chunk1 = session_dir.join("chunk_0001.ts");
    let chunk2 = session_dir.join("chunk_0002.ts");
    let chat0 = session_dir.join("chat_0000.jsonl");

    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");
    fs::write(&chunk1, vec![0u8; 1024]).expect("write chunk 1");
    fs::write(&chunk2, vec![0u8; 512]).expect("write chunk 2");
    fs::write(&chat0, vec![0u8; 256]).expect("write chat 0");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("test_channel", "TestStreamer")],
        ..Default::default()
    };

    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    // Verify report statistics
    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 3);
    assert_eq!(report.chunks_quarantined, 1);

    // Verify chunk 2 (tail) was renamed to chunk_0002.ts.quarantine
    assert!(
        !chunk2.exists(),
        "chunk_0002.ts should no longer exist under original name"
    );
    let chunk2_quarantine = session_dir.join("chunk_0002.ts.quarantine");
    assert!(
        chunk2_quarantine.exists(),
        "chunk_0002.ts.quarantine should exist"
    );
    assert_eq!(
        fs::metadata(&chunk2_quarantine).unwrap().len(),
        512,
        "chunk_0002.ts.quarantine size should remain 512 bytes"
    );

    // Sealed chunks and chat file remain on disk until processed by upload worker
    assert!(chunk0.exists(), "chunk_0000.ts should remain on disk");
    assert!(chunk1.exists(), "chunk_0001.ts should remain on disk");
    assert!(chat0.exists(), "chat_0000.jsonl should remain on disk");

    // Tasks received on upload_rx contain chunk_0000.ts, chunk_0001.ts, and chat_0000.jsonl
    let mut tasks = Vec::new();
    while let Ok(task) = upload_rx.try_recv() {
        tasks.push(task);
    }
    assert_eq!(tasks.len(), 3, "Expected 3 upload tasks enqueued");

    let mut chunk_names: Vec<String> = tasks.iter().map(|t| t.chunk_name.clone()).collect();
    chunk_names.sort();
    assert_eq!(
        chunk_names,
        vec![
            "chat_0000.jsonl".to_string(),
            "chunk_0000.ts".to_string(),
            "chunk_0001.ts".to_string(),
        ]
    );

    for task in &tasks {
        assert_eq!(task.channel_id, "test_channel");
        assert_eq!(task.streamer_name, "TestStreamer");
        assert_eq!(
            task.session_folder_id,
            "[2026-10-01_1000] [TestStreamer] TestStreamer - Title"
        );
        assert_eq!(
            task.remote_dir,
            "[2026-10-01_1000] [TestStreamer] TestStreamer - Title"
        );
    }
}

#[tokio::test]
async fn test_reconciliation_single_chunk_quarantined() {
    let guard = TempDirGuard::new("test_reconcile_single");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    let chunk0 = session_dir.join("chunk_0000.ts");
    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings::default();
    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 0);
    assert_eq!(report.chunks_quarantined, 1);

    assert!(
        !chunk0.exists(),
        "chunk_0000.ts should be renamed to quarantine"
    );
    let chunk0_quarantine = session_dir.join("chunk_0000.ts.quarantine");
    assert!(
        chunk0_quarantine.exists(),
        "chunk_0000.ts.quarantine should exist"
    );
    assert_eq!(
        fs::metadata(&chunk0_quarantine).unwrap().len(),
        1024,
        "Quarantined chunk size should be 1024 bytes"
    );

    assert!(
        upload_rx.try_recv().is_err(),
        "No upload tasks should be enqueued"
    );
}

#[tokio::test]
async fn test_reconciliation_empty_chunk_quarantined() {
    let guard = TempDirGuard::new("test_reconcile_empty");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    let chunk0 = session_dir.join("chunk_0000.ts");
    fs::write(&chunk0, b"").expect("write empty chunk 0");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings::default();
    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 0);
    assert_eq!(report.chunks_quarantined, 1);

    assert!(
        !chunk0.exists(),
        "Empty chunk_0000.ts should be renamed to quarantine"
    );
    let chunk0_quarantine = session_dir.join("chunk_0000.ts.quarantine");
    assert!(
        chunk0_quarantine.exists(),
        "chunk_0000.ts.quarantine should exist"
    );
    assert_eq!(
        fs::metadata(&chunk0_quarantine).unwrap().len(),
        0,
        "Quarantined empty chunk size should be 0 bytes"
    );

    assert!(
        upload_rx.try_recv().is_err(),
        "No upload tasks should be enqueued"
    );
}

#[tokio::test]
async fn test_reconciliation_quarantines_tail_chat_alongside_tail_video() {
    let guard = TempDirGuard::new("test_reconcile_tail_chat");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    let chunk0 = session_dir.join("chunk_0000.ts");
    let chat0 = session_dir.join("chat_0000.jsonl");
    let chunk1 = session_dir.join("chunk_0001.ts");
    let chat1 = session_dir.join("chat_0001.jsonl");
    let chunk2 = session_dir.join("chunk_0002.ts");
    let chat2 = session_dir.join("chat_0002.jsonl");

    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");
    fs::write(&chat0, vec![0u8; 256]).expect("write chat 0");
    fs::write(&chunk1, vec![0u8; 1024]).expect("write chunk 1");
    fs::write(&chat1, vec![0u8; 256]).expect("write chat 1");
    fs::write(&chunk2, vec![0u8; 512]).expect("write chunk 2");
    fs::write(&chat2, vec![0u8; 128]).expect("write chat 2");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("test_channel", "TestStreamer")],
        ..Default::default()
    };

    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    // Chunk 0 and 1 (and matching chat 0 and 1) should be enqueued (4 tasks)
    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 4);
    // Both tail chunk 2 and tail chat 2 should be quarantined (2 quarantined)
    assert_eq!(report.chunks_quarantined, 2);

    // Verify chunk 2 and chat 2 were renamed to .quarantine
    assert!(!chunk2.exists());
    assert!(session_dir.join("chunk_0002.ts.quarantine").exists());

    assert!(!chat2.exists(), "chat_0002.jsonl should be quarantined");
    assert!(
        session_dir.join("chat_0002.jsonl.quarantine").exists(),
        "chat_0002.jsonl.quarantine must exist"
    );

    // Verify tasks received on upload_rx
    let mut tasks = Vec::new();
    while let Ok(task) = upload_rx.try_recv() {
        tasks.push(task);
    }
    assert_eq!(tasks.len(), 4);
    let mut chunk_names: Vec<String> = tasks.iter().map(|t| t.chunk_name.clone()).collect();
    chunk_names.sort();
    assert_eq!(
        chunk_names,
        vec![
            "chat_0000.jsonl",
            "chat_0001.jsonl",
            "chunk_0000.ts",
            "chunk_0001.ts",
        ]
    );
}

#[tokio::test]
async fn test_reconciliation_non_contiguous_sealed_chunks_with_prior_uploads() {
    let guard = TempDirGuard::new("test_reconcile_gap");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    // chunk 0 and chat 0 remained on disk (e.g. earlier upload in DLQ)
    let chunk0 = session_dir.join("chunk_0000.ts");
    let chat0 = session_dir.join("chat_0000.jsonl");
    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");
    fs::write(&chat0, vec![0u8; 256]).expect("write chat 0");

    // chunk 1 and chat 1 were already uploaded and deleted from disk before crash!

    // chunk 2 and chat 2 were active when crash occurred (tail chunks)
    let chunk2 = session_dir.join("chunk_0002.ts");
    let chat2 = session_dir.join("chat_0002.jsonl");
    fs::write(&chunk2, vec![0u8; 512]).expect("write chunk 2");
    fs::write(&chat2, vec![0u8; 128]).expect("write chat 2");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("test_channel", "TestStreamer")],
        ..Default::default()
    };

    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    // Chunk 0 and chat 0 must be enqueued (2 tasks)
    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 2);
    // Tail chunk 2 and tail chat 2 must be quarantined
    assert_eq!(report.chunks_quarantined, 2);

    assert!(chunk0.exists());
    assert!(chat0.exists());
    assert!(!chunk2.exists());
    assert!(session_dir.join("chunk_0002.ts.quarantine").exists());
    assert!(!chat2.exists());
    assert!(session_dir.join("chat_0002.jsonl.quarantine").exists());

    let mut tasks = Vec::new();
    while let Ok(task) = upload_rx.try_recv() {
        tasks.push(task);
    }
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].chunk_name, "chunk_0000.ts");
    assert_eq!(tasks[1].chunk_name, "chat_0000.jsonl");
}

#[tokio::test]
async fn test_reconciliation_enqueues_tasks_in_ascending_chunk_order() {
    let guard = TempDirGuard::new("test_reconcile_order");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    // Write chunk 1 before chunk 0 to stress out-of-order dir scanning
    let chunk1 = session_dir.join("chunk_0001.ts");
    let chat1 = session_dir.join("chat_0001.jsonl");
    let chunk0 = session_dir.join("chunk_0000.ts");
    let chat0 = session_dir.join("chat_0000.jsonl");
    let chunk2 = session_dir.join("chunk_0002.ts");

    fs::write(&chunk1, vec![0u8; 1024]).expect("write chunk 1");
    fs::write(&chat1, vec![0u8; 256]).expect("write chat 1");
    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");
    fs::write(&chat0, vec![0u8; 256]).expect("write chat 0");
    fs::write(&chunk2, vec![0u8; 512]).expect("write chunk 2");

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut _event_rx) = mpsc::channel::<AppEvent>(100);

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("test_channel", "TestStreamer")],
        ..Default::default()
    };

    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    assert_eq!(report.chunks_enqueued, 4);
    assert_eq!(report.chunks_quarantined, 1);

    let mut tasks = Vec::new();
    while let Ok(task) = upload_rx.try_recv() {
        tasks.push(task);
    }
    assert_eq!(tasks.len(), 4);
    let ordered_names: Vec<String> = tasks.into_iter().map(|t| t.chunk_name).collect();
    assert_eq!(
        ordered_names,
        vec![
            "chunk_0000.ts",
            "chat_0000.jsonl",
            "chunk_0001.ts",
            "chat_0001.jsonl",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn test_reconciliation_dispatches_to_upload_worker_and_recovers_via_dlq() {
    let guard = TempDirGuard::new("test_reconcile_dlq");
    let session_dir = guard
        .path()
        .join("[2026-10-01_1000] [TestStreamer] TestStreamer - Title");
    fs::create_dir_all(&session_dir).expect("failed to create session directory");

    let chunk0 = session_dir.join("chunk_0000.ts");
    let chat0 = session_dir.join("chat_0000.jsonl");
    let chunk1 = session_dir.join("chunk_0001.ts");

    fs::write(&chunk0, vec![0u8; 1024]).expect("write chunk 0");
    fs::write(&chat0, vec![0u8; 256]).expect("write chat 0");
    fs::write(&chunk1, vec![0u8; 512]).expect("write chunk 1");

    // Initialize mock backend with simulated network outage
    let mock_backend = Arc::new(MockUploadBackend::new());
    mock_backend.should_fail.store(true, Ordering::SeqCst);

    let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(100);
    let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(100);

    let dlq_config = DlqConfig {
        initial_delay: Duration::from_millis(50),
        circuit_breaker_failures: 10,
        ..Default::default()
    };

    let worker_handle = UploadWorker::spawn_with_options(
        Some(mock_backend.clone()),
        event_tx.clone(),
        upload_rx,
        1,
        dlq_config,
    );

    let settings = Settings {
        channels: vec![ChannelConfig::with_alias("test_channel", "TestStreamer")],
        ..Default::default()
    };

    let report = reconcile_orphaned_sessions(guard.path(), &upload_tx, &event_tx, &settings).await;

    // Chunk 0 and chat 0 enqueued, tail chunk 1 quarantined
    assert_eq!(report.orphaned_sessions_scanned, 1);
    assert_eq!(report.chunks_enqueued, 2);
    assert_eq!(report.chunks_quarantined, 1);
    assert!(session_dir.join("chunk_0001.ts.quarantine").exists());

    // Advance virtual time to process initial upload attempts
    tokio::time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;

    // Files are preserved on disk during network failure
    assert!(chunk0.exists());
    assert!(chat0.exists());

    // Verify DLQ transfer logs were emitted
    let mut saw_dlq_log = false;
    while let Ok(ev) = event_rx.try_recv() {
        if let AppEvent::Log(entry) = ev
            && entry
                .message
                .contains("[DLQ] Transferred chunk_0000.ts to DLQ")
        {
            saw_dlq_log = true;
        }
    }
    assert!(saw_dlq_log, "DLQ transfer log should have been emitted");

    // Network is restored!
    mock_backend.should_fail.store(false, Ordering::SeqCst);

    // Close upload channel so worker shuts down after draining DLQ
    drop(upload_tx);

    // Advance time to allow DLQ backoff to expire and retry
    tokio::time::advance(Duration::from_millis(100)).await;
    worker_handle.await.expect("worker task completed");

    // After recovery, orphaned sealed files are uploaded and removed from disk
    assert!(
        !chunk0.exists(),
        "chunk_0000.ts should be uploaded and deleted"
    );
    assert!(
        !chat0.exists(),
        "chat_0000.jsonl should be uploaded and deleted"
    );

    let uploads = mock_backend.uploads.lock().await;
    assert_eq!(uploads.len(), 2, "Expected 2 uploaded files in backend");
}
