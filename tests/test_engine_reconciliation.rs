use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

use chzzk_load::config::{ChannelConfig, Settings};
use chzzk_load::engine::reconciliation::reconcile_orphaned_sessions;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::uploader::UploadTask;

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
