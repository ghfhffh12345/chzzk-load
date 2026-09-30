use futures_util::FutureExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::backend::{ProgressCallback, UploadBackend};
use crate::uploader::{UploadTask, broadcast_identifier};

/// Background upload worker managing per-channel FIFO serialization and fair cross-channel concurrency.
pub struct UploadWorker;

impl UploadWorker {
    /// Spawns an upload worker task with the default concurrency level (3).
    pub fn spawn(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_with_concurrency(backend_opt, event_tx, upload_rx, 3)
    }

    /// Spawns an upload worker task with a configurable channel concurrency limit.
    pub fn spawn_with_concurrency(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        mut upload_rx: Receiver<UploadTask>,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<()> {
        let concurrency = concurrency.max(1);
        tokio::spawn(async move {
            let mut active_channels: HashSet<String> = HashSet::new();
            let mut channel_queues: HashMap<String, VecDeque<UploadTask>> = HashMap::new();
            let mut ready_channels: VecDeque<String> = VecDeque::new();
            let mut join_set: tokio::task::JoinSet<String> = tokio::task::JoinSet::new();
            let mut rx_closed = false;

            let handle_finished_task =
                |res: Result<String, tokio::task::JoinError>,
                 active_channels: &mut HashSet<String>,
                 channel_queues: &mut HashMap<String, VecDeque<UploadTask>>,
                 ready_channels: &mut VecDeque<String>,
                 event_tx: &Sender<AppEvent>| {
                    match res {
                        Ok(finished_cid) => {
                            active_channels.remove(&finished_cid);
                            if let Some(queue) = channel_queues.get_mut(&finished_cid) {
                                if queue.is_empty() {
                                    channel_queues.remove(&finished_cid);
                                } else if !ready_channels.contains(&finished_cid) {
                                    ready_channels.push_back(finished_cid);
                                }
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.try_send(AppEvent::Log(LogEntry::error(format!(
                                "Upload worker task join error: {e}"
                            ))));
                        }
                    }
                };

            loop {
                // Reap completed tasks
                while let Some(res) = join_set.try_join_next() {
                    handle_finished_task(
                        res,
                        &mut active_channels,
                        &mut channel_queues,
                        &mut ready_channels,
                        &event_tx,
                    );
                }

                // Dispatch pending tasks for idle channels up to concurrency limit (O(1))
                while join_set.len() < concurrency
                    && let Some(ch_id) = ready_channels.pop_front()
                {
                    let task_opt = if let Some(queue) = channel_queues.get_mut(&ch_id) {
                        let task = queue.pop_front();
                        if queue.is_empty() {
                            channel_queues.remove(&ch_id);
                        }
                        task
                    } else {
                        None
                    };

                    if let Some(task) = task_opt {
                        active_channels.insert(ch_id.clone());
                        let backend = backend_opt.clone();
                        let event_tx = event_tx.clone();
                        let task_cid = ch_id.clone();
                        let task_streamer = task.streamer_name.clone();
                        let task_name = task.chunk_name.clone();

                        join_set.spawn(async move {
                            let tx_panic = event_tx.clone();
                            let cid_panic = task_cid.clone();
                            let name_panic = task_name.clone();
                            let streamer_panic = task_streamer.clone();

                            let unwind_res = std::panic::AssertUnwindSafe(async move {
                                if let Some(ref backend) = backend {
                                    let UploadTask {
                                        channel_id: cid,
                                        chunk_path,
                                        chunk_name: name,
                                        streamer_name: streamer,
                                        remote_dir,
                                        session_folder_id,
                                        ..
                                    } = task;

                                    let tx = event_tx.clone();
                                    let n = name.clone();
                                    let c = cid.clone();
                                    let s = streamer.clone();

                                    let progress_cb: ProgressCallback =
                                        Box::new(move |uploaded, total, speed| {
                                            let _ = tx.try_send(AppEvent::UploadProgress {
                                                channel_id: c.clone(),
                                                chunk_name: n.clone(),
                                                streamer_name: s.clone(),
                                                uploaded_bytes: uploaded,
                                                total_bytes: total,
                                                speed_mb_s: speed,
                                            });
                                        });

                                    let upload_res = backend
                                        .upload_file_and_delete(
                                            &chunk_path,
                                            &remote_dir,
                                            progress_cb,
                                        )
                                        .await;

                                    let target = broadcast_identifier(&streamer, &cid);
                                    match upload_res {
                                        Ok(reclaimed) => {
                                            let _ = event_tx
                                                .send(AppEvent::UploadCompleted {
                                                    channel_id: cid.clone(),
                                                    chunk_name: name.clone(),
                                                    reclaimed_bytes: reclaimed,
                                                })
                                                .await;
                                            let _ = event_tx
                                                .send(AppEvent::Log(LogEntry::clean(format!(
                                                    "[{target}] Uploaded & deleted {name} (reclaimed {:.1} MB)",
                                                    reclaimed as f64 / 1_048_576.0
                                                ))))
                                                .await;

                                            // If the chunk's parent directory is an empty or metadata-only session directory, clean it up
                                            if let Some(parent) = chunk_path.parent() {
                                                let is_session_dir = parent
                                                    .file_name()
                                                    .and_then(|n| n.to_str())
                                                    .map(|n| {
                                                        n == remote_dir
                                                            || n == session_folder_id
                                                            || n.starts_with(&format!("{cid}_"))
                                                    })
                                                    .unwrap_or(false);

                                                if is_session_dir
                                                    && let Ok(true) =
                                                        crate::engine::cleanup::cleanup_session_dir_if_empty(
                                                            parent,
                                                        )
                                                        .await
                                                {
                                                    let _ = event_tx
                                                        .send(AppEvent::Log(LogEntry::clean(format!(
                                                            "[{target}] Cleaned up empty session folder '{}'",
                                                            parent.display()
                                                        ))))
                                                        .await;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = event_tx
                                                .send(AppEvent::UploadFailed {
                                                    channel_id: cid.clone(),
                                                    chunk_name: name.clone(),
                                                })
                                                .await;
                                            let _ = event_tx
                                                .send(AppEvent::Log(LogEntry::error(format!(
                                                    "[{target}] Upload failed for {name}: {e}"
                                                ))))
                                                .await;
                                        }
                                    }
                                }
                            })
                            .catch_unwind()
                            .await;

                            if unwind_res.is_err() {
                                let target = broadcast_identifier(&streamer_panic, &cid_panic);
                                let _ = tx_panic
                                    .send(AppEvent::UploadFailed {
                                        channel_id: cid_panic.clone(),
                                        chunk_name: name_panic.clone(),
                                    })
                                    .await;
                                let _ = tx_panic
                                    .send(AppEvent::Log(LogEntry::error(format!(
                                        "[{target}] Upload task panicked unexpectedly during {name_panic}"
                                    ))))
                                    .await;
                            }

                            task_cid
                        });
                    }
                }

                // If upload receiver is closed and no uploads remain in-flight, exit
                if rx_closed && join_set.is_empty() {
                    break;
                }

                tokio::select! {
                    task_opt = upload_rx.recv(), if !rx_closed => {
                        match task_opt {
                            Some(task) => {
                                let cid = task.channel_id.clone();
                                let queue = channel_queues.entry(cid.clone()).or_default();
                                queue.push_back(task);
                                if !active_channels.contains(&cid) && !ready_channels.contains(&cid) {
                                    ready_channels.push_back(cid);
                                }
                            }
                            None => {
                                rx_closed = true;
                            }
                        }
                    }
                    res = join_set.join_next(), if !join_set.is_empty() => {
                        if let Some(join_res) = res {
                            handle_finished_task(
                                join_res,
                                &mut active_channels,
                                &mut channel_queues,
                                &mut ready_channels,
                                &event_tx,
                            );
                        }
                    }
                    else => {
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uploader::MockUploadBackend;
    use std::io::Write;
    use std::path::Path;

    #[tokio::test]
    async fn test_upload_worker_with_mock_backend() {
        let mock_backend = Arc::new(MockUploadBackend::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_worker_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let chunk1_path = temp_dir.join("chunk_0000.ts");
        let mut file = std::fs::File::create(&chunk1_path).unwrap();
        file.write_all(b"worker chunk payload 123456").unwrap();
        drop(file);

        let worker_handle = UploadWorker::spawn(Some(mock_backend.clone()), event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch1".to_string(),
                session_folder_id: "session1".to_string(),
                remote_dir: "session1".to_string(),
                chunk_path: chunk1_path.clone(),
                chunk_name: "chunk_0000.ts".to_string(),
                streamer_name: "Streamer1".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        // Verify the file was uploaded and deleted locally
        let uploads = mock_backend.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, chunk1_path);
        assert_eq!(uploads[0].1, "session1");
        assert!(!chunk1_path.exists());

        // Verify events were emitted
        let mut saw_progress = false;
        let mut saw_completed = false;
        while let Ok(ev) = event_rx.try_recv() {
            match ev {
                AppEvent::UploadProgress { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                    saw_progress = true;
                }
                AppEvent::UploadCompleted { chunk_name, .. } if chunk_name == "chunk_0000.ts" => {
                    saw_completed = true;
                }
                _ => {}
            }
        }
        assert!(saw_progress);
        assert!(saw_completed);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_upload_worker_without_backend_discards_gracefully() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let worker_handle = UploadWorker::spawn(None, event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch1".to_string(),
                session_folder_id: "session1".to_string(),
                remote_dir: "session1".to_string(),
                chunk_path: std::path::PathBuf::from("nonexistent.ts"),
                chunk_name: "nonexistent.ts".to_string(),
                streamer_name: "Streamer1".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        // No events should be produced when backend is None
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_upload_worker_cleans_empty_session_dir_with_metadata_jsonl() {
        let mock_backend = Arc::new(MockUploadBackend::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_worker_clean_{}", rand::random::<u32>()));
        let session_dir = temp_dir.join("session_clean_test");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        let chunk_path = session_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_path, b"chunk data").await.unwrap();

        let meta_path = session_dir.join("metadata.jsonl");
        tokio::fs::write(&meta_path, b"{\"event\":\"INITIAL_STATE\"}\n")
            .await
            .unwrap();

        let worker_handle = UploadWorker::spawn(Some(mock_backend.clone()), event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch_clean".to_string(),
                session_folder_id: "session_clean_test".to_string(),
                remote_dir: "session_clean_test".to_string(),
                chunk_path: chunk_path.clone(),
                chunk_name: "chunk_0000.ts".to_string(),
                streamer_name: "StreamerClean".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        assert!(!chunk_path.exists());
        assert!(!meta_path.exists(), "metadata.jsonl should be removed");
        assert!(!session_dir.exists(), "session_dir should be removed");

        let mut saw_clean_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::Log(entry) = ev
                && entry.message.contains("Cleaned up empty session folder")
            {
                saw_clean_log = true;
            }
        }
        assert!(saw_clean_log);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    struct PanickingBackend;
    impl UploadBackend for PanickingBackend {
        fn upload_file_and_delete<'a>(
            &'a self,
            _local_path: &'a Path,
            _remote_dir: &'a str,
            _on_progress: ProgressCallback,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<u64>> {
            Box::pin(async move {
                panic!("Simulated critical backend failure!");
            })
        }

        fn upload_text<'a>(
            &'a self,
            _remote_dir: &'a str,
            _file_name: &'a str,
            _content: &'a str,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn check_connection<'a>(
            &'a self,
        ) -> crate::uploader::backend::BoxFuture<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[tokio::test]
    async fn test_upload_worker_reports_panic_in_backend() {
        let panicking_backend = Arc::new(PanickingBackend);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(100);

        let worker_handle = UploadWorker::spawn(Some(panicking_backend), event_tx, upload_rx);

        upload_tx
            .send(UploadTask {
                channel_id: "ch_panic".to_string(),
                session_folder_id: "session_panic".to_string(),
                remote_dir: "session_panic".to_string(),
                chunk_path: std::path::PathBuf::from("chunk_panic.ts"),
                chunk_name: "chunk_panic.ts".to_string(),
                streamer_name: "StreamerPanic".to_string(),
            })
            .await
            .unwrap();

        drop(upload_tx);
        worker_handle.await.unwrap();

        let mut saw_failed = false;
        let mut saw_panic_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            match ev {
                AppEvent::UploadFailed { chunk_name, .. } if chunk_name == "chunk_panic.ts" => {
                    saw_failed = true;
                }
                AppEvent::Log(entry) if entry.message.contains("panicked unexpectedly") => {
                    saw_panic_log = true;
                }
                _ => {}
            }
        }
        assert!(saw_failed, "UploadFailed event should be emitted on panic");
        assert!(saw_panic_log, "Panic log entry should be emitted");
    }
}
