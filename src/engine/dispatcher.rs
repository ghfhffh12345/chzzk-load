use std::path::Path;
use tokio::sync::mpsc::Sender;

use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::{UploadTask, broadcast_identifier};

/// Processes a single sealed chunk: emits ChunkSealed event, and forwards it to the upload queue
/// or logs that it has been saved locally.
pub async fn process_sealed_chunk(
    chunk_path: &Path,
    remote_dir: &str,
    channel_id: &str,
    streamer_name: &str,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    backend_active: bool,
) {
    if let Some(chunk_name) = chunk_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
    {
        let size = tokio::fs::metadata(chunk_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let _ = event_tx
            .send(AppEvent::ChunkSealed {
                chunk_name: chunk_name.to_string(),
                size_bytes: size,
            })
            .await;

        let target = broadcast_identifier(streamer_name, channel_id);

        if backend_active {
            let send_res = upload_tx
                .send(UploadTask {
                    channel_id: channel_id.to_string(),
                    session_folder_id: remote_dir.to_string(),
                    remote_dir: remote_dir.to_string(),
                    chunk_path: chunk_path.to_path_buf(),
                    chunk_name: chunk_name.to_string(),
                    streamer_name: streamer_name.to_string(),
                })
                .await;

            if send_res.is_ok() {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "[{target}] {chunk_name} sealed. Pushed to cloud upload queue."
                    ))))
                    .await;
            } else {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "[{target}] {chunk_name} sealed (saved locally)."
                    ))))
                    .await;
            }
        } else {
            let _ = event_tx
                .send(AppEvent::Log(LogEntry::rec(format!(
                    "[{target}] {chunk_name} sealed (saved locally)."
                ))))
                .await;
        }
    }
}

/// Detects newly sealed chunks using SegmentWatcher and dispatches each to the upload queue.
#[allow(clippy::too_many_arguments)]
pub async fn seal_and_enqueue_chunks(
    watcher: &mut SegmentWatcher,
    remote_dir: &str,
    channel_id: &str,
    streamer_name: &str,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    backend_active: bool,
    is_finished: bool,
) {
    let sealed_chunks = watcher.detect_sealed(is_finished);
    for chunk_path in sealed_chunks {
        process_sealed_chunk(
            &chunk_path,
            remote_dir,
            channel_id,
            streamer_name,
            upload_tx,
            event_tx,
            backend_active,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn test_process_sealed_chunk_with_backend() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(10);
        let (upload_tx, mut upload_rx) = tokio::sync::mpsc::channel(10);

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_disp_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let chunk_path = temp_dir.join("chunk_0000.ts");
        let mut file = std::fs::File::create(&chunk_path).unwrap();
        file.write_all(b"test chunk").unwrap();
        drop(file);

        process_sealed_chunk(
            &chunk_path,
            "session_folder",
            "ch1",
            "Streamer1",
            &upload_tx,
            &event_tx,
            true,
        )
        .await;

        let task = upload_rx.recv().await.unwrap();
        assert_eq!(task.channel_id, "ch1");
        assert_eq!(task.remote_dir, "session_folder");
        assert_eq!(task.chunk_name, "chunk_0000.ts");

        let mut saw_chunk_sealed = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::ChunkSealed { chunk_name, .. } = ev
                && chunk_name == "chunk_0000.ts"
            {
                saw_chunk_sealed = true;
            }
        }

        assert!(saw_chunk_sealed);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
