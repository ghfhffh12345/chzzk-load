use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::Sender;

use crate::config::Settings;
use crate::recorder::watcher::parse_chunk_index;
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::UploadTask;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub orphaned_sessions_scanned: usize,
    pub chunks_enqueued: usize,
    pub chunks_quarantined: usize,
}

#[derive(Debug)]
struct IndexedChunk {
    index: u64,
    name: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ChunkKind {
    Video,
    Chat,
}

impl ChunkKind {
    fn extension(&self) -> &'static str {
        match self {
            Self::Video => "ts",
            Self::Chat => "jsonl",
        }
    }
}

async fn quarantine_chunk(
    path: &Path,
    name: &str,
    ext: &str,
    is_empty: bool,
    event_tx: &Sender<AppEvent>,
) -> bool {
    let quarantine_path = path.with_extension(format!("{ext}.quarantine"));
    if tokio::fs::rename(path, &quarantine_path).await.is_ok() {
        let reason = if is_empty {
            "empty"
        } else {
            "unfinalized tail"
        };
        let _ = event_tx
            .send(AppEvent::Log(LogEntry::warn(format!(
                "[RECONCILIATION] Quarantined {reason} chunk {name} -> {}",
                quarantine_path.display()
            ))))
            .await;
        true
    } else {
        false
    }
}

fn build_upload_task(
    channel_id: &str,
    folder_name: &str,
    streamer_name: &str,
    chunk: &IndexedChunk,
) -> UploadTask {
    UploadTask::chunk(
        channel_id,
        folder_name,
        folder_name,
        chunk.path.clone(),
        chunk.name.clone(),
        streamer_name,
    )
}

fn parse_chat_chunk_index(file_name: &str) -> Option<u64> {
    let lower = file_name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".jsonl")?;
    let index_str = stem.strip_prefix("chat_")?;
    index_str.parse::<u64>().ok()
}

/// Identifies the channel ID and streamer display name from an orphaned session folder name
/// by matching configured channel IDs and aliases.
fn match_channel_info(folder_name: &str, settings: &Settings) -> (String, String) {
    for ch in &settings.channels {
        if folder_name.contains(&ch.id) {
            let streamer = ch.alias.clone().unwrap_or_else(|| ch.id.clone());
            return (ch.id.clone(), streamer);
        }
        if let Some(ref alias) = ch.alias {
            if folder_name.contains(&format!("[{alias}]")) {
                return (ch.id.clone(), alias.clone());
            }
        }
    }
    ("orphaned".to_string(), "Orphaned".to_string())
}

/// Scans the `recordings_dir` for unfinalized/orphaned session folders from prior runs.
///
/// Under the Contiguity and Tail Quarantine invariants:
/// 1. For each orphaned session, chunk $i$ is verified sealed only if some chunk $j > i$ exists on disk with size > 0.
/// 2. Any chunk without a higher index counterpart (e.g. the active tail chunk being written when crash occurred)
///    is quarantined by renaming to `.ts.quarantine` or `.jsonl.quarantine`.
/// 3. Valid sealed chunks (and their matching completed chat `.jsonl` files) are enqueued to `upload_tx` to reclaim local disk space.
/// 4. Orphaned `metadata.jsonl` files are enqueued with `delete_on_success: true` following all media chunks.
pub async fn reconcile_orphaned_sessions(
    recordings_dir: &Path,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    settings: &Settings,
) -> ReconciliationReport {
    reconcile_orphaned_sessions_with_custodian(recordings_dir, upload_tx, event_tx, settings, None)
        .await
}

/// Scans the `recordings_dir` for orphaned sessions, registering draining sessions with an optional `SessionCustodian`.
pub async fn reconcile_orphaned_sessions_with_custodian(
    recordings_dir: &Path,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    settings: &Settings,
    custodian: Option<&crate::engine::custodian::SessionCustodian>,
) -> ReconciliationReport {
    let mut report = ReconciliationReport::default();

    let mut read_dir = match tokio::fs::read_dir(recordings_dir).await {
        Ok(rd) => rd,
        Err(_) => return report,
    };

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        let is_dir = match entry.file_type().await {
            Ok(ft) => ft.is_dir(),
            Err(_) => path.is_dir(),
        };

        if !is_dir {
            continue;
        }

        let folder_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };

        report.orphaned_sessions_scanned += 1;
        let (channel_id, streamer_name) = match_channel_info(&folder_name, settings);

        // Scan session folder for chunks and chat files
        let mut session_entries = match tokio::fs::read_dir(&path).await {
            Ok(rd) => rd,
            Err(_) => continue,
        };

        let mut chunks: Vec<IndexedChunk> = Vec::new();
        let mut chat_chunks: Vec<IndexedChunk> = Vec::new();
        let mut orphaned_metadata: Option<PathBuf> = None;

        while let Ok(Some(file_entry)) = session_entries.next_entry().await {
            let file_path = file_entry.path();
            let is_file = match file_entry.file_type().await {
                Ok(ft) => ft.is_file(),
                Err(_) => file_path.is_file(),
            };
            if !is_file {
                continue;
            }

            let file_name = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Skip already quarantined or temporary files
            if file_name.ends_with(".quarantine") || file_name.ends_with(".tmp") {
                continue;
            }

            let size = tokio::fs::metadata(&file_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);

            if size == 0 {
                // Empty files are quarantined
                let ext_opt = if file_name.ends_with(".ts") {
                    Some("ts")
                } else if file_name == "metadata.jsonl"
                    || (file_name.starts_with("chat_") && file_name.ends_with(".jsonl"))
                {
                    Some("jsonl")
                } else {
                    None
                };

                if let Some(ext) = ext_opt
                    && quarantine_chunk(&file_path, &file_name, ext, true, event_tx).await
                {
                    report.chunks_quarantined += 1;
                }
                continue;
            }

            if file_name == "metadata.jsonl" {
                orphaned_metadata = Some(file_path);
            } else if file_name.ends_with(".ts") {
                if let Some(index) = parse_chunk_index(&file_name) {
                    chunks.push(IndexedChunk {
                        index,
                        name: file_name,
                        path: file_path,
                    });
                }
            } else if file_name.starts_with("chat_") && file_name.ends_with(".jsonl") {
                if let Some(index) = parse_chat_chunk_index(&file_name) {
                    chat_chunks.push(IndexedChunk {
                        index,
                        name: file_name,
                        path: file_path,
                    });
                }
            }
        }

        let existing_ts_indices: HashSet<u64> = chunks.iter().map(|c| c.index).collect();
        let existing_chat_indices: HashSet<u64> = chat_chunks.iter().map(|c| c.index).collect();

        // Sort chunks by index ascending
        chunks.sort_by_key(|c| c.index);
        chat_chunks.sort_by_key(|c| c.index);

        struct EnqueueItem {
            index: u64,
            kind: ChunkKind,
            task: UploadTask,
        }
        let mut enqueued_items: Vec<EnqueueItem> = Vec::new();

        // Video chunks: chunk i is sealed iff some chunk j > i exists on disk with size > 0
        for chunk in &chunks {
            let is_sealed = existing_ts_indices.iter().any(|&j| j > chunk.index);
            if is_sealed {
                enqueued_items.push(EnqueueItem {
                    index: chunk.index,
                    kind: ChunkKind::Video,
                    task: build_upload_task(&channel_id, &folder_name, &streamer_name, chunk),
                });
            } else if quarantine_chunk(
                &chunk.path,
                &chunk.name,
                ChunkKind::Video.extension(),
                false,
                event_tx,
            )
            .await
            {
                report.chunks_quarantined += 1;
            }
        }

        // Chat chunks: sealed if its corresponding video chunk is sealed, or if a higher index exists on disk
        for chat in &chat_chunks {
            let is_sealed = if existing_ts_indices.contains(&chat.index) {
                // Matching video chunk exists: sealed iff that video chunk is sealed
                existing_ts_indices.iter().any(|&j| j > chat.index)
            } else {
                // No matching video chunk: sealed if any higher video or chat chunk exists
                existing_ts_indices.iter().any(|&j| j > chat.index)
                    || existing_chat_indices.iter().any(|&j| j > chat.index)
            };

            if is_sealed {
                enqueued_items.push(EnqueueItem {
                    index: chat.index,
                    kind: ChunkKind::Chat,
                    task: build_upload_task(&channel_id, &folder_name, &streamer_name, chat),
                });
            } else if quarantine_chunk(
                &chat.path,
                &chat.name,
                ChunkKind::Chat.extension(),
                false,
                event_tx,
            )
            .await
            {
                report.chunks_quarantined += 1;
            }
        }

        // Sort items strictly by (chunk_index, kind) so video chunk is dispatched before matching chat chunk
        enqueued_items.sort_by_key(|item| (item.index, item.kind));

        let mut session_enqueued = false;

        for item in enqueued_items {
            let kind = item.kind;
            let chunk_name = item.task.chunk_name.clone();
            if upload_tx.send(item.task).await.is_ok() {
                session_enqueued = true;
                report.chunks_enqueued += 1;
                let log_entry = match kind {
                    ChunkKind::Video => LogEntry::rec(format!(
                        "[RECONCILIATION] Enqueued sealed orphaned chunk {} from session '{}'",
                        chunk_name, folder_name
                    )),
                    ChunkKind::Chat => LogEntry::chat(format!(
                        "[RECONCILIATION] Enqueued orphaned chat file {} from session '{}'",
                        chunk_name, folder_name
                    )),
                };
                let _ = event_tx.send(AppEvent::Log(log_entry)).await;
            }
        }

        // Enqueue orphaned metadata.jsonl with unlinking enabled following all orphaned media chunks
        if let Some(metadata_path) = orphaned_metadata {
            let task = UploadTask::metadata(
                &channel_id,
                &folder_name,
                &folder_name,
                metadata_path,
                &streamer_name,
                true,
            );
            let chunk_name = task.chunk_name.clone();
            if upload_tx.send(task).await.is_ok() {
                session_enqueued = true;
                report.chunks_enqueued += 1;
                let log_entry = LogEntry::rec(format!(
                    "[RECONCILIATION] Enqueued orphaned metadata file {} from session '{}'",
                    chunk_name, folder_name
                ));
                let _ = event_tx.send(AppEvent::Log(log_entry)).await;
            }
        }

        if session_enqueued {
            if let Some(c) = custodian {
                c.register_draining(&path, &channel_id, &streamer_name);
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChannelConfig;
    use crate::engine::custodian::SessionCustodian;
    use std::fs;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_reconciliation_enqueues_metadata_and_registers_custodian() {
        let temp_dir = std::env::temp_dir().join(format!(
            "test_reconcile_colocated_{}",
            rand::random::<u32>()
        ));
        fs::create_dir_all(&temp_dir).unwrap();

        let session_dir = temp_dir.join("[2026-10-01_1000] [Streamer] Streamer - Title");
        fs::create_dir_all(&session_dir).unwrap();

        let chunk0 = session_dir.join("chunk_0000.ts");
        let chunk1 = session_dir.join("chunk_0001.ts");
        fs::write(&chunk0, vec![0u8; 1024]).unwrap();
        fs::write(&chunk1, vec![0u8; 512]).unwrap();

        let meta = session_dir.join("metadata.jsonl");
        fs::write(&meta, b"{\"event\":\"start\",\"stream_offset_ms\":0}\n").unwrap();

        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);
        let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(10);
        let settings = Settings {
            channels: vec![ChannelConfig::with_alias("chan_test", "Streamer")],
            ..Default::default()
        };

        let custodian = SessionCustodian::without_events();

        let report = reconcile_orphaned_sessions_with_custodian(
            &temp_dir,
            &upload_tx,
            &event_tx,
            &settings,
            Some(&custodian),
        )
        .await;

        assert_eq!(report.orphaned_sessions_scanned, 1);
        assert_eq!(report.chunks_enqueued, 2);
        assert_eq!(report.chunks_quarantined, 1);

        let task_media = upload_rx.try_recv().expect("media task must be enqueued");
        assert_eq!(task_media.chunk_name, "chunk_0000.ts");
        assert!(!task_media.is_metadata());
        assert!(task_media.delete_on_success);

        let task_meta = upload_rx
            .try_recv()
            .expect("metadata task must be enqueued after media");
        assert_eq!(task_meta.chunk_name, "metadata.jsonl");
        assert!(task_meta.is_metadata());
        assert!(task_meta.delete_on_success);

        // Verify custodian registered the session as draining
        let tracked = custodian.tracked_state(&session_dir);
        assert!(tracked.is_some());
        assert!(tracked.unwrap().is_draining());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
