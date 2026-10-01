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
/// 1. For each orphaned session, chunk $i$ is verified sealed only if chunk $i+1$ exists on disk with size > 0.
/// 2. Any chunk without an $i+1$ counterpart (e.g. the active tail chunk being written when crash occurred)
///    is quarantined by renaming to `.ts.quarantine`.
/// 3. Valid sealed chunks (and any completed chat `.jsonl` files) are enqueued to `upload_tx` to reclaim local disk space.
pub async fn reconcile_orphaned_sessions(
    recordings_dir: &Path,
    upload_tx: &Sender<UploadTask>,
    event_tx: &Sender<AppEvent>,
    settings: &Settings,
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
        let mut chat_files: Vec<(String, PathBuf)> = Vec::new();

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
                if file_name.ends_with(".ts") {
                    let quarantine_path = file_path.with_extension("ts.quarantine");
                    if tokio::fs::rename(&file_path, &quarantine_path)
                        .await
                        .is_ok()
                    {
                        report.chunks_quarantined += 1;
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::warn(format!(
                                "[RECONCILIATION] Quarantined empty chunk {} -> {}",
                                file_name,
                                quarantine_path.display()
                            ))))
                            .await;
                    }
                }
                continue;
            }

            if file_name.ends_with(".ts") {
                if let Some(index) = parse_chunk_index(&file_name) {
                    chunks.push(IndexedChunk {
                        index,
                        name: file_name,
                        path: file_path,
                    });
                }
            } else if file_name.starts_with("chat_") && file_name.ends_with(".jsonl") {
                chat_files.push((file_name, file_path));
            }
        }

        let existing_indices: HashSet<u64> = chunks.iter().map(|c| c.index).collect();

        // Contiguity check: chunk i is verified sealed iff chunk i+1 exists on disk with size > 0
        for chunk in &chunks {
            let has_next = existing_indices.contains(&(chunk.index + 1));
            if has_next {
                // Chunk is confirmed complete & sealed
                let task = UploadTask {
                    channel_id: channel_id.clone(),
                    session_folder_id: folder_name.clone(),
                    remote_dir: folder_name.clone(),
                    chunk_path: chunk.path.clone(),
                    chunk_name: chunk.name.clone(),
                    streamer_name: streamer_name.clone(),
                };
                if upload_tx.send(task).await.is_ok() {
                    report.chunks_enqueued += 1;
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "[RECONCILIATION] Enqueued sealed orphaned chunk {} from session '{}'",
                            chunk.name, folder_name
                        ))))
                        .await;
                }
            } else {
                // Tail chunk without an N+1 counterpart: quarantine to prevent corrupted upload
                let quarantine_path = chunk.path.with_extension("ts.quarantine");
                if tokio::fs::rename(&chunk.path, &quarantine_path)
                    .await
                    .is_ok()
                {
                    report.chunks_quarantined += 1;
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::warn(format!(
                            "[RECONCILIATION] Quarantined unfinalized tail chunk {} -> {}",
                            chunk.name,
                            quarantine_path.display()
                        ))))
                        .await;
                }
            }
        }

        // Enqueue any completed chat jsonl chunks
        for (chat_name, chat_path) in chat_files {
            let task = UploadTask {
                channel_id: channel_id.clone(),
                session_folder_id: folder_name.clone(),
                remote_dir: folder_name.clone(),
                chunk_path: chat_path,
                chunk_name: chat_name.clone(),
                streamer_name: streamer_name.clone(),
            };
            if upload_tx.send(task).await.is_ok() {
                report.chunks_enqueued += 1;
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::chat(format!(
                        "[RECONCILIATION] Enqueued orphaned chat file {} from session '{}'",
                        chat_name, folder_name
                    ))))
                    .await;
            }
        }
    }

    report
}
