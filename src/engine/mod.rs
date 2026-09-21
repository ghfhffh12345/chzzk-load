use crate::app_path::resolve_path;
use crate::chzzk::client::ChzzkClient;
use crate::chzzk::models::LiveStreamInfo;
use crate::config::Settings;
use crate::drive::client::DriveClient;
use crate::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::AppEvent;
use crate::uploader::{UploadTask, UploadWorker};
use chrono::Local;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct FinishedSession {
    pub live_id: Option<u64>,
    pub finished_at: std::time::Instant,
}

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    drive: Option<DriveClient>,
    event_tx: Sender<AppEvent>,
    active_recordings: Arc<tokio::sync::Mutex<HashSet<String>>>,
    finished_sessions: Arc<tokio::sync::Mutex<HashMap<String, FinishedSession>>>,
    cancel_token: CancellationToken,
    refresh_notify: Arc<tokio::sync::Notify>,
    session_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl EngineOrchestrator {
    pub fn new(
        settings: Settings,
        chzzk: ChzzkClient,
        drive: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
    ) -> Self {
        Self::with_cancel_token(settings, chzzk, drive, event_tx, CancellationToken::new())
    }

    pub fn with_cancel_token(
        settings: Settings,
        chzzk: ChzzkClient,
        drive: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            settings,
            chzzk,
            drive,
            event_tx,
            active_recordings: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            finished_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cancel_token,
            refresh_notify: Arc::new(tokio::sync::Notify::new()),
            session_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn trigger_refresh(&self) {
        self.refresh_notify.notify_one();
    }

    pub fn active_recordings(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.active_recordings.clone()
    }

    pub fn finished_sessions(&self) -> Arc<tokio::sync::Mutex<HashMap<String, FinishedSession>>> {
        self.finished_sessions.clone()
    }

    pub async fn register_finished_session(&self, channel_id: &str, live_id: Option<u64>) {
        let mut finished = self.finished_sessions.lock().await;
        finished.insert(
            channel_id.to_string(),
            FinishedSession {
                live_id,
                finished_at: std::time::Instant::now(),
            },
        );
    }

    pub fn spawn_upload_consumer(
        drive_opt: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
        upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_upload_consumer_with_concurrency(drive_opt, event_tx, upload_rx, 3)
    }

    pub fn spawn_upload_consumer_with_concurrency(
        drive_opt: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
        mut upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<()> {
        let concurrency = concurrency.max(1);
        tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut join_set = tokio::task::JoinSet::new();

            while let Some(task) = upload_rx.recv().await {
                // Periodically reap completed tasks to keep memory bounded
                while join_set.try_join_next().is_some() {}

                let permit = match Arc::clone(&semaphore).acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let drive = drive_opt.clone();
                let event_tx = event_tx.clone();

                join_set.spawn(async move {
                    let _permit = permit;
                    if let Some(ref drive) = drive {
                        let tx = event_tx.clone();
                        let cid = task.channel_id.clone();
                        let streamer = task.streamer_name.clone();
                        let name = task.chunk_name.clone();
                        let n = name.clone();
                        let c = cid.clone();
                        let s = streamer.clone();
                        let chunk_start_time = std::time::Instant::now();

                        let upload_res =
                            UploadWorker::upload_and_delete(drive, task, move |uploaded, total| {
                                let mb_s = (uploaded as f64 / 1_048_576.0)
                                    / chunk_start_time.elapsed().as_secs_f64().max(0.1);
                                let _ = tx.try_send(AppEvent::UploadProgress {
                                    channel_id: c.clone(),
                                    chunk_name: n.clone(),
                                    streamer_name: s.clone(),
                                    uploaded_bytes: uploaded,
                                    total_bytes: total,
                                    speed_mb_s: mb_s,
                                });
                            })
                            .await;

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
                                    .send(AppEvent::Log(format!(
                                        "[CLEAN] Uploaded & deleted {} (reclaimed {:.1} MB)",
                                        name,
                                        reclaimed as f64 / 1_048_576.0
                                    )))
                                    .await;
                            }
                            Err(e) => {
                                let _ = event_tx
                                    .send(AppEvent::UploadFailed {
                                        channel_id: cid.clone(),
                                        chunk_name: name.clone(),
                                    })
                                    .await;
                                let _ = event_tx
                                    .send(AppEvent::Log(format!(
                                        "[ERROR] Upload failed for {}: {}",
                                        name, e
                                    )))
                                    .await;
                            }
                        }
                    }
                });
            }

            // Drain remaining uploads on shutdown
            while join_set.join_next().await.is_some() {}
        })
    }

    pub async fn ensure_session_folder(
        drive: &DriveClient,
        root_name: &str,
        subfolder_name: &str,
        event_tx: &Sender<AppEvent>,
    ) -> Option<String> {
        match drive.get_or_create_folder(root_name, None).await {
            Ok(root_id) => {
                match drive
                    .get_or_create_folder(subfolder_name, Some(&root_id))
                    .await
                {
                    Ok(sub_id) => {
                        let _ = event_tx
                            .send(AppEvent::Log(format!(
                                "[DRIVE] Session folder ready: '{}/{}'",
                                root_name, subfolder_name
                            )))
                            .await;
                        Some(sub_id)
                    }
                    Err(e) => {
                        let _ = event_tx
                            .send(AppEvent::Log(format!(
                                "[WARN] Failed to create Drive session subfolder: {}",
                                e
                            )))
                            .await;
                        None
                    }
                }
            }
            Err(e) => {
                let _ = event_tx
                    .send(AppEvent::Log(format!(
                        "[WARN] Failed to access Drive root folder: {}",
                        e
                    )))
                    .await;
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn process_sealed_chunk(
        chunk_path: &Path,
        session_folder_id: &mut Option<String>,
        drive_opt: Option<&DriveClient>,
        root_name: &str,
        drive_subfolder_name: &str,
        channel_id: &str,
        streamer_name: &str,
        upload_tx: &Sender<UploadTask>,
        event_tx: &Sender<AppEvent>,
    ) {
        if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
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

            if session_folder_id.is_none()
                && let Some(drive) = drive_opt
            {
                *session_folder_id =
                    Self::ensure_session_folder(drive, root_name, drive_subfolder_name, event_tx)
                        .await;
            }

            if let Some(folder_id) = session_folder_id.as_ref() {
                let send_res = upload_tx
                    .send(UploadTask {
                        channel_id: channel_id.to_string(),
                        session_folder_id: folder_id.clone(),
                        chunk_path: chunk_path.to_path_buf(),
                        chunk_name: chunk_name.to_string(),
                        streamer_name: streamer_name.to_string(),
                    })
                    .await;

                if send_res.is_ok() {
                    let _ = event_tx
                        .send(AppEvent::Log(format!(
                            "[REC] {} sealed. Pushed to Drive upload queue.",
                            chunk_name
                        )))
                        .await;
                } else {
                    let _ = event_tx
                        .send(AppEvent::Log(format!(
                            "[REC] {} sealed (saved locally).",
                            chunk_name
                        )))
                        .await;
                }
            } else {
                let _ = event_tx
                    .send(AppEvent::Log(format!(
                        "[REC] {} sealed (saved locally).",
                        chunk_name
                    )))
                    .await;
            }
        }
    }

    pub fn spawn_recording_session(
        &self,
        channel_id: String,
        info: LiveStreamInfo,
        upload_tx: Sender<UploadTask>,
    ) {
        if self.cancel_token.is_cancelled() {
            return;
        }

        let settings = self.settings.clone();
        let drive_opt = self.drive.clone();
        let chzzk = self.chzzk.clone();
        let event_tx = self.event_tx.clone();
        let active_recordings = self.active_recordings.clone();
        let finished_sessions = self.finished_sessions.clone();
        let cancel_token = self.cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = event_tx
                .send(AppEvent::RecordingStarted {
                    channel_id: channel_id.clone(),
                    session_title: info.title.clone(),
                })
                .await;

            let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
            let folder_name = format!("{}_{}", channel_id, timestamp);
            let recordings_base = resolve_path(Path::new(&settings.general.recordings_dir));
            let session_dir = recordings_base.join(&folder_name);

            if let Err(e) = tokio::fs::create_dir_all(&session_dir).await {
                let _ = event_tx
                    .send(AppEvent::Log(format!(
                        "[ERROR] Failed to create session directory {}: {}",
                        session_dir.display(),
                        e
                    )))
                    .await;
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
                return;
            }

            let root_name = settings.google_drive.root_folder_name.clone();
            let safe_streamer = sanitize_filename(&info.streamer_name);
            let safe_title = sanitize_filename(&info.title);
            let drive_subfolder_name = format!(
                "[{}] {} - {}",
                Local::now().format("%Y-%m-%d_%H%M"),
                safe_streamer,
                safe_title
            );

            // Drive folder is created lazily in process_sealed_chunk when the first valid chunk is sealed
            let mut session_folder_id: Option<String> = None;

            let output_pattern = session_dir.join("chunk_%04d.ts");
            let chunk_dur = settings.general.chunk_duration_seconds;
            let cookie = chzzk.cookie_header();
            let mut cmd = build_ffmpeg_command(&info.hls_url, &output_pattern, chunk_dur, cookie);

            let mut child = match cmd.spawn() {
                Ok(mut child) => {
                    if let Some(stderr) = child.stderr.take() {
                        let event_tx_stderr = event_tx.clone();
                        tokio::spawn(async move {
                            use tokio::io::{AsyncBufReadExt, BufReader};
                            let mut lines = BufReader::new(stderr).lines();
                            while let Ok(Some(line)) = lines.next_line().await {
                                let trimmed = line.trim();
                                if !trimmed.is_empty() {
                                    let _ = event_tx_stderr
                                        .send(AppEvent::Log(format!("[FFMPEG] {}", trimmed)))
                                        .await;
                                }
                            }
                        });
                    }

                    let _ = event_tx
                        .send(AppEvent::Log(format!(
                            "[REC] Spawned FFmpeg segmenter ({}s TS chunks) -> {}",
                            chunk_dur,
                            session_dir.display()
                        )))
                        .await;
                    child
                }
                Err(e) => {
                    let _ = event_tx
                        .send(AppEvent::Log(format!(
                            "[ERROR] Failed to spawn FFmpeg: {}",
                            e
                        )))
                        .await;
                    let mut active = active_recordings.lock().await;
                    active.remove(&channel_id);
                    return;
                }
            };

            let mut watcher = SegmentWatcher::new(session_dir.clone());
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        let _ = event_tx
                            .send(AppEvent::Log(format!(
                                "[REC] Cancellation received for channel {}, stopping FFmpeg gracefully...",
                                channel_id
                            )))
                            .await;

                        if let Some(mut stdin) = child.stdin.take() {
                            use tokio::io::AsyncWriteExt;
                            let _ = stdin.write_all(b"q\n").await;
                            let _ = stdin.flush().await;
                            drop(stdin);
                        }

                        match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
                            Ok(Ok(status)) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(format!(
                                        "[REC] FFmpeg process exited cleanly: {}",
                                        status
                                    )))
                                    .await;
                            }
                            _ => {
                                let _ = event_tx
                                    .send(AppEvent::Log(
                                        "[REC] FFmpeg did not exit within timeout, terminating process...".to_string(),
                                    ))
                                    .await;
                                let _ = child.kill().await;
                                let _ = child.wait().await;
                            }
                        }

                        let sealed_chunks = watcher.detect_sealed(true);
                        for chunk_path in sealed_chunks {
                            Self::process_sealed_chunk(
                                &chunk_path,
                                &mut session_folder_id,
                                drive_opt.as_ref(),
                                &root_name,
                                &drive_subfolder_name,
                                &channel_id,
                                &info.streamer_name,
                                &upload_tx,
                                &event_tx,
                            )
                            .await;
                        }

                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let is_finished = match child.try_wait() {
                            Ok(Some(status)) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(format!(
                                        "[REC] FFmpeg process exited with status: {}",
                                        status
                                    )))
                                    .await;
                                true
                            }
                            Ok(None) => false,
                            Err(e) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(format!(
                                        "[WARN] Error waiting on FFmpeg child: {}",
                                        e
                                    )))
                                    .await;
                                true
                            }
                        };

                        let sealed_chunks = watcher.detect_sealed(is_finished);
                        for chunk_path in sealed_chunks {
                            Self::process_sealed_chunk(
                                &chunk_path,
                                &mut session_folder_id,
                                drive_opt.as_ref(),
                                &root_name,
                                &drive_subfolder_name,
                                &channel_id,
                                &info.streamer_name,
                                &upload_tx,
                                &event_tx,
                            )
                            .await;
                        }

                        if is_finished {
                            break;
                        }
                    }
                }
            }

            {
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
            }
            {
                let mut finished = finished_sessions.lock().await;
                finished.insert(
                    channel_id.clone(),
                    FinishedSession {
                        live_id: info.live_id,
                        finished_at: std::time::Instant::now(),
                    },
                );
            }
            let _ = event_tx
                .send(AppEvent::RecordingEnded {
                    channel_id: channel_id.clone(),
                })
                .await;
            let _ = event_tx
                .send(AppEvent::Log(format!(
                    "[REC] Recording session ended for channel {} (liveId: {:?})",
                    channel_id, info.live_id
                )))
                .await;

            // Clean up session directory if no chunks were saved
            if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await
                && rd.next_entry().await.ok().flatten().is_none()
            {
                let _ = tokio::fs::remove_dir(&session_dir).await;
            }
        });

        if let Ok(mut guard) = self.session_handles.lock() {
            guard.retain(|h| !h.is_finished());
            guard.push(handle);
        }
    }

    pub async fn poll_channels_once(&self, upload_tx: &Sender<UploadTask>) {
        for channel in &self.settings.channels {
            if self.cancel_token.is_cancelled() {
                break;
            }
            match self.chzzk.get_live_detail(&channel.id).await {
                Ok(Some(info)) => {
                    let is_recording = {
                        let active = self.active_recordings.lock().await;
                        active.contains(&channel.id)
                    };

                    let is_duplicate_or_cooldown = {
                        let mut finished = self.finished_sessions.lock().await;
                        if let Some(prev) = finished.get(&channel.id) {
                            match (&info.live_id, &prev.live_id) {
                                (Some(curr_id), Some(prev_id)) if curr_id == prev_id => {
                                    // Same liveId as just-finished session -> definitely stale CDN cache
                                    true
                                }
                                (Some(curr_id), Some(prev_id)) if curr_id != prev_id => {
                                    // liveId changed -> Genuinely new stream started!
                                    finished.remove(&channel.id);
                                    false
                                }
                                _ => {
                                    // One or both live_ids are None -> Fall back to cooldown period
                                    let cooldown = Duration::from_secs(
                                        self.settings.general.stream_cooldown_seconds,
                                    );
                                    if prev.finished_at.elapsed() < cooldown {
                                        true
                                    } else {
                                        finished.remove(&channel.id);
                                        false
                                    }
                                }
                            }
                        } else {
                            false
                        }
                    };

                    if is_recording {
                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: info.streamer_name.clone(),
                                is_live: true,
                                title: info.title.clone(),
                            })
                            .await;
                    } else if is_duplicate_or_cooldown {
                        let _ = self
                            .event_tx
                            .send(AppEvent::Log(format!(
                                "[POLL] Channel {} ({}) stream recently concluded (liveId: {:?}). Waiting for API cache to close...",
                                channel.id, channel.name, info.live_id
                            )))
                            .await;

                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: channel.name.clone(),
                                is_live: false,
                                title: "Stream Concluded (Cooldown)".to_string(),
                            })
                            .await;
                    } else {
                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: info.streamer_name.clone(),
                                is_live: true,
                                title: info.title.clone(),
                            })
                            .await;

                        {
                            let mut active = self.active_recordings.lock().await;
                            active.insert(channel.id.clone());
                        }
                        self.spawn_recording_session(channel.id.clone(), info, upload_tx.clone());
                    }
                }
                Ok(None) => {
                    // Channel reported CLOSE (offline)
                    {
                        let mut finished = self.finished_sessions.lock().await;
                        finished.remove(&channel.id);
                    }

                    let _ = self
                        .event_tx
                        .send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: channel.name.clone(),
                            is_live: false,
                            title: "Offline".to_string(),
                        })
                        .await;
                }
                Err(e) => {
                    let _ = self
                        .event_tx
                        .send(AppEvent::Log(format!(
                            "[WARN] Polling failed for {}: {}",
                            channel.id, e
                        )))
                        .await;
                }
            }
        }
    }

    pub async fn run(self: Arc<Self>) {
        let concurrency = self.settings.google_drive.upload_concurrency;
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(50);
        let upload_handle = Self::spawn_upload_consumer_with_concurrency(
            self.drive.clone(),
            self.event_tx.clone(),
            upload_rx,
            concurrency,
        );

        let poll_interval = Duration::from_secs(self.settings.general.poll_interval_seconds);
        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

            self.poll_channels_once(&upload_tx).await;

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    break;
                }
                _ = self.refresh_notify.notified() => {
                    // Manual refresh triggered: poll immediately on next iteration
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // Normal interval elapsed
                }
            }
        }

        // Graceful shutdown sequence:
        // 1. Await all recording sessions (they will finish stopping FFmpeg and flushing sealed chunks)
        let handles: Vec<tokio::task::JoinHandle<()>> = {
            if let Ok(mut guard) = self.session_handles.lock() {
                std::mem::take(&mut *guard)
            } else {
                Vec::new()
            }
        };
        for handle in handles {
            let _ = handle.await;
        }

        // 2. Drop the orchestrator's upload_tx sender so upload_rx closes when empty
        drop(upload_tx);

        // 3. Await upload consumer to finish all in-flight and queued uploads
        let _ = upload_handle.await;

        let _ = self
            .event_tx
            .send(AppEvent::Log(
                "[INFO] Engine graceful shutdown complete.".to_string(),
            ))
            .await;
    }
}
