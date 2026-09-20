use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use chrono::Local;
use tokio::sync::mpsc::Sender;
use crate::app_path::resolve_path;
use crate::chzzk::client::ChzzkClient;
use crate::chzzk::models::LiveStreamInfo;
use crate::config::Settings;
use crate::drive::client::DriveClient;
use crate::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::AppEvent;
use crate::uploader::{UploadTask, UploadWorker};

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    drive: Option<DriveClient>,
    event_tx: Sender<AppEvent>,
    active_recordings: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl EngineOrchestrator {
    pub fn new(
        settings: Settings,
        chzzk: ChzzkClient,
        drive: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
    ) -> Self {
        Self {
            settings,
            chzzk,
            drive,
            event_tx,
            active_recordings: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    pub fn active_recordings(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.active_recordings.clone()
    }

    pub fn spawn_upload_consumer(
        drive_opt: Option<DriveClient>,
        event_tx: Sender<AppEvent>,
        mut upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(task) = upload_rx.recv().await {
                if let Some(ref drive) = drive_opt {
                    let tx = event_tx.clone();
                    let name = task.chunk_name.clone();
                    let n = name.clone();
                    let chunk_start_time = std::time::Instant::now();

                    let upload_res = UploadWorker::upload_and_delete(
                        drive,
                        task,
                        move |uploaded, total| {
                            let mb_s = (uploaded as f64 / 1_048_576.0)
                                / chunk_start_time.elapsed().as_secs_f64().max(0.1);
                            let _ = tx.try_send(AppEvent::UploadProgress {
                                chunk_name: n.clone(),
                                uploaded_bytes: uploaded,
                                total_bytes: total,
                                speed_mb_s: mb_s,
                            });
                        },
                    )
                    .await;

                    match upload_res {
                        Ok(reclaimed) => {
                            let _ = event_tx
                                .send(AppEvent::UploadCompleted {
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
                                .send(AppEvent::Log(format!(
                                    "[ERROR] Upload failed for {}: {}",
                                    name, e
                                )))
                                .await;
                        }
                    }
                }
            }
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
                match drive.get_or_create_folder(subfolder_name, Some(&root_id)).await {
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

    pub async fn process_sealed_chunk(
        chunk_path: &Path,
        session_folder_id: &mut Option<String>,
        drive_opt: Option<&DriveClient>,
        root_name: &str,
        drive_subfolder_name: &str,
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
                *session_folder_id = Self::ensure_session_folder(
                    drive,
                    root_name,
                    drive_subfolder_name,
                    event_tx,
                )
                .await;
            }

            if let Some(folder_id) = session_folder_id.as_ref() {
                let send_res = upload_tx
                    .send(UploadTask {
                        session_folder_id: folder_id.clone(),
                        chunk_path: chunk_path.to_path_buf(),
                        chunk_name: chunk_name.to_string(),
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
        let settings = self.settings.clone();
        let drive_opt = self.drive.clone();
        let chzzk = self.chzzk.clone();
        let event_tx = self.event_tx.clone();
        let active_recordings = self.active_recordings.clone();

        tokio::spawn(async move {
            let _ = event_tx
                .send(AppEvent::RecordingStarted {
                    channel_id: channel_id.clone(),
                    session_title: info.title.clone(),
                })
                .await;

            let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
            let folder_name = format!("{}_{}", channel_id, timestamp);
            let recordings_base =
                resolve_path(Path::new(&settings.general.recordings_dir));
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

            let mut session_folder_id: Option<String> = None;
            if let Some(ref drive) = drive_opt {
                session_folder_id = Self::ensure_session_folder(
                    drive,
                    &root_name,
                    &drive_subfolder_name,
                    &event_tx,
                )
                .await;
            }

            let output_pattern = session_dir.join("chunk_%04d.ts");
            let chunk_dur = settings.general.chunk_duration_seconds;
            let cookie = chzzk.cookie_header();
            let mut cmd = build_ffmpeg_command(&info.hls_url, &output_pattern, chunk_dur, cookie);

            let mut child = match cmd.spawn() {
                Ok(child) => {
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
                tokio::time::sleep(Duration::from_secs(1)).await;

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
                        &upload_tx,
                        &event_tx,
                    )
                    .await;
                }

                if is_finished {
                    break;
                }
            }

            {
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
            }
            let _ = event_tx
                .send(AppEvent::Log(format!(
                    "[REC] Recording session ended for channel {}",
                    channel_id
                )))
                .await;
        });
    }

    pub async fn poll_channels_once(&self, upload_tx: &Sender<UploadTask>) {
        for channel in &self.settings.channels {
            match self.chzzk.get_live_detail(&channel.id).await {
                Ok(Some(info)) => {
                    let _ = self
                        .event_tx
                        .send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: info.streamer_name.clone(),
                            is_live: true,
                            title: info.title.clone(),
                        })
                        .await;

                    let is_recording = {
                        let active = self.active_recordings.lock().await;
                        active.contains(&channel.id)
                    };

                    if !is_recording {
                        {
                            let mut active = self.active_recordings.lock().await;
                            active.insert(channel.id.clone());
                        }
                        self.spawn_recording_session(
                            channel.id.clone(),
                            info,
                            upload_tx.clone(),
                        );
                    }
                }
                Ok(None) => {
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
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(50);
        Self::spawn_upload_consumer(self.drive.clone(), self.event_tx.clone(), upload_rx);

        let poll_interval =
            Duration::from_secs(self.settings.general.poll_interval_seconds);
        loop {
            self.poll_channels_once(&upload_tx).await;
            tokio::time::sleep(poll_interval).await;
        }
    }
}
