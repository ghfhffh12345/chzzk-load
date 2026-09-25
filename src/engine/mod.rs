use crate::app_path::resolve_path;
use crate::chzzk::chat::ChzzkChatClient;
use crate::chzzk::client::ChzzkClient;
use crate::chzzk::models::LiveStreamInfo;
use crate::config::Settings;
use crate::drive::client::DriveClient;
use crate::recorder::ffmpeg::{build_ffmpeg_command, sanitize_filename};
use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::{AppEvent, LogEntry};
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

#[derive(Debug, Clone, Default)]
pub struct ActiveSessionState {
    pub start_timestamp: String,
    pub streamer_name: String,
    pub current_title: String,
    pub session_folder_id: Option<String>,
    pub title_history: Vec<(String, String)>,
    pub title_history_file_id: Option<String>,
}

impl ActiveSessionState {
    pub fn new(start_timestamp: String, streamer_name: String, current_title: String) -> Self {
        let initial_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        Self {
            start_timestamp,
            streamer_name,
            title_history: vec![(initial_time, current_title.clone())],
            current_title,
            session_folder_id: None,
            title_history_file_id: None,
        }
    }

    pub fn folder_name(&self) -> String {
        format!(
            "[{}] {} - {}",
            self.start_timestamp,
            sanitize_filename(&self.streamer_name),
            sanitize_filename(&self.current_title)
        )
    }

    pub fn record_title_change(&mut self, new_title: String, timestamp: String) {
        self.current_title = new_title.clone();
        self.title_history.push((timestamp, new_title));
    }

    pub fn format_title_history(&self) -> String {
        let mut out = String::new();
        for (timestamp, title) in &self.title_history {
            out.push_str(&format!("[{}] {}\n", timestamp, title));
        }
        out
    }
}

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    drive: Option<DriveClient>,
    event_tx: Sender<AppEvent>,
    active_recordings: Arc<tokio::sync::Mutex<HashSet<String>>>,
    active_sessions: Arc<tokio::sync::Mutex<HashMap<String, ActiveSessionState>>>,
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
            active_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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

    pub fn active_sessions(&self) -> Arc<tokio::sync::Mutex<HashMap<String, ActiveSessionState>>> {
        self.active_sessions.clone()
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
                                    .send(AppEvent::Log(LogEntry::clean(format!(
                                        "Uploaded & deleted {} (reclaimed {:.1} MB)",
                                        name,
                                        reclaimed as f64 / 1_048_576.0
                                    ))))
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
                                    .send(AppEvent::Log(LogEntry::error(format!(
                                        "Upload failed for {}: {}",
                                        name, e
                                    ))))
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

    pub async fn cleanup_empty_session_dirs(recordings_dir: &Path) -> std::io::Result<usize> {
        if !recordings_dir.exists() || !recordings_dir.is_dir() {
            return Ok(0);
        }

        let mut removed_count = 0;
        let mut entries = tokio::fs::read_dir(recordings_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let is_dir = match entry.file_type().await {
                Ok(ft) => ft.is_dir(),
                Err(_) => entry.path().is_dir(),
            };
            if is_dir {
                let path = entry.path();
                if let Ok(mut sub_entries) = tokio::fs::read_dir(&path).await
                    && let Ok(None) = sub_entries.next_entry().await
                    && tokio::fs::remove_dir(&path).await.is_ok()
                {
                    removed_count += 1;
                }
            }
        }
        Ok(removed_count)
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
                            .send(AppEvent::Log(LogEntry::drive(format!(
                                "Session folder ready: '{}/{}'",
                                root_name, subfolder_name
                            ))))
                            .await;
                        Some(sub_id)
                    }
                    Err(e) => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::warn(format!(
                                "Failed to create Drive session subfolder: {}",
                                e
                            ))))
                            .await;
                        None
                    }
                }
            }
            Err(e) => {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::warn(format!(
                        "Failed to access Drive root folder: {}",
                        e
                    ))))
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
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "{} sealed. Pushed to Drive upload queue.",
                            chunk_name
                        ))))
                        .await;
                } else {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "{} sealed (saved locally).",
                            chunk_name
                        ))))
                        .await;
                }
            } else {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "{} sealed (saved locally).",
                        chunk_name
                    ))))
                    .await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn seal_and_enqueue_chunks(
        watcher: &mut SegmentWatcher,
        session_folder_id: &mut Option<String>,
        drive_opt: Option<&DriveClient>,
        root_name: &str,
        subfolder_name: &str,
        channel_id: &str,
        streamer_name: &str,
        upload_tx: &Sender<UploadTask>,
        event_tx: &Sender<AppEvent>,
        is_finished: bool,
    ) {
        let sealed_chunks = watcher.detect_sealed(is_finished);
        for chunk_path in sealed_chunks {
            Self::process_sealed_chunk(
                &chunk_path,
                session_folder_id,
                drive_opt,
                root_name,
                subfolder_name,
                channel_id,
                streamer_name,
                upload_tx,
                event_tx,
            )
            .await;
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
        let active_sessions = self.active_sessions.clone();
        let finished_sessions = self.finished_sessions.clone();
        let cancel_token = self.cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = event_tx
                .send(AppEvent::RecordingStarted {
                    channel_id: channel_id.clone(),
                    session_title: info.title.clone(),
                })
                .await;

            let start_timestamp = Local::now().format("%Y-%m-%d_%H%M").to_string();
            let initial_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            {
                let mut sessions = active_sessions.lock().await;
                sessions
                    .entry(channel_id.clone())
                    .or_insert_with(|| ActiveSessionState {
                        start_timestamp: start_timestamp.clone(),
                        streamer_name: info.streamer_name.clone(),
                        current_title: info.title.clone(),
                        session_folder_id: None,
                        title_history: vec![(initial_time, info.title.clone())],
                        title_history_file_id: None,
                    });
            }

            let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
            let folder_name = format!("{}_{}", channel_id, timestamp);
            let recordings_base = resolve_path(Path::new(&settings.general.recordings_dir));
            let session_dir = recordings_base.join(&folder_name);

            if let Err(e) = tokio::fs::create_dir_all(&session_dir).await {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::error(format!(
                        "Failed to create session directory {}: {}",
                        session_dir.display(),
                        e
                    ))))
                    .await;
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
                let mut sessions = active_sessions.lock().await;
                sessions.remove(&channel_id);
                let _ = event_tx
                    .send(AppEvent::RecordingEnded {
                        channel_id: channel_id.clone(),
                    })
                    .await;
                return;
            }

            let root_name = settings.google_drive.root_folder_name.clone();

            // Drive folder is created lazily in process_sealed_chunk when the first valid chunk is sealed
            let mut session_folder_id: Option<String> = None;
            let session_cancel = cancel_token.child_token();

            let chat_session_cancel = session_cancel.clone();
            let chat_task = if settings.general.record_chat {
                if let Some(chat_cid) = info.chat_channel_id.clone() {
                    let chzzk_chat = chzzk.clone();
                    let event_tx_chat = event_tx.clone();
                    let chat_cid_id = channel_id.clone();
                    let chat_target_path = session_dir.join("chat.jsonl");
                    let flush_sec = settings.general.chat_flush_interval_seconds;

                    Some(tokio::spawn(async move {
                        let access_token = tokio::select! {
                            _ = chat_session_cancel.cancelled() => return,
                            res = chzzk_chat.get_chat_access_token(&chat_cid) => match res {
                                Ok(t) => {
                                    let _ = event_tx_chat
                                        .send(AppEvent::Log(LogEntry::chat(format!(
                                            "Retrieved chat access token for channel {}",
                                            chat_cid_id
                                        ))))
                                        .await;
                                    t
                                }
                                Err(e) => {
                                    let _ = event_tx_chat
                                        .send(AppEvent::Log(LogEntry::warn(format!(
                                            "Failed to retrieve chat access token for channel {}: {}",
                                            chat_cid_id, e
                                        ))))
                                        .await;
                                    return;
                                }
                            }
                        };

                        if chat_session_cancel.is_cancelled() {
                            return;
                        }

                        let (stats_tx, mut stats_rx) = tokio::sync::mpsc::channel::<u64>(50);
                        let forward_cid = chat_cid_id.clone();
                        let forward_tx = event_tx_chat.clone();
                        let forward_handle = tokio::spawn(async move {
                            while let Some(count) = stats_rx.recv().await {
                                let _ = forward_tx.try_send(AppEvent::ChatStats {
                                    channel_id: forward_cid.clone(),
                                    message_count: count,
                                });
                            }
                        });

                        let mut client = ChzzkChatClient::new(
                            chat_cid,
                            access_token,
                            chat_target_path,
                            Duration::from_secs(flush_sec),
                            chat_session_cancel,
                        );
                        if let Some(ws_url) = chzzk_chat.chat_ws_url() {
                            client = client.with_custom_ws_url(ws_url);
                        }

                        let _ = event_tx_chat
                            .send(AppEvent::Log(LogEntry::chat(format!(
                                "Started real-time chat recording for channel {}",
                                chat_cid_id
                            ))))
                            .await;

                        match client.run(Some(stats_tx)).await {
                            Ok(total_msgs) => {
                                let _ = event_tx_chat
                                    .send(AppEvent::Log(LogEntry::chat(format!(
                                        "Chat recording finished for channel {} ({} messages)",
                                        chat_cid_id, total_msgs
                                    ))))
                                    .await;
                            }
                            Err(e) => {
                                let _ = event_tx_chat
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Chat recording error for channel {}: {}",
                                        chat_cid_id, e
                                    ))))
                                    .await;
                            }
                        }

                        let _ = forward_handle.await;
                    }))
                } else {
                    None
                }
            } else {
                None
            };

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
                                        .try_send(AppEvent::Log(LogEntry::ffmpeg(trimmed)));
                                }
                            }
                        });
                    }

                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "Spawned FFmpeg segmenter ({}s TS chunks) -> {}",
                            chunk_dur,
                            session_dir.display()
                        ))))
                        .await;
                    child
                }
                Err(e) => {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::error(format!(
                            "Failed to spawn FFmpeg: {}",
                            e
                        ))))
                        .await;
                    session_cancel.cancel();
                    if let Some(mut chat_handle) = chat_task
                        && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                            .await
                            .is_err()
                    {
                        chat_handle.abort();
                    }
                    let chat_file = session_dir.join("chat.jsonl");
                    if tokio::fs::try_exists(&chat_file).await.unwrap_or(false) {
                        let _ = tokio::fs::remove_file(&chat_file).await;
                    }
                    let _ = tokio::fs::remove_dir(&session_dir).await;
                    let mut active = active_recordings.lock().await;
                    active.remove(&channel_id);
                    let mut sessions = active_sessions.lock().await;
                    sessions.remove(&channel_id);
                    let _ = event_tx
                        .send(AppEvent::RecordingEnded {
                            channel_id: channel_id.clone(),
                        })
                        .await;
                    return;
                }
            };

            let mut watcher = SegmentWatcher::new(session_dir.clone());
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "Cancellation received for channel {}, stopping FFmpeg gracefully...",
                                channel_id
                            ))))
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
                                    .send(AppEvent::Log(LogEntry::rec(format!(
                                        "FFmpeg process exited cleanly: {}",
                                        status
                                    ))))
                                    .await;
                            }
                            _ => {
                                let _ = child.kill().await;
                                let _ = child.wait().await;
                                let _ = event_tx
                                    .send(AppEvent::Log(LogEntry::rec(
                                        "FFmpeg did not exit within timeout, terminating process...",
                                    )))
                                    .await;
                            }
                        }

                        let current_subfolder_name = {
                            let sessions = active_sessions.lock().await;
                            if let Some(s) = sessions.get(&channel_id) {
                                s.folder_name()
                            } else {
                                ActiveSessionState {
                                    start_timestamp: start_timestamp.clone(),
                                    streamer_name: info.streamer_name.clone(),
                                    current_title: info.title.clone(),
                                    session_folder_id: None,
                                    title_history: vec![],
                                    title_history_file_id: None,
                                }
                                .folder_name()
                            }
                        };

                        Self::seal_and_enqueue_chunks(
                            &mut watcher,
                            &mut session_folder_id,
                            drive_opt.as_ref(),
                            &root_name,
                            &current_subfolder_name,
                            &channel_id,
                            &info.streamer_name,
                            &upload_tx,
                            &event_tx,
                            true,
                        )
                        .await;

                        if session_folder_id.is_some() {
                            let mut sessions = active_sessions.lock().await;
                            if let Some(s) = sessions.get_mut(&channel_id) {
                                s.session_folder_id = session_folder_id.clone();
                                if s.title_history_file_id.is_none()
                                    && let (Some(fid), Some(drive)) =
                                        (session_folder_id.as_deref(), drive_opt.as_ref())
                                {
                                    let history_text = s.format_title_history();
                                    if let Ok(file_id) = drive
                                        .upload_text_file(
                                            fid,
                                            "title_history.txt",
                                            &history_text,
                                            None,
                                        )
                                        .await
                                    {
                                        s.title_history_file_id = Some(file_id);
                                        let _ = event_tx
                                            .send(AppEvent::Log(LogEntry::drive(format!(
                                                "Initialized 'title_history.txt' in Drive folder for {}",
                                                channel_id
                                            ))))
                                            .await;
                                    }
                                }
                            }
                        }

                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let is_finished = match child.try_wait() {
                            Ok(Some(status)) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(LogEntry::rec(format!(
                                        "FFmpeg process exited with status: {}",
                                        status
                                    ))))
                                    .await;
                                true
                            }
                            Ok(None) => false,
                            Err(e) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Error waiting on FFmpeg child: {}",
                                        e
                                    ))))
                                    .await;
                                true
                            }
                        };

                        let current_subfolder_name = {
                            let sessions = active_sessions.lock().await;
                            if let Some(s) = sessions.get(&channel_id) {
                                s.folder_name()
                            } else {
                                ActiveSessionState {
                                    start_timestamp: start_timestamp.clone(),
                                    streamer_name: info.streamer_name.clone(),
                                    current_title: info.title.clone(),
                                    session_folder_id: None,
                                    title_history: vec![],
                                    title_history_file_id: None,
                                }
                                .folder_name()
                            }
                        };

                        Self::seal_and_enqueue_chunks(
                            &mut watcher,
                            &mut session_folder_id,
                            drive_opt.as_ref(),
                            &root_name,
                            &current_subfolder_name,
                            &channel_id,
                            &info.streamer_name,
                            &upload_tx,
                            &event_tx,
                            is_finished,
                        )
                        .await;

                        if session_folder_id.is_some() {
                            let mut sessions = active_sessions.lock().await;
                            if let Some(s) = sessions.get_mut(&channel_id) {
                                s.session_folder_id = session_folder_id.clone();
                                if s.title_history_file_id.is_none()
                                    && let (Some(fid), Some(drive)) =
                                        (session_folder_id.as_deref(), drive_opt.as_ref())
                                {
                                    let history_text = s.format_title_history();
                                    if let Ok(file_id) = drive
                                        .upload_text_file(
                                            fid,
                                            "title_history.txt",
                                            &history_text,
                                            None,
                                        )
                                        .await
                                    {
                                        s.title_history_file_id = Some(file_id);
                                        let _ = event_tx
                                            .send(AppEvent::Log(LogEntry::drive(format!(
                                                "Initialized 'title_history.txt' in Drive folder for {}",
                                                channel_id
                                            ))))
                                            .await;
                                    }
                                }
                            }
                        }

                        if is_finished {
                            break;
                        }
                    }
                }
            }

            session_cancel.cancel();
            if let Some(mut chat_handle) = chat_task
                && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                    .await
                    .is_err()
            {
                chat_handle.abort();
            }

            let chat_path = session_dir.join("chat.jsonl");
            if chat_path.exists()
                && let Some(ref drive) = drive_opt
            {
                if session_folder_id.is_none() {
                    let current_subfolder_name = {
                        let sessions = active_sessions.lock().await;
                        sessions
                            .get(&channel_id)
                            .map(|s| s.folder_name())
                            .unwrap_or_else(|| {
                                ActiveSessionState {
                                    start_timestamp: start_timestamp.clone(),
                                    streamer_name: info.streamer_name.clone(),
                                    current_title: info.title.clone(),
                                    session_folder_id: None,
                                    title_history: vec![],
                                    title_history_file_id: None,
                                }
                                .folder_name()
                            })
                    };
                    session_folder_id = Self::ensure_session_folder(
                        drive,
                        &root_name,
                        &current_subfolder_name,
                        &event_tx,
                    )
                    .await;
                }

                if let Some(ref fid) = session_folder_id {
                    {
                        let mut sessions = active_sessions.lock().await;
                        if let Some(s) = sessions.get_mut(&channel_id) {
                            s.session_folder_id = session_folder_id.clone();
                            if s.title_history_file_id.is_none() {
                                let history_text = s.format_title_history();
                                if let Ok(file_id) = drive
                                    .upload_text_file(fid, "title_history.txt", &history_text, None)
                                    .await
                                {
                                    s.title_history_file_id = Some(file_id);
                                    let _ = event_tx
                                            .send(AppEvent::Log(LogEntry::drive(format!(
                                                "Initialized 'title_history.txt' in Drive folder for {}",
                                                channel_id
                                            ))))
                                            .await;
                                }
                            }
                        }
                    }

                    match drive
                        .upload_file_resumable(&chat_path, fid, |_, _| {})
                        .await
                    {
                        Ok(_) => {
                            let _ = tokio::fs::remove_file(&chat_path).await;
                            let _ = event_tx
                                .send(AppEvent::Log(LogEntry::drive(format!(
                                    "Uploaded 'chat.jsonl' for {}",
                                    channel_id
                                ))))
                                .await;
                        }
                        Err(e) => {
                            let _ = event_tx
                                .send(AppEvent::Log(LogEntry::warn(format!(
                                    "Failed to upload 'chat.jsonl' for {}: {}",
                                    channel_id, e
                                ))))
                                .await;
                        }
                    }
                }
            }

            {
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
            }
            {
                let mut sessions = active_sessions.lock().await;
                sessions.remove(&channel_id);
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
                .send(AppEvent::Log(LogEntry::rec(format!(
                    "Recording session ended for channel {} (liveId: {:?})",
                    channel_id, info.live_id
                ))))
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
                                (Some(curr_id), Some(prev_id)) if curr_id != prev_id => {
                                    // liveId changed -> Genuinely new stream started!
                                    finished.remove(&channel.id);
                                    false
                                }
                                _ => {
                                    // Same liveId or one/both live_ids are None:
                                    // Guard against Chzzk CDN cache TTL delays during cooldown window.
                                    // If cooldown has elapsed and stream is still OPEN, recording was interrupted and should resume.
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
                        let rename_task = {
                            let mut sessions = self.active_sessions.lock().await;
                            if let Some(session) = sessions.get_mut(&channel.id) {
                                if session.current_title != info.title {
                                    let old_title = session.current_title.clone();
                                    let new_title = info.title.clone();
                                    let now_str =
                                        Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                                    session.record_title_change(new_title.clone(), now_str);
                                    let new_folder_name = session.folder_name();
                                    let folder_id = session.session_folder_id.clone();
                                    let history_file_id = session.title_history_file_id.clone();
                                    let history_content = session.format_title_history();
                                    Some((
                                        old_title,
                                        new_title,
                                        new_folder_name,
                                        folder_id,
                                        history_file_id,
                                        history_content,
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };

                        if let Some((
                            old_title,
                            new_title,
                            new_folder_name,
                            folder_id,
                            history_file_id,
                            history_content,
                        )) = rename_task
                        {
                            if let (Some(ref fid), Some(drive)) = (folder_id, self.drive.as_ref()) {
                                match drive.rename_folder(fid, &new_folder_name).await {
                                    Ok(_) => {
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::drive(format!(
                                                "Stream title changed ('{}' -> '{}'). Renamed session folder for {} to '{}'",
                                                old_title, new_title, channel.id, new_folder_name
                                            ))))
                                            .await;
                                    }
                                    Err(e) => {
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::warn(format!(
                                                "Failed to rename Drive folder for {} to '{}': {}",
                                                channel.id, new_folder_name, e
                                            ))))
                                            .await;
                                    }
                                }

                                match drive
                                    .upload_text_file(
                                        fid,
                                        "title_history.txt",
                                        &history_content,
                                        history_file_id.as_deref(),
                                    )
                                    .await
                                {
                                    Ok(file_id) => {
                                        let mut sessions = self.active_sessions.lock().await;
                                        if let Some(session) = sessions.get_mut(&channel.id) {
                                            session.title_history_file_id = Some(file_id);
                                        }
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::drive(format!(
                                                "Updated 'title_history.txt' in Drive folder for {}",
                                                channel.id
                                            ))))
                                            .await;
                                    }
                                    Err(e) => {
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::warn(format!(
                                                "Failed to update 'title_history.txt' for {}: {}",
                                                channel.id, e
                                            ))))
                                            .await;
                                    }
                                }
                            } else {
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::Log(LogEntry::rec(format!(
                                        "Stream title changed for {} ('{}' -> '{}'). Pending folder name updated to '{}'",
                                        channel.id, old_title, new_title, new_folder_name
                                    ))))
                                    .await;
                            }
                        }

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
                            .send(AppEvent::Log(LogEntry::poll(format!(
                                "Channel {} ({}) stream recently concluded (liveId: {:?}). Waiting for API cache to close...",
                                channel.id, channel.name, info.live_id
                            ))))
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
                        {
                            let mut sessions = self.active_sessions.lock().await;
                            sessions.insert(
                                channel.id.clone(),
                                ActiveSessionState {
                                    start_timestamp: Local::now()
                                        .format("%Y-%m-%d_%H%M")
                                        .to_string(),
                                    streamer_name: info.streamer_name.clone(),
                                    current_title: info.title.clone(),
                                    session_folder_id: None,
                                    title_history: vec![(
                                        Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                        info.title.clone(),
                                    )],
                                    title_history_file_id: None,
                                },
                            );
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
                    {
                        let mut sessions = self.active_sessions.lock().await;
                        sessions.remove(&channel.id);
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
                        .send(AppEvent::Log(LogEntry::warn(format!(
                            "Polling failed for {}: {}",
                            channel.id, e
                        ))))
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

        // 4. Clean up any empty stream session folders inside the local recordings directory
        let recordings_base = resolve_path(Path::new(&self.settings.general.recordings_dir));
        match Self::cleanup_empty_session_dirs(&recordings_base).await {
            Ok(count) if count > 0 => {
                let _ = self
                    .event_tx
                    .try_send(AppEvent::Log(LogEntry::clean(format!(
                        "Cleaned up {} empty session folder(s) in '{}'",
                        count,
                        recordings_base.display()
                    ))));
            }
            Ok(_) => {}
            Err(e) => {
                let _ = self.event_tx.try_send(AppEvent::Log(LogEntry::warn(format!(
                    "Failed to clean up empty session folders: {}",
                    e
                ))));
            }
        }

        let _ = self.event_tx.try_send(AppEvent::Log(LogEntry::info(
            "Engine graceful shutdown complete.",
        )));
    }
}
