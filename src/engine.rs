use crate::app_path::resolve_path;
use crate::chzzk::chat::ChzzkChatClient;
use crate::chzzk::client::ChzzkClient;
use crate::chzzk::models::{LiveDetail, LiveStreamInfo};
use crate::config::Settings;
use crate::recorder::ffmpeg::{
    build_ffmpeg_command, build_ffmpeg_command_with_bin, sanitize_filename,
};
use crate::recorder::watcher::SegmentWatcher;
use crate::chzzk::models_metadata::{
    MetadataDelta, MetadataEvent, MetadataEventType, StreamMetadataState,
};
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::{ProgressCallback, UploadBackend, UploadTask, broadcast_identifier};
use chrono::{Local, Utc};
use futures_util::FutureExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct FinishedSession {
    pub live_id: Option<u64>,
    pub finished_at: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct ActiveSessionState {
    pub start_timestamp: String,
    pub session_start_instant: std::time::Instant,
    pub streamer_name: String,
    pub alias: Option<String>,
    pub initial_title: String,
    pub current_title: String,
    pub current_metadata: StreamMetadataState,
    pub metadata_history: Vec<MetadataEvent>,
}

impl Default for ActiveSessionState {
    fn default() -> Self {
        Self {
            start_timestamp: String::new(),
            session_start_instant: std::time::Instant::now(),
            streamer_name: String::new(),
            alias: None,
            initial_title: String::new(),
            current_title: String::new(),
            current_metadata: StreamMetadataState::default(),
            metadata_history: Vec::new(),
        }
    }
}

impl ActiveSessionState {
    pub fn new(
        start_timestamp: String,
        streamer_name: String,
        alias: Option<String>,
        initial_metadata: StreamMetadataState,
    ) -> Self {
        let now = Local::now();
        let utc_now = Utc::now();
        let initial_title = initial_metadata.live_title.clone();

        let initial_event = MetadataEvent {
            version: 1,
            event: MetadataEventType::InitialState,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms: 0,
            changes: None,
            state: initial_metadata.clone(),
        };

        Self {
            start_timestamp,
            session_start_instant: std::time::Instant::now(),
            streamer_name,
            alias,
            initial_title: initial_title.clone(),
            current_title: initial_title,
            current_metadata: initial_metadata,
            metadata_history: vec![initial_event],
        }
    }

    pub fn folder_name(&self) -> String {
        let streamer = sanitize_filename(&self.streamer_name);
        let streamer = streamer.trim_end_matches([' ', '.']);
        let streamer = if streamer.is_empty() {
            "Unknown"
        } else {
            streamer
        };

        let title = sanitize_filename(&self.initial_title);
        let title = title.trim_end_matches([' ', '.']);
        let timestamp = &self.start_timestamp;

        let sanitized_alias = self
            .alias
            .as_deref()
            .map(|a| sanitize_filename(a).trim_matches([' ', '.']).to_string())
            .filter(|a| !a.is_empty());

        let folder = if let Some(alias) = sanitized_alias {
            if title.is_empty() {
                format!("[{timestamp}] [{alias}] {streamer}")
            } else {
                format!("[{timestamp}] [{alias}] {streamer} - {title}")
            }
        } else if title.is_empty() {
            format!("[{timestamp}] {streamer}")
        } else {
            format!("[{timestamp}] {streamer} - {title}")
        };
        folder.trim_end_matches([' ', '.']).to_string()
    }

    pub fn record_metadata_change(
        &mut self,
        new_metadata: StreamMetadataState,
    ) -> Option<(MetadataDelta, MetadataEvent)> {
        let delta = self.current_metadata.compute_delta(&new_metadata)?;
        let now = Local::now();
        let utc_now = Utc::now();
        let stream_offset_ms = self.session_start_instant.elapsed().as_millis() as u64;

        let event = MetadataEvent {
            version: 1,
            event: MetadataEventType::MetadataChanged,
            timestamp: utc_now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            time_local: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            stream_offset_ms,
            changes: Some(delta.clone()),
            state: new_metadata.clone(),
        };

        self.current_title = new_metadata.live_title.clone();
        self.current_metadata = new_metadata;
        self.metadata_history.push(event.clone());
        Some((delta, event))
    }

    pub fn format_metadata_jsonl(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        for event in &self.metadata_history {
            if let Ok(line) = serde_json::to_string(event) {
                let _ = writeln!(out, "{line}");
            }
        }
        out
    }
}

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    backend: Option<Arc<dyn UploadBackend>>,
    event_tx: Sender<AppEvent>,
    active_recordings: Arc<tokio::sync::Mutex<HashSet<String>>>,
    active_sessions: Arc<tokio::sync::Mutex<HashMap<String, ActiveSessionState>>>,
    finished_sessions: Arc<tokio::sync::Mutex<HashMap<String, FinishedSession>>>,
    restricted_channels: Arc<tokio::sync::Mutex<HashSet<String>>>,
    restricted_live_ids: Arc<tokio::sync::Mutex<HashMap<String, u64>>>,
    api_restricted_channels: Arc<tokio::sync::Mutex<HashSet<String>>>,
    channel_names: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
    session_cancel_tokens: Arc<tokio::sync::Mutex<HashMap<String, CancellationToken>>>,
    cancel_token: CancellationToken,
    refresh_notify: Arc<tokio::sync::Notify>,
    session_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ffmpeg_bin: Option<String>,
}

impl EngineOrchestrator {
    pub fn new(
        settings: Settings,
        chzzk: ChzzkClient,
        backend: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
    ) -> Self {
        Self::with_cancel_token(settings, chzzk, backend, event_tx, CancellationToken::new())
    }

    pub fn with_cancel_token(
        settings: Settings,
        chzzk: ChzzkClient,
        backend: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            settings,
            chzzk,
            backend,
            event_tx,
            active_recordings: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            active_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            finished_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            restricted_channels: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            restricted_live_ids: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            api_restricted_channels: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            channel_names: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            session_cancel_tokens: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cancel_token,
            refresh_notify: Arc::new(tokio::sync::Notify::new()),
            session_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            ffmpeg_bin: None,
        }
    }

    pub fn channel_names(&self) -> Arc<tokio::sync::RwLock<HashMap<String, String>>> {
        self.channel_names.clone()
    }

    pub fn resolve_display_name(
        channel: &crate::config::ChannelConfig,
        cached_names: &HashMap<String, String>,
    ) -> String {
        if let Some(alias) = &channel.alias {
            alias.clone()
        } else if let Some(official) = cached_names.get(&channel.id) {
            official.clone()
        } else {
            channel.id.clone()
        }
    }

    pub fn with_ffmpeg_bin(mut self, bin: impl Into<String>) -> Self {
        self.ffmpeg_bin = Some(bin.into());
        self
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

    pub fn restricted_channels(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.restricted_channels.clone()
    }

    pub fn restricted_live_ids(&self) -> Arc<tokio::sync::Mutex<HashMap<String, u64>>> {
        self.restricted_live_ids.clone()
    }

    pub fn api_restricted_channels(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.api_restricted_channels.clone()
    }

    pub fn session_cancel_tokens(
        &self,
    ) -> Arc<tokio::sync::Mutex<HashMap<String, CancellationToken>>> {
        self.session_cancel_tokens.clone()
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
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        Self::spawn_upload_consumer_with_concurrency(backend_opt, event_tx, upload_rx, 3)
    }

    pub fn spawn_upload_consumer_with_concurrency(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        mut upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
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
                    if let Some(queue) = channel_queues.get_mut(&ch_id)
                        && let Some(task) = queue.pop_front()
                    {
                        active_channels.insert(ch_id.clone());
                        let backend = backend_opt.clone();
                        let event_tx = event_tx.clone();
                        let task_cid = ch_id.clone();

                        join_set.spawn(async move {
                            let _ = std::panic::AssertUnwindSafe(async move {
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

                                            // If the chunk's parent directory is an empty session directory for this channel, clean it up
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
                                                    && let Ok(mut rd) =
                                                        tokio::fs::read_dir(parent).await
                                                    && rd
                                                        .next_entry()
                                                        .await
                                                        .ok()
                                                        .flatten()
                                                        .is_none()
                                                    && tokio::fs::remove_dir(parent).await.is_ok()
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
                            task_cid
                        });
                    }
                }

                // If upload receiver is closed and no uploads remain in-flight or queued, exit
                if rx_closed && join_set.is_empty() && channel_queues.is_empty() {
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
                }
            }
        })
    }

    pub async fn cleanup_empty_session_dirs_excluding(
        recordings_dir: &Path,
        active_channels: &HashSet<String>,
    ) -> std::io::Result<usize> {
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
                let dir_name = entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();

                let is_active = active_channels.iter().any(|item| {
                    dir_name_str == item.as_str() || dir_name_str.starts_with(&format!("{item}_"))
                });
                if is_active {
                    continue;
                }

                let mut is_empty_or_metadata_only = true;
                let mut has_metadata = false;
                if let Ok(mut sub_entries) = tokio::fs::read_dir(&path).await {
                    while let Ok(Some(sub_entry)) = sub_entries.next_entry().await {
                        if sub_entry.file_name() == "metadata.jsonl" {
                            has_metadata = true;
                        } else {
                            is_empty_or_metadata_only = false;
                            break;
                        }
                    }
                    if is_empty_or_metadata_only {
                        if has_metadata {
                            let _ = tokio::fs::remove_file(path.join("metadata.jsonl")).await;
                        }
                        if tokio::fs::remove_dir(&path).await.is_ok() {
                            removed_count += 1;
                        }
                    }
                }
            }
        }
        Ok(removed_count)
    }

    pub async fn cleanup_empty_session_dirs(recordings_dir: &Path) -> std::io::Result<usize> {
        Self::cleanup_empty_session_dirs_excluding(recordings_dir, &HashSet::new()).await
    }

    pub async fn process_sealed_chunk(
        chunk_path: &Path,
        remote_dir: &str,
        channel_id: &str,
        streamer_name: &str,
        upload_tx: &Sender<UploadTask>,
        event_tx: &Sender<AppEvent>,
        backend_active: bool,
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
            Self::process_sealed_chunk(
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
        let backend_opt = self.backend.clone();
        let chzzk = self.chzzk.clone();
        let event_tx = self.event_tx.clone();
        let active_recordings = self.active_recordings.clone();
        let active_sessions = self.active_sessions.clone();
        let finished_sessions = self.finished_sessions.clone();
        let restricted_channels = self.restricted_channels.clone();
        let restricted_live_ids = self.restricted_live_ids.clone();
        let session_cancel_tokens = self.session_cancel_tokens.clone();
        let cancel_token = self.cancel_token.clone();
        let ffmpeg_bin = self.ffmpeg_bin.clone();

        let handle = tokio::spawn(async move {
            let _ = event_tx
                .send(AppEvent::RecordingStarted {
                    channel_id: channel_id.clone(),
                    session_title: info.title.clone(),
                })
                .await;

            let alias = settings
                .channels
                .iter()
                .find(|c| c.id == channel_id)
                .and_then(|c| c.alias.clone());

            let (session_folder_name, start_timestamp, initial_metadata_jsonl) = {
                let mut sessions = active_sessions.lock().await;
                let session = sessions.entry(channel_id.clone()).or_insert_with(|| {
                    let start_timestamp = Local::now().format("%Y-%m-%d_%H%M%S").to_string();
                    ActiveSessionState::new(
                        start_timestamp,
                        info.streamer_name.clone(),
                        alias.clone(),
                        info.metadata.clone(),
                    )
                });
                (
                    session.folder_name(),
                    session.start_timestamp.clone(),
                    session.format_metadata_jsonl(),
                )
            };

            let recordings_base = resolve_path(Path::new(&settings.general.recordings_dir));
            let session_dir = recordings_base.join(&session_folder_name);

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

            let metadata_path = session_dir.join("metadata.jsonl");
            if let Err(e) = tokio::fs::write(&metadata_path, &initial_metadata_jsonl).await {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "[{channel_id}] Failed to write 'metadata.jsonl': {e}"
                    ))))
                    .await;
            }

            if let Some(ref backend) = backend_opt {
                let backend = backend.clone();
                let remote_dir = session_folder_name.clone();
                let initial_jsonl = initial_metadata_jsonl.clone();
                let event_tx_clone = event_tx.clone();
                let channel_id_clone = channel_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = backend
                        .upload_text(&remote_dir, "metadata.jsonl", &initial_jsonl)
                        .await
                    {
                        let _ = event_tx_clone
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "[{channel_id_clone}] Failed to upload initial 'metadata.jsonl': {e}"
                            ))))
                            .await;
                    } else {
                        let _ = event_tx_clone
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "[{channel_id_clone}] Uploaded initial 'metadata.jsonl'"
                            ))))
                            .await;
                    }
                });
            }
            let session_cancel = cancel_token.child_token();
            {
                let mut tokens = session_cancel_tokens.lock().await;
                tokens.insert(channel_id.clone(), session_cancel.clone());
            }

            let (chat_sealed_tx, mut chat_sealed_rx) = tokio::sync::mpsc::channel::<PathBuf>(32);
            let chat_forward_handle = {
                let upload_tx = upload_tx.clone();
                let event_tx = event_tx.clone();
                let channel_id = channel_id.clone();
                let session_folder = session_folder_name.clone();
                let streamer = info.streamer_name.clone();
                let backend_active = backend_opt.is_some();

                tokio::spawn(async move {
                    while let Some(chat_path) = chat_sealed_rx.recv().await {
                        let chunk_name = match chat_path.file_name().and_then(|n| n.to_str()) {
                            Some(n) => n.to_string(),
                            None => continue,
                        };
                        let size = tokio::fs::metadata(&chat_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);

                        let _ = event_tx
                            .send(AppEvent::ChunkSealed {
                                chunk_name: chunk_name.clone(),
                                size_bytes: size,
                            })
                            .await;

                        let target = broadcast_identifier(&streamer, &channel_id);

                        if backend_active {
                            let send_res = upload_tx
                                .send(UploadTask {
                                    channel_id: channel_id.clone(),
                                    session_folder_id: session_folder.clone(),
                                    remote_dir: session_folder.clone(),
                                    chunk_path: chat_path,
                                    chunk_name: chunk_name.clone(),
                                    streamer_name: streamer.clone(),
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
                })
            };

            let chat_session_cancel = session_cancel.clone();
            let chat_task = if settings.general.record_chat {
                if let Some(chat_cid) = info.chat_channel_id.clone() {
                    let chzzk_chat = chzzk.clone();
                    let event_tx_chat = event_tx.clone();
                    let chat_cid_id = channel_id.clone();
                    let chat_session_dir = session_dir.clone();
                    let flush_sec = settings.general.chat_flush_interval_seconds;
                    let chat_chunk_sealed_tx = chat_sealed_tx.clone();

                    Some(tokio::spawn(async move {
                        let access_token = tokio::select! {
                            _ = chat_session_cancel.cancelled() => return,
                            res = chzzk_chat.get_chat_access_token(&chat_cid) => match res {
                                Ok(t) => {
                                    let _ = event_tx_chat
                                        .send(AppEvent::Log(LogEntry::chat(format!(
                                            "Retrieved chat access token for channel {chat_cid_id}"
                                        ))))
                                        .await;
                                    t
                                }
                                Err(e) => {
                                    let _ = event_tx_chat
                                        .send(AppEvent::Log(LogEntry::warn(format!(
                                            "Failed to retrieve chat access token for channel {chat_cid_id}: {e}"
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

                        let chunk_dur =
                            Duration::from_secs(settings.general.chunk_duration_seconds);
                        let mut client = ChzzkChatClient::new(
                            chat_cid,
                            access_token,
                            chat_session_dir,
                            chunk_dur,
                            Duration::from_secs(flush_sec),
                            chat_session_cancel,
                        );
                        if let Some(ws_url) = chzzk_chat.chat_ws_url() {
                            client = client.with_custom_ws_url(ws_url);
                        }

                        let _ = event_tx_chat
                            .send(AppEvent::Log(LogEntry::chat(format!(
                                "Started real-time chat recording for channel {chat_cid_id}"
                            ))))
                            .await;

                        match client.run(Some(stats_tx), Some(chat_chunk_sealed_tx)).await {
                            Ok(total_msgs) => {
                                let _ = event_tx_chat
                                    .send(AppEvent::Log(LogEntry::chat(format!(
                                        "Chat recording finished for channel {chat_cid_id} ({total_msgs} messages)"
                                    ))))
                                    .await;
                            }
                            Err(e) => {
                                let _ = event_tx_chat
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Chat recording error for channel {chat_cid_id}: {e}"
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
            drop(chat_sealed_tx);

            let output_pattern = session_dir.join("chunk_%04d.ts");
            let chunk_dur = settings.general.chunk_duration_seconds;
            let cookie = chzzk.cookie_header();
            let mut cmd = match ffmpeg_bin.as_deref() {
                Some(bin) => build_ffmpeg_command_with_bin(
                    bin,
                    &info.hls_url,
                    &output_pattern,
                    chunk_dur,
                    cookie,
                ),
                None => build_ffmpeg_command(&info.hls_url, &output_pattern, chunk_dur, cookie),
            };

            let key_forbidden = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let key_forbidden_notify = Arc::new(tokio::sync::Notify::new());

            let mut child = match cmd.spawn() {
                Ok(mut child) => {
                    if let Some(stderr) = child.stderr.take() {
                        let event_tx_stderr = event_tx.clone();
                        let key_forbidden_stderr = key_forbidden.clone();
                        let key_forbidden_notify = key_forbidden_notify.clone();
                        tokio::spawn(async move {
                            use std::sync::atomic::Ordering;
                            use tokio::io::{AsyncBufReadExt, BufReader};
                            let mut reader = BufReader::new(stderr);
                            let mut line_buf = String::new();
                            while let Ok(n) = reader.read_line(&mut line_buf).await {
                                if n == 0 {
                                    break;
                                }
                                let trimmed = line_buf.trim();
                                if !trimmed.is_empty() {
                                    let is_key_error = trimmed.contains("Unable to open key file")
                                        || (trimmed.contains("403 Forbidden")
                                            && (trimmed.contains("aes_key")
                                                || trimmed.contains("key file")))
                                        || (trimmed.contains("aes_key")
                                            && trimmed.contains("access denied"));

                                    if is_key_error {
                                        key_forbidden_stderr.store(true, Ordering::SeqCst);
                                        key_forbidden_notify.notify_one();
                                    }

                                    if !key_forbidden_stderr.load(Ordering::SeqCst) {
                                        let _ = event_tx_stderr
                                            .try_send(AppEvent::Log(LogEntry::ffmpeg(trimmed)));
                                    }
                                }
                                line_buf.clear();
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
                            "Failed to spawn FFmpeg: {e}"
                        ))))
                        .await;
                    session_cancel.cancel();
                    {
                        let mut tokens = session_cancel_tokens.lock().await;
                        tokens.remove(&channel_id);
                    }
                    if let Some(mut chat_handle) = chat_task
                        && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                            .await
                            .is_err()
                    {
                        chat_handle.abort();
                    }
                    if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                        while let Ok(Some(entry)) = rd.next_entry().await {
                            let _ = tokio::fs::remove_file(entry.path()).await;
                        }
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
            let mut pending_chunks: VecDeque<PathBuf> = VecDeque::new();
            let mut restricted_abort = false;

            loop {
                tokio::select! {
                    _ = key_forbidden_notify.notified() => {
                        restricted_abort = true;
                        break;
                    }
                    _ = session_cancel.cancelled() => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "Cancellation received for channel {channel_id}, stopping FFmpeg gracefully..."
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
                                        "FFmpeg process exited cleanly: {status}"
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

                        // Collect lingering chunks on cancellation
                        let final_chunks = watcher.detect_sealed(true);
                        for chunk_path in final_chunks {
                            if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                let size = tokio::fs::metadata(&chunk_path)
                                    .await
                                    .map(|m| m.len())
                                    .unwrap_or(0);
                                let _ = event_tx
                                    .send(AppEvent::ChunkSealed {
                                        chunk_name: chunk_name.to_string(),
                                        size_bytes: size,
                                    })
                                    .await;
                            }
                            pending_chunks.push_back(chunk_path);
                        }

                        let target = broadcast_identifier(&info.streamer_name, &channel_id);
                        if backend_opt.is_some() {
                            while let Some(chunk_path) = pending_chunks.pop_front() {
                                if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                    let send_res = upload_tx
                                        .send(UploadTask {
                                            channel_id: channel_id.to_string(),
                                            session_folder_id: session_folder_name.clone(),
                                            remote_dir: session_folder_name.clone(),
                                            chunk_path: chunk_path.clone(),
                                            chunk_name: chunk_name.to_string(),
                                            streamer_name: info.streamer_name.clone(),
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
                                }
                            }
                        } else {
                            while let Some(chunk_path) = pending_chunks.pop_front() {
                                if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::rec(format!(
                                            "[{target}] {chunk_name} sealed (saved locally)."
                                        ))))
                                        .await;
                                }
                            }
                        }

                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if key_forbidden.load(std::sync::atomic::Ordering::SeqCst) {
                            restricted_abort = true;
                            break;
                        }

                        let is_finished = match child.try_wait() {
                            Ok(Some(status)) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(LogEntry::rec(format!(
                                        "FFmpeg process exited with status: {status}"
                                    ))))
                                    .await;
                                true
                            }
                            Ok(None) => false,
                            Err(e) => {
                                let _ = event_tx
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Error waiting on FFmpeg child: {e}"
                                    ))))
                                    .await;
                                true
                            }
                        };

                        let newly_sealed = watcher.detect_sealed(is_finished);
                        for chunk_path in newly_sealed {
                            if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                let size = tokio::fs::metadata(&chunk_path)
                                    .await
                                    .map(|m| m.len())
                                    .unwrap_or(0);
                                let _ = event_tx
                                    .send(AppEvent::ChunkSealed {
                                        chunk_name: chunk_name.to_string(),
                                        size_bytes: size,
                                    })
                                    .await;
                            }
                            pending_chunks.push_back(chunk_path);
                        }

                        let target = broadcast_identifier(&info.streamer_name, &channel_id);
                        if backend_opt.is_some() {
                            while let Some(chunk_path) = pending_chunks.pop_front() {
                                if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                    let send_res = upload_tx
                                        .send(UploadTask {
                                            channel_id: channel_id.to_string(),
                                            session_folder_id: session_folder_name.clone(),
                                            remote_dir: session_folder_name.clone(),
                                            chunk_path: chunk_path.clone(),
                                            chunk_name: chunk_name.to_string(),
                                            streamer_name: info.streamer_name.clone(),
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
                                }
                            }
                        } else {
                            while let Some(chunk_path) = pending_chunks.pop_front() {
                                if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::rec(format!(
                                            "[{target}] {chunk_name} sealed (saved locally)."
                                        ))))
                                        .await;
                                }
                            }
                        }

                        if is_finished {
                            break;
                        }
                    }
                }
            }

            if restricted_abort {
                let _ = child.kill().await;
                let _ = child.wait().await;

                session_cancel.cancel();
                if let Some(mut chat_handle) = chat_task
                    && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                        .await
                        .is_err()
                {
                    chat_handle.abort();
                }

                // Clean up session directory and any empty/partial files
                if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                    while let Ok(Some(entry)) = rd.next_entry().await {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                    }
                }
                let _ = tokio::fs::remove_dir(&session_dir).await;

                {
                    let mut tokens = session_cancel_tokens.lock().await;
                    tokens.remove(&channel_id);
                }
                {
                    let mut active = active_recordings.lock().await;
                    active.remove(&channel_id);
                }
                {
                    let mut sessions = active_sessions.lock().await;
                    sessions.remove(&channel_id);
                }

                let is_newly_restricted = {
                    let mut restricted = restricted_channels.lock().await;
                    restricted.insert(channel_id.clone())
                };

                if let Some(lid) = info.live_id {
                    let mut r_ids = restricted_live_ids.lock().await;
                    r_ids.insert(channel_id.clone(), lid);
                }

                if is_newly_restricted {
                    let log_msg = format!(
                        "Recording unavailable for channel {} ({}): restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                        channel_id, info.streamer_name
                    );
                    let _ = event_tx.send(AppEvent::Log(LogEntry::error(log_msg))).await;
                }

                let _ = event_tx
                    .send(AppEvent::RecordingEnded {
                        channel_id: channel_id.clone(),
                    })
                    .await;

                let _ = event_tx
                    .send(AppEvent::ChannelUpdate {
                        channel_id: channel_id.clone(),
                        channel_name: info.streamer_name.clone(),
                        is_live: true,
                        title: info.title.clone(),
                    })
                    .await;

                return;
            }

            // Drain any remaining chunks if the process finished and chunks were queued
            if !pending_chunks.is_empty() {
                let target = broadcast_identifier(&info.streamer_name, &channel_id);
                if backend_opt.is_some() {
                    while let Some(chunk_path) = pending_chunks.pop_front() {
                        if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                            let send_res = upload_tx
                                .send(UploadTask {
                                    channel_id: channel_id.to_string(),
                                    session_folder_id: session_folder_name.clone(),
                                    remote_dir: session_folder_name.clone(),
                                    chunk_path: chunk_path.clone(),
                                    chunk_name: chunk_name.to_string(),
                                    streamer_name: info.streamer_name.clone(),
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
                        }
                    }
                } else {
                    while let Some(chunk_path) = pending_chunks.pop_front() {
                        if let Some(chunk_name) = chunk_path.file_name().and_then(|n| n.to_str()) {
                            let _ = event_tx
                                .send(AppEvent::Log(LogEntry::rec(format!(
                                    "[{target}] {chunk_name} sealed (saved locally)."
                                ))))
                                .await;
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
            let _ = tokio::time::timeout(Duration::from_secs(2), chat_forward_handle).await;

            let metadata_jsonl = {
                let sessions = active_sessions.lock().await;
                sessions
                    .get(&channel_id)
                    .map(|s| s.format_metadata_jsonl())
                    .unwrap_or_default()
            };
            if !metadata_jsonl.is_empty()
                && let Some(ref backend) = backend_opt
            {
                match backend
                    .upload_text(&session_folder_name, "metadata.jsonl", &metadata_jsonl)
                    .await
                {
                    Ok(_) => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "Uploaded 'metadata.jsonl' for {channel_id}"
                            ))))
                            .await;
                    }
                    Err(e) => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::warn(format!(
                                "Failed to upload 'metadata.jsonl' for {channel_id}: {e}"
                            ))))
                            .await;
                    }
                }
            }

            {
                let mut tokens = session_cancel_tokens.lock().await;
                tokens.remove(&channel_id);
            }
            {
                let mut active = active_recordings.lock().await;
                active.remove(&channel_id);
            }
            {
                let mut sessions = active_sessions.lock().await;
                if sessions
                    .get(&channel_id)
                    .is_some_and(|s| s.start_timestamp == start_timestamp)
                {
                    sessions.remove(&channel_id);
                }
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

            // Clean up session directory if empty (e.g. no chunks were saved or all chunks/chat were already uploaded)
            let target = broadcast_identifier(&info.streamer_name, &channel_id);
            let mut is_empty_or_metadata_only = true;
            let mut has_metadata = false;
            if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                while let Ok(Some(sub_entry)) = rd.next_entry().await {
                    if sub_entry.file_name() == "metadata.jsonl" {
                        has_metadata = true;
                    } else {
                        is_empty_or_metadata_only = false;
                        break;
                    }
                }
                if is_empty_or_metadata_only {
                    if has_metadata {
                        let _ = tokio::fs::remove_file(session_dir.join("metadata.jsonl")).await;
                    }
                    if tokio::fs::remove_dir(&session_dir).await.is_ok() {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::clean(format!(
                                "[{target}] Cleaned up empty session folder '{}'",
                                session_dir.display()
                            ))))
                            .await;
                    }
                }
            }
        });

        if let Ok(mut guard) = self.session_handles.lock() {
            guard.retain(|h| !h.is_finished());
            guard.push(handle);
        }
    }

    pub async fn poll_channels_once(&self, upload_tx: &Sender<UploadTask>) {
        for channel in &self.settings.channels {
            let detail_res = tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                res = self.chzzk.get_live_detail(&channel.id) => res,
            };
            match detail_res {
                Ok(LiveDetail::Open(info)) => {
                    {
                        let mut names = self.channel_names.write().await;
                        names.insert(channel.id.clone(), info.streamer_name.clone());
                    }
                    let display_name = {
                        let names = self.channel_names.read().await;
                        Self::resolve_display_name(channel, &names)
                    };

                    let was_api_restricted = {
                        let mut api_restricted = self.api_restricted_channels.lock().await;
                        api_restricted.remove(&channel.id)
                    };

                    if was_api_restricted {
                        {
                            let mut r_ids = self.restricted_live_ids.lock().await;
                            r_ids.remove(&channel.id);
                        }
                        {
                            let mut finished = self.finished_sessions.lock().await;
                            finished.remove(&channel.id);
                        }
                        {
                            let mut restricted = self.restricted_channels.lock().await;
                            restricted.remove(&channel.id);
                        }

                        let log_msg = format!(
                            "Restricted stream for channel {} ({}) returned to public broadcast (liveId: {:?}). Starting new recording session in new broadcast folder...",
                            channel.id, display_name, info.live_id
                        );
                        let _ = self
                            .event_tx
                            .send(AppEvent::Log(LogEntry::rec(log_msg)))
                            .await;
                    }

                    let is_live_id_restricted = if let Some(lid) = info.live_id {
                        let r_ids = self.restricted_live_ids.lock().await;
                        r_ids.get(&channel.id).copied() == Some(lid)
                    } else {
                        false
                    };

                    if is_live_id_restricted {
                        let is_newly_restricted = {
                            let mut restricted = self.restricted_channels.lock().await;
                            restricted.insert(channel.id.clone())
                        };

                        if is_newly_restricted {
                            let log_msg = format!(
                                "Recording unavailable for channel {} ({}): restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                                channel.id, display_name
                            );
                            let _ = self
                                .event_tx
                                .send(AppEvent::Log(LogEntry::error(log_msg)))
                                .await;
                        }

                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: display_name.clone(),
                                is_live: true,
                                title: info.title.clone(),
                            })
                            .await;
                        continue;
                    }

                    {
                        let mut restricted = self.restricted_channels.lock().await;
                        restricted.remove(&channel.id);
                    }

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
                        let metadata_change = {
                            let mut sessions = self.active_sessions.lock().await;
                            if let Some(session) = sessions.get_mut(&channel.id) {
                                if let Some((delta, event)) =
                                    session.record_metadata_change(info.metadata.clone())
                                {
                                    let remote_dir = session.folder_name();
                                    let full_jsonl = session.format_metadata_jsonl();
                                    Some((delta, event, remote_dir, full_jsonl))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };

                        if let Some((delta, event, remote_dir, full_jsonl)) = metadata_change {
                            // 1. Append locally to <session_dir>/metadata.jsonl
                            let recordings_base =
                                resolve_path(Path::new(&self.settings.general.recordings_dir));
                            let session_dir = recordings_base.join(&remote_dir);
                            let event_line = serde_json::to_string(&event).unwrap_or_default();
                            if !event_line.is_empty() {
                                use tokio::io::AsyncWriteExt;
                                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(session_dir.join("metadata.jsonl"))
                                    .await
                                {
                                    let _ =
                                        file.write_all(format!("{event_line}\n").as_bytes()).await;
                                }
                            }

                            // 2. Synchronize to cloud storage via rcat
                            if let Some(backend) = self.backend.as_ref() {
                                let res = backend
                                    .upload_text(&remote_dir, "metadata.jsonl", &full_jsonl)
                                    .await;
                                match res {
                                    Ok(_) => {
                                        let channel_id = &channel.id;
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::rec(format!(
                                                "[{channel_id}] Stream metadata changed. Updated 'metadata.jsonl'"
                                            ))))
                                            .await;
                                    }
                                    Err(e) => {
                                        let channel_id = &channel.id;
                                        let _ = self
                                            .event_tx
                                            .send(AppEvent::Log(LogEntry::warn(format!(
                                                "[{channel_id}] Failed to update 'metadata.jsonl': {e}"
                                            ))))
                                            .await;
                                    }
                                }
                            } else {
                                let channel_id = &channel.id;
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::Log(LogEntry::rec(format!(
                                        "[{channel_id}] Stream metadata changed. Updated 'metadata.jsonl'"
                                    ))))
                                    .await;
                            }

                            if delta.live_title.is_some() {
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::ChannelUpdate {
                                        channel_id: channel.id.clone(),
                                        channel_name: display_name.clone(),
                                        is_live: true,
                                        title: info.title.clone(),
                                    })
                                    .await;
                            }
                        }

                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: display_name.clone(),
                                is_live: true,
                                title: info.title.clone(),
                            })
                            .await;
                    } else if is_duplicate_or_cooldown {
                        let _ = self
                            .event_tx
                            .send(AppEvent::Log(LogEntry::poll(format!(
                                "Channel {} ({}) stream recently concluded (liveId: {:?}). Waiting for API cache to close...",
                                channel.id,
                                display_name,
                                info.live_id
                            ))))
                            .await;

                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: display_name.clone(),
                                is_live: false,
                                title: "Stream Concluded (Cooldown)".to_string(),
                            })
                            .await;
                    } else {
                        let _ = self
                            .event_tx
                            .send(AppEvent::ChannelUpdate {
                                channel_id: channel.id.clone(),
                                channel_name: display_name.clone(),
                                is_live: true,
                                title: info.title.clone(),
                            })
                            .await;

                        let start_timestamp = if was_api_restricted {
                            Local::now().format("%Y-%m-%d_%H%M%S").to_string()
                        } else {
                            Local::now().format("%Y-%m-%d_%H%M").to_string()
                        };

                        {
                            let mut active = self.active_recordings.lock().await;
                            active.insert(channel.id.clone());
                        }
                        let alias = channel.alias.clone();
                        {
                            let mut sessions = self.active_sessions.lock().await;
                            sessions.insert(
                                channel.id.clone(),
                                ActiveSessionState::new(
                                    start_timestamp,
                                    info.streamer_name.clone(),
                                    alias,
                                    info.metadata.clone(),
                                ),
                            );
                        }
                        self.spawn_recording_session(channel.id.clone(), info, upload_tx.clone());
                    }
                }
                Ok(LiveDetail::Restricted {
                    channel_id: _,
                    live_id: _,
                    streamer_name,
                    title,
                    chat_channel_id: _,
                    adult,
                }) => {
                    if let Some(token) = self.session_cancel_tokens.lock().await.remove(&channel.id)
                    {
                        token.cancel();
                    }

                    let was_active = {
                        let mut active = self.active_recordings.lock().await;
                        active.remove(&channel.id)
                    };
                    if was_active {
                        let mut sessions = self.active_sessions.lock().await;
                        sessions.remove(&channel.id);
                        let _ = self
                            .event_tx
                            .send(AppEvent::RecordingEnded {
                                channel_id: channel.id.clone(),
                            })
                            .await;
                    }

                    {
                        let mut api_restricted = self.api_restricted_channels.lock().await;
                        api_restricted.insert(channel.id.clone());
                    }

                    let is_newly_restricted = {
                        let mut restricted = self.restricted_channels.lock().await;
                        restricted.insert(channel.id.clone())
                    };

                    {
                        let mut names = self.channel_names.write().await;
                        names.insert(channel.id.clone(), streamer_name.clone());
                    }
                    let display_name = {
                        let names = self.channel_names.read().await;
                        Self::resolve_display_name(channel, &names)
                    };

                    if is_newly_restricted {
                        let log_msg = if adult {
                            format!(
                                "Recording unavailable for channel {} ({}): 19+ age-restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                                channel.id, display_name
                            )
                        } else {
                            format!(
                                "Recording unavailable for channel {} ({}): restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                                channel.id, display_name
                            )
                        };
                        let _ = self
                            .event_tx
                            .send(AppEvent::Log(LogEntry::error(log_msg)))
                            .await;
                    }

                    let _ = self
                        .event_tx
                        .send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: display_name,
                            is_live: true,
                            title,
                        })
                        .await;
                }
                Ok(LiveDetail::Close { streamer_name }) => {
                    if let Some(streamer) = streamer_name {
                        let mut names = self.channel_names.write().await;
                        names.insert(channel.id.clone(), streamer);
                    }
                    let display_name = {
                        let names = self.channel_names.read().await;
                        Self::resolve_display_name(channel, &names)
                    };

                    // Channel reported CLOSE (offline)
                    if let Some(token) = self.session_cancel_tokens.lock().await.remove(&channel.id)
                    {
                        token.cancel();
                    }
                    {
                        let mut api_restricted = self.api_restricted_channels.lock().await;
                        api_restricted.remove(&channel.id);
                    }
                    {
                        let mut restricted = self.restricted_channels.lock().await;
                        restricted.remove(&channel.id);
                    }
                    {
                        let mut r_ids = self.restricted_live_ids.lock().await;
                        r_ids.remove(&channel.id);
                    }
                    {
                        let mut finished = self.finished_sessions.lock().await;
                        finished.remove(&channel.id);
                    }
                    {
                        let mut sessions = self.active_sessions.lock().await;
                        sessions.remove(&channel.id);
                    }

                    let recordings_base =
                        resolve_path(Path::new(&self.settings.general.recordings_dir));
                    let active_dirs = {
                        let sessions = self.active_sessions.lock().await;
                        let active_rec = self.active_recordings.lock().await;
                        let mut set: HashSet<String> =
                            sessions.values().map(|s| s.folder_name()).collect();
                        set.extend(active_rec.iter().cloned());
                        set
                    };
                    if let Ok(count) =
                        Self::cleanup_empty_session_dirs_excluding(&recordings_base, &active_dirs)
                            .await
                        && count > 0
                    {
                        let _ = self
                            .event_tx
                            .send(AppEvent::Log(LogEntry::clean(format!(
                                "Cleaned up {count} empty session folder(s) in '{}'",
                                recordings_base.display()
                            ))))
                            .await;
                    }

                    let _ = self
                        .event_tx
                        .send(AppEvent::ChannelUpdate {
                            channel_id: channel.id.clone(),
                            channel_name: display_name,
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
        let concurrency = self.settings.rclone.upload_concurrency;
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<UploadTask>(50);
        let upload_handle = Self::spawn_upload_consumer_with_concurrency(
            self.backend.clone(),
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

            let recordings_base = resolve_path(Path::new(&self.settings.general.recordings_dir));
            let active_dirs = {
                let sessions = self.active_sessions.lock().await;
                let active_rec = self.active_recordings.lock().await;
                let mut set: HashSet<String> = sessions.values().map(|s| s.folder_name()).collect();
                set.extend(active_rec.iter().cloned());
                set
            };
            if let Ok(count) =
                Self::cleanup_empty_session_dirs_excluding(&recordings_base, &active_dirs).await
                && count > 0
            {
                let _ = self
                    .event_tx
                    .try_send(AppEvent::Log(LogEntry::clean(format!(
                        "Cleaned up {count} empty session folder(s) in '{}'",
                        recordings_base.display()
                    ))));
            }

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
                    "Failed to clean up empty session folders: {e}"
                ))));
            }
        }

        let _ = self.event_tx.try_send(AppEvent::Log(LogEntry::info(
            "Engine graceful shutdown complete.",
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session(
        start_timestamp: &str,
        streamer_name: &str,
        alias: Option<&str>,
        title: &str,
    ) -> ActiveSessionState {
        ActiveSessionState::new(
            start_timestamp.to_string(),
            streamer_name.to_string(),
            alias.map(|s| s.to_string()),
            StreamMetadataState {
                channel_name: streamer_name.to_string(),
                live_title: title.to_string(),
                ..Default::default()
            },
        )
    }

    #[test]
    fn test_active_session_state_stable_folder_name() {
        let mut session = test_session(
            "2026-09-29_120000",
            "Streamer",
            None,
            "Initial Title",
        );
        let initial_folder = session.folder_name();
        assert_eq!(
            initial_folder,
            "[2026-09-29_120000] Streamer - Initial Title"
        );
        assert_eq!(session.initial_title, "Initial Title");

        let mut updated_meta = session.current_metadata.clone();
        updated_meta.live_title = "Updated Title".to_string();
        session.record_metadata_change(updated_meta);
        assert_eq!(session.current_title, "Updated Title");
        assert_eq!(session.folder_name(), initial_folder);
    }

    #[test]
    fn test_active_session_state_folder_name_with_alias() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer",
            Some("MyAlias"),
            "My Stream",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] [MyAlias] Streamer - My Stream"
        );
    }

    #[test]
    fn test_active_session_state_folder_name_without_alias() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer",
            None,
            "My Stream",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] Streamer - My Stream"
        );
    }

    #[test]
    fn test_active_session_state_folder_name_empty_alias_fallback() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer",
            Some("   "),
            "My Stream",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] Streamer - My Stream"
        );
    }

    #[test]
    fn test_active_session_state_folder_name_sanitization() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer/Name...",
            Some("Alias:Special "),
            "Gaming Stream? Playing Now... ",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] [Alias_Special] Streamer_Name - Gaming Stream_ Playing Now"
        );
    }

    #[test]
    fn test_active_session_state_folder_name_dots_alias_fallback() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer",
            Some("..."),
            "My Stream",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] Streamer - My Stream"
        );
    }

    #[test]
    fn test_active_session_state_folder_name_empty_title() {
        let session = test_session(
            "2026-09-30_1100",
            "Streamer",
            Some("MyAlias"),
            "...",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] [MyAlias] Streamer"
        );

        let session_no_alias = test_session(
            "2026-09-30_1100",
            "Streamer",
            None,
            "   ",
        );
        assert_eq!(session_no_alias.folder_name(), "[2026-09-30_1100] Streamer");
    }

    #[test]
    fn test_active_session_state_folder_name_empty_streamer() {
        let session = test_session(
            "2026-09-30_1100",
            "...",
            Some("MyAlias"),
            "My Stream",
        );
        assert_eq!(
            session.folder_name(),
            "[2026-09-30_1100] [MyAlias] Unknown - My Stream"
        );
    }

    #[test]
    fn test_active_session_state_metadata_jsonl_formatting() {
        use crate::chzzk::models_metadata::{MetadataEventType, StreamMetadataState};

        let mut initial_meta = StreamMetadataState::default();
        initial_meta.channel_name = "TestStreamer".to_string();
        initial_meta.live_title = "Initial Title".to_string();

        let mut session = ActiveSessionState::new(
            "2026-09-30_140000".to_string(),
            "TestStreamer".to_string(),
            Some("Alias".to_string()),
            initial_meta.clone(),
        );

        assert_eq!(session.metadata_history.len(), 1);
        assert_eq!(session.metadata_history[0].event, MetadataEventType::InitialState);
        assert_eq!(session.metadata_history[0].stream_offset_ms, 0);

        let mut updated_meta = initial_meta.clone();
        updated_meta.live_title = "Second Title".to_string();
        let change = session.record_metadata_change(updated_meta);
        assert!(change.is_some());
        assert_eq!(session.metadata_history.len(), 2);
        assert_eq!(session.metadata_history[1].event, MetadataEventType::MetadataChanged);

        let jsonl = session.format_metadata_jsonl();
        let lines: Vec<&str> = jsonl.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"INITIAL_STATE\""));
        assert!(lines[1].contains("\"METADATA_CHANGED\""));
        assert!(lines[1].contains("\"Second Title\""));
    }

    #[tokio::test]
    async fn test_upload_consumer_with_mock_backend() {
        use crate::uploader::MockUploadBackend;

        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_engine_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .expect("create tempdir");
        let chunk_file = temp_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_file, b"test chunk content")
            .await
            .expect("write chunk");

        let mock_backend = Arc::new(MockUploadBackend::new());
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(10);

        let consumer_handle = EngineOrchestrator::spawn_upload_consumer(
            Some(mock_backend.clone()),
            event_tx,
            upload_rx,
        );

        let task = UploadTask {
            channel_id: "test_channel".to_string(),
            session_folder_id: "[2026-09-29_120000] Streamer - Title".to_string(),
            remote_dir: "[2026-09-29_120000] Streamer - Title".to_string(),
            chunk_path: chunk_file.clone(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "Streamer".to_string(),
        };

        upload_tx.send(task).await.expect("send task");
        drop(upload_tx);

        consumer_handle.await.expect("join consumer");

        // Verify file was uploaded
        let uploads = mock_backend.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, chunk_file);
        assert_eq!(uploads[0].1, "[2026-09-29_120000] Streamer - Title");

        // Verify local file was deleted upon completion
        assert!(!chunk_file.exists());

        // Verify events were dispatched
        let mut got_completed = false;
        while let Ok(event) = event_rx.try_recv() {
            if let AppEvent::UploadCompleted {
                channel_id,
                chunk_name,
                ..
            } = event
            {
                assert_eq!(channel_id, "test_channel");
                assert_eq!(chunk_name, "chunk_0000.ts");
                got_completed = true;
            }
        }
        assert!(got_completed);
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_upload_consumer_without_backend_discards_gracefully() {
        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_engine_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .expect("create tempdir");
        let chunk_file = temp_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_file, b"test").await.expect("write");

        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(100);
        let (upload_tx, upload_rx) = tokio::sync::mpsc::channel(10);

        let consumer_handle = EngineOrchestrator::spawn_upload_consumer(None, event_tx, upload_rx);

        let task = UploadTask {
            channel_id: "ch1".to_string(),
            session_folder_id: "folder".to_string(),
            remote_dir: "folder".to_string(),
            chunk_path: chunk_file.clone(),
            chunk_name: "chunk_0000.ts".to_string(),
            streamer_name: "Streamer".to_string(),
        };

        upload_tx.send(task).await.expect("send");
        drop(upload_tx);

        consumer_handle.await.expect("join");
        // Without backend, file is not deleted by consumer (remains local)
        assert!(chunk_file.exists());
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_process_sealed_chunk_with_backend() {
        let temp_dir =
            std::env::temp_dir().join(format!("chzzk_engine_test_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .expect("create tempdir");
        let chunk_file = temp_dir.join("chunk_0001.ts");
        tokio::fs::write(&chunk_file, b"12345678")
            .await
            .expect("write");

        let (upload_tx, mut upload_rx) = tokio::sync::mpsc::channel(10);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(10);

        EngineOrchestrator::process_sealed_chunk(
            &chunk_file,
            "remote_session_dir",
            "ch1",
            "Streamer",
            &upload_tx,
            &event_tx,
            true,
        )
        .await;

        let task = upload_rx.recv().await.expect("task received");
        assert_eq!(task.channel_id, "ch1");
        assert_eq!(task.remote_dir, "remote_session_dir");
        assert_eq!(task.session_folder_id, "remote_session_dir");
        assert_eq!(task.chunk_name, "chunk_0001.ts");

        let sealed_event = event_rx.recv().await.expect("event received");
        match sealed_event {
            AppEvent::ChunkSealed {
                chunk_name,
                size_bytes,
            } => {
                assert_eq!(chunk_name, "chunk_0001.ts");
                assert_eq!(size_bytes, 8);
            }
            other => panic!("Unexpected event: {other:?}"),
        }
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
