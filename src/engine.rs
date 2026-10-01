use crate::app_path::resolve_path;
use crate::chzzk::client::ChzzkClient;
use crate::chzzk::models::{LiveDetail, LiveStreamInfo};
use crate::config::Settings;
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::{UploadBackend, UploadTask, UploadWorker};
use chrono::Local;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

pub mod cleanup;
pub mod dispatcher;
pub mod reconciliation;
pub mod recording;
pub mod registry;
pub mod session;
pub mod state;

pub use cleanup::{
    cleanup_empty_session_dirs, cleanup_empty_session_dirs_excluding, cleanup_session_dir_if_empty,
};
pub use dispatcher::{process_sealed_chunk, seal_and_enqueue_chunks};
pub use reconciliation::{ReconciliationReport, reconcile_orphaned_sessions};
pub use recording::RecordingSession;
pub use registry::{
    ChannelLifecycleKind, ChannelLifecycleRegistry, ChannelLifecycleState, PollAction,
    RestrictionReason,
};
pub use session::{ActiveSessionState, FinishedSession};
pub use state::EngineState;

pub struct EngineOrchestrator {
    settings: Settings,
    chzzk: ChzzkClient,
    backend: Option<Arc<dyn UploadBackend>>,
    event_tx: Sender<AppEvent>,
    registry: ChannelLifecycleRegistry,
    state: EngineState,
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
            registry: ChannelLifecycleRegistry::new(),
            state: EngineState::new(),
            cancel_token,
            refresh_notify: Arc::new(tokio::sync::Notify::new()),
            session_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
            ffmpeg_bin: None,
        }
    }

    pub fn registry(&self) -> &ChannelLifecycleRegistry {
        &self.registry
    }

    pub fn is_recording(&self, channel_id: &str) -> bool {
        self.registry.is_recording(channel_id)
    }

    pub fn is_restricted(&self, channel_id: &str) -> bool {
        self.registry.is_restricted(channel_id)
    }

    pub fn active_session(&self, channel_id: &str) -> Option<ActiveSessionState> {
        self.registry.active_session(channel_id)
    }

    pub fn active_recording_ids(&self) -> Vec<String> {
        self.registry.active_recording_ids()
    }

    pub fn channel_state(&self, channel_id: &str) -> ChannelLifecycleState {
        self.registry.channel_state(channel_id)
    }

    pub fn active_sessions_snapshot(&self) -> HashMap<String, ActiveSessionState> {
        self.registry.active_sessions()
    }

    pub fn register_active_session(&self, channel_id: &str, session: ActiveSessionState) {
        let cancel_token = self.cancel_token.child_token();
        self.registry
            .start_recording(channel_id, session, cancel_token);
    }

    pub fn channel_names(&self) -> Arc<tokio::sync::RwLock<HashMap<String, String>>> {
        self.state.channel_names.clone()
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
        self.registry.cancel_all();
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn trigger_refresh(&self) {
        self.refresh_notify.notify_one();
    }

    #[deprecated(note = "use is_recording or active_recording_ids instead")]
    pub fn active_recordings(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.state.active_recordings.clone()
    }

    #[deprecated(note = "use active_session or active_sessions_snapshot instead")]
    pub fn active_sessions(&self) -> Arc<tokio::sync::Mutex<HashMap<String, ActiveSessionState>>> {
        self.state.active_sessions.clone()
    }

    #[deprecated(note = "use register_finished_session instead")]
    pub fn finished_sessions(&self) -> Arc<tokio::sync::Mutex<HashMap<String, FinishedSession>>> {
        self.state.finished_sessions.clone()
    }

    #[deprecated(note = "use is_restricted or channel_state instead")]
    pub fn restricted_channels(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.state.restricted_channels.clone()
    }

    #[deprecated(note = "use channel_state instead")]
    pub fn restricted_live_ids(&self) -> Arc<tokio::sync::Mutex<HashMap<String, u64>>> {
        self.state.restricted_live_ids.clone()
    }

    pub fn api_restricted_channels(&self) -> Arc<tokio::sync::Mutex<HashSet<String>>> {
        self.state.api_restricted_channels.clone()
    }

    pub fn session_cancel_tokens(
        &self,
    ) -> Arc<tokio::sync::Mutex<HashMap<String, CancellationToken>>> {
        self.state.session_cancel_tokens.clone()
    }

    pub async fn register_finished_session(&self, channel_id: &str, live_id: Option<u64>) {
        self.registry.finish_recording(channel_id, live_id);
        self.state.active_recordings.lock().await.remove(channel_id);
        self.state.active_sessions.lock().await.remove(channel_id);
        self.state
            .register_finished_session(channel_id, live_id)
            .await;
    }

    pub fn spawn_upload_consumer(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
    ) -> tokio::task::JoinHandle<()> {
        UploadWorker::spawn(backend_opt, event_tx, upload_rx)
    }

    pub fn spawn_upload_consumer_with_concurrency(
        backend_opt: Option<Arc<dyn UploadBackend>>,
        event_tx: Sender<AppEvent>,
        upload_rx: tokio::sync::mpsc::Receiver<UploadTask>,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<()> {
        UploadWorker::spawn_with_concurrency(backend_opt, event_tx, upload_rx, concurrency)
    }

    pub async fn cleanup_empty_session_dirs_excluding(
        recordings_dir: &Path,
        active_dirs: &HashSet<String>,
    ) -> std::io::Result<usize> {
        cleanup_empty_session_dirs_excluding(recordings_dir, active_dirs).await
    }

    pub async fn cleanup_empty_session_dirs(recordings_dir: &Path) -> std::io::Result<usize> {
        cleanup_empty_session_dirs(recordings_dir).await
    }

    pub async fn cleanup_session_dir_if_empty(session_dir: &Path) -> std::io::Result<bool> {
        cleanup_session_dir_if_empty(session_dir).await
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
        dispatcher::process_sealed_chunk(
            chunk_path,
            remote_dir,
            channel_id,
            streamer_name,
            upload_tx,
            event_tx,
            backend_active,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn seal_and_enqueue_chunks(
        watcher: &mut crate::recorder::watcher::SegmentWatcher,
        remote_dir: &str,
        channel_id: &str,
        streamer_name: &str,
        upload_tx: &Sender<UploadTask>,
        event_tx: &Sender<AppEvent>,
        backend_active: bool,
        is_finished: bool,
    ) {
        dispatcher::seal_and_enqueue_chunks(
            watcher,
            remote_dir,
            channel_id,
            streamer_name,
            upload_tx,
            event_tx,
            backend_active,
            is_finished,
        )
        .await;
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

        if !self.registry.is_recording(&channel_id) {
            let alias = self
                .settings
                .channels
                .iter()
                .find(|c| c.id == channel_id)
                .and_then(|c| c.alias.clone());
            let start_timestamp = Local::now().format("%Y-%m-%d_%H%M%S").to_string();
            let session_state = ActiveSessionState::new(
                start_timestamp,
                info.streamer_name.clone(),
                alias,
                info.metadata.clone(),
            );
            let session_cancel = self.cancel_token.child_token();
            self.registry
                .start_recording(&channel_id, session_state, session_cancel);
        }

        let handle = RecordingSession::spawn(
            channel_id,
            info,
            upload_tx,
            self.settings.clone(),
            self.backend.clone(),
            self.chzzk.clone(),
            self.event_tx.clone(),
            self.registry.clone(),
            self.state.clone(),
            self.cancel_token.clone(),
            self.ffmpeg_bin.clone(),
        );

        if let Ok(mut guard) = self.session_handles.lock() {
            guard.retain(|h| !h.is_finished());
            guard.push(handle);
        }
    }

    pub async fn poll_channels_once(&self, upload_tx: &Sender<UploadTask>) {
        let cooldown_window = Duration::from_secs(self.settings.general.stream_cooldown_seconds);
        for channel in &self.settings.channels {
            let detail_res = tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                res = self.chzzk.get_live_detail(&channel.id) => res,
            };
            match detail_res {
                Ok(detail) => {
                    // Sync cached streamer name with legacy state
                    let incoming_streamer_name = match &detail {
                        LiveDetail::Open(info) => Some(&info.streamer_name),
                        LiveDetail::Restricted { streamer_name, .. } => Some(streamer_name),
                        LiveDetail::Close { streamer_name } => streamer_name.as_ref(),
                    };
                    if let Some(streamer_name) = incoming_streamer_name {
                        let mut names = self.state.channel_names.write().await;
                        names.insert(channel.id.clone(), streamer_name.clone());
                    }

                    // Bridge legacy state mutations (from existing integration tests) into registry
                    if !self.registry.is_recording(&channel.id) {
                        let legacy_session = {
                            let sessions = self.state.active_sessions.lock().await;
                            sessions.get(&channel.id).cloned()
                        };
                        if let Some(session) = legacy_session {
                            let token = self.cancel_token.child_token();
                            self.registry.start_recording(&channel.id, session, token);
                        }
                    }
                    if !self.registry.is_restricted(&channel.id) {
                        let is_restricted = self
                            .state
                            .restricted_channels
                            .lock()
                            .await
                            .contains(&channel.id);
                        if is_restricted {
                            let live_id = self
                                .state
                                .restricted_live_ids
                                .lock()
                                .await
                                .get(&channel.id)
                                .copied();
                            let reason = if self
                                .state
                                .api_restricted_channels
                                .lock()
                                .await
                                .contains(&channel.id)
                            {
                                RestrictionReason::RequiresCredentials
                            } else {
                                RestrictionReason::KeyForbidden
                            };
                            self.registry.mark_restricted(&channel.id, live_id, reason);
                        }
                    }

                    let action = self.registry.evaluate_poll(
                        &channel.id,
                        channel.alias.as_deref(),
                        &detail,
                        cooldown_window,
                    );

                    match action {
                        PollAction::ReadyToRecord {
                            info,
                            display_name,
                            was_api_restricted,
                        } => {
                            let min_disk = self.settings.general.min_free_disk_gb;
                            let recordings_base =
                                resolve_path(Path::new(&self.settings.general.recordings_dir));
                            if !crate::disk::has_sufficient_disk_space(&recordings_base, min_disk) {
                                if let Ok(space) = crate::disk::get_disk_space(&recordings_base) {
                                    let _ = self
                                        .event_tx
                                        .send(AppEvent::Log(LogEntry::warn(format!(
                                            "[DISK] Disk space critically low ({:.2} GB < {:.2} GB). Recording paused to prevent disk exhaustion.",
                                            space.available_gb(), min_disk
                                        ))))
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

                            if was_api_restricted {
                                let log_msg = format!(
                                    "Restricted stream for channel {} ({}) returned to public broadcast (liveId: {:?}). Starting new recording session in new broadcast folder...",
                                    channel.id, display_name, info.live_id
                                );
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::Log(LogEntry::rec(log_msg)))
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

                            let start_timestamp = if was_api_restricted {
                                Local::now().format("%Y-%m-%d_%H%M%S").to_string()
                            } else {
                                Local::now().format("%Y-%m-%d_%H%M").to_string()
                            };

                            let session_cancel = self.cancel_token.child_token();
                            let session_state = ActiveSessionState::new(
                                start_timestamp,
                                info.streamer_name.clone(),
                                channel.alias.clone(),
                                info.metadata.clone(),
                            );

                            self.registry.start_recording(
                                &channel.id,
                                session_state.clone(),
                                session_cancel.clone(),
                            );

                            // Sync legacy state for backward compatibility during phased migration
                            {
                                let mut active = self.state.active_recordings.lock().await;
                                active.insert(channel.id.clone());
                            }
                            {
                                let mut sessions = self.state.active_sessions.lock().await;
                                sessions.insert(channel.id.clone(), session_state);
                            }
                            {
                                let mut tokens = self.state.session_cancel_tokens.lock().await;
                                tokens.insert(channel.id.clone(), session_cancel);
                            }
                            {
                                let mut restricted = self.state.restricted_channels.lock().await;
                                restricted.remove(&channel.id);
                            }
                            {
                                let mut r_ids = self.state.restricted_live_ids.lock().await;
                                r_ids.remove(&channel.id);
                            }
                            {
                                let mut api_restricted =
                                    self.state.api_restricted_channels.lock().await;
                                api_restricted.remove(&channel.id);
                            }
                            {
                                let mut finished = self.state.finished_sessions.lock().await;
                                finished.remove(&channel.id);
                            }

                            self.spawn_recording_session(
                                channel.id.clone(),
                                info,
                                upload_tx.clone(),
                            );
                        }
                        PollAction::RecordingMetadataChanged {
                            delta,
                            event,
                            remote_dir,
                            full_jsonl,
                            display_name,
                            title,
                        } => {
                            // Sync legacy state
                            {
                                let mut sessions = self.state.active_sessions.lock().await;
                                if let Some(session) = sessions.get_mut(&channel.id) {
                                    session.record_metadata_change(event.state.clone());
                                }
                            }

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
                                        title: title.clone(),
                                    })
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
                        PollAction::AlreadyRecording {
                            display_name,
                            title,
                        } => {
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
                        PollAction::InCooldown {
                            display_name,
                            live_id,
                            ..
                        } => {
                            let _ = self
                                .event_tx
                                .send(AppEvent::Log(LogEntry::poll(format!(
                                    "Channel {} ({}) stream recently concluded (liveId: {:?}). Waiting for API cache to close...",
                                    channel.id, display_name, live_id
                                ))))
                                .await;

                            let _ = self
                                .event_tx
                                .send(AppEvent::ChannelUpdate {
                                    channel_id: channel.id.clone(),
                                    channel_name: display_name,
                                    is_live: false,
                                    title: "Stream Concluded (Cooldown)".to_string(),
                                })
                                .await;
                        }
                        PollAction::Restricted {
                            display_name,
                            title,
                            reason,
                            is_newly_restricted,
                        } => {
                            // Sync legacy state
                            if let Some(token) = self
                                .state
                                .session_cancel_tokens
                                .lock()
                                .await
                                .remove(&channel.id)
                            {
                                token.cancel();
                            }
                            let was_active = {
                                let mut active = self.state.active_recordings.lock().await;
                                active.remove(&channel.id)
                            };
                            if was_active {
                                let mut sessions = self.state.active_sessions.lock().await;
                                sessions.remove(&channel.id);
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::RecordingEnded {
                                        channel_id: channel.id.clone(),
                                    })
                                    .await;
                            }
                            {
                                let mut restricted = self.state.restricted_channels.lock().await;
                                restricted.insert(channel.id.clone());
                            }
                            if matches!(
                                reason,
                                RestrictionReason::AgeRestricted
                                    | RestrictionReason::RequiresCredentials
                            ) {
                                let mut api_restricted =
                                    self.state.api_restricted_channels.lock().await;
                                api_restricted.insert(channel.id.clone());
                            }
                            if let LiveDetail::Restricted {
                                live_id: Some(lid), ..
                            }
                            | LiveDetail::Open(LiveStreamInfo {
                                live_id: Some(lid), ..
                            }) = &detail
                            {
                                let mut r_ids = self.state.restricted_live_ids.lock().await;
                                r_ids.insert(channel.id.clone(), *lid);
                            }

                            if is_newly_restricted {
                                let log_msg = reason.display_message(&channel.id, &display_name);
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
                        PollAction::StreamClosed {
                            display_name,
                            was_recording,
                        } => {
                            if let Some(token) = self
                                .state
                                .session_cancel_tokens
                                .lock()
                                .await
                                .remove(&channel.id)
                            {
                                token.cancel();
                            }
                            {
                                let mut api_restricted =
                                    self.state.api_restricted_channels.lock().await;
                                api_restricted.remove(&channel.id);
                            }
                            {
                                let mut restricted = self.state.restricted_channels.lock().await;
                                restricted.remove(&channel.id);
                            }
                            {
                                let mut r_ids = self.state.restricted_live_ids.lock().await;
                                r_ids.remove(&channel.id);
                            }
                            {
                                let mut finished = self.state.finished_sessions.lock().await;
                                finished.remove(&channel.id);
                            }
                            {
                                let mut sessions = self.state.active_sessions.lock().await;
                                sessions.remove(&channel.id);
                            }
                            {
                                let mut active = self.state.active_recordings.lock().await;
                                active.remove(&channel.id);
                            }

                            if was_recording {
                                let _ = self
                                    .event_tx
                                    .send(AppEvent::RecordingEnded {
                                        channel_id: channel.id.clone(),
                                    })
                                    .await;
                            }

                            let recordings_base =
                                resolve_path(Path::new(&self.settings.general.recordings_dir));
                            let active_dirs = {
                                let sessions = self.registry.active_sessions();
                                sessions.values().map(|s| s.folder_name()).collect()
                            };
                            if let Ok(count) = Self::cleanup_empty_session_dirs_excluding(
                                &recordings_base,
                                &active_dirs,
                            )
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
                    }
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

        // Startup Crash Reconciliation: If cloud backend is active, reconcile orphaned chunks from prior runs
        if self.backend.is_some() {
            let recordings_base = resolve_path(Path::new(&self.settings.general.recordings_dir));
            let report = reconcile_orphaned_sessions(
                &recordings_base,
                &upload_tx,
                &self.event_tx,
                &self.settings,
            )
            .await;
            if report.chunks_enqueued > 0 || report.chunks_quarantined > 0 {
                let _ = self
                    .event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "[RECONCILIATION] Reconciled {} orphaned session(s): {} chunk(s) enqueued for upload, {} partial tail chunk(s) quarantined.",
                        report.orphaned_sessions_scanned,
                        report.chunks_enqueued,
                        report.chunks_quarantined
                    ))))
                    .await;
            }
        }

        loop {
            if self.cancel_token.is_cancelled() {
                self.registry.cancel_all();
                break;
            }

            self.poll_channels_once(&upload_tx).await;

            let recordings_base = resolve_path(Path::new(&self.settings.general.recordings_dir));
            let active_dirs: HashSet<String> = self
                .registry
                .active_sessions()
                .values()
                .map(|s| s.folder_name())
                .collect();

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
                    self.registry.cancel_all();
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
pub mod tests {
    use super::*;
    pub use session::tests::*;

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
