use chrono::Local;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use crate::app_path::resolve_path;
use crate::chzzk::chat::ChzzkChatClient;
use crate::chzzk::models::LiveStreamInfo;
use crate::chzzk::source::LiveStreamSource;
use crate::config::Settings;
use crate::engine::SessionCustodian;
use crate::engine::registry::{ChannelLifecycleRegistry, RestrictionReason};
use crate::engine::session::ActiveSessionState;
use crate::recorder::ffmpeg::{FfmpegEvent, FfmpegExit, FfmpegSession};
use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::{UploadBackend, UploadTask, broadcast_identifier};

/// Parameters required to spawn a `RecordingSession`.
#[derive(Clone)]
pub struct RecordingSessionParams {
    pub channel_id: String,
    pub info: LiveStreamInfo,
    pub upload_tx: Sender<UploadTask>,
    pub settings: Settings,
    pub backend: Option<Arc<dyn UploadBackend>>,
    pub chzzk: Arc<dyn LiveStreamSource>,
    pub event_tx: Sender<AppEvent>,
    pub registry: ChannelLifecycleRegistry,
    pub cancel_token: CancellationToken,
    pub ffmpeg_bin: Option<String>,
    pub custodian: Arc<SessionCustodian>,
}

/// Internal helper capturing ambient session context to dispatch sealed chunks
/// to the upload queue and emit telemetry events.
#[derive(Clone)]
pub(crate) struct SessionChunkDispatcher {
    pub(crate) session_folder: String,
    pub(crate) channel_id: String,
    pub(crate) streamer_name: String,
    pub(crate) upload_tx: Sender<UploadTask>,
    pub(crate) event_tx: Sender<AppEvent>,
    pub(crate) backend_active: bool,
}

impl SessionChunkDispatcher {
    pub(crate) fn new(
        session_folder: String,
        channel_id: String,
        streamer_name: String,
        upload_tx: Sender<UploadTask>,
        event_tx: Sender<AppEvent>,
        backend_active: bool,
    ) -> Self {
        Self {
            session_folder,
            channel_id,
            streamer_name,
            upload_tx,
            event_tx,
            backend_active,
        }
    }

    /// Processes a single sealed chunk: emits `ChunkSealed` event, and forwards it to the upload queue
    /// or logs that it has been saved locally.
    pub(crate) async fn process_sealed_chunk(&self, chunk_path: &Path) {
        if let Some(chunk_name) = chunk_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
        {
            let size = tokio::fs::metadata(chunk_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            let _ = self
                .event_tx
                .send(AppEvent::ChunkSealed {
                    chunk_name: chunk_name.to_string(),
                    size_bytes: size,
                })
                .await;

            let target = broadcast_identifier(&self.streamer_name, &self.channel_id);

            if self.backend_active {
                let send_res = self
                    .upload_tx
                    .send(UploadTask {
                        channel_id: self.channel_id.clone(),
                        session_folder_id: self.session_folder.clone(),
                        remote_dir: self.session_folder.clone(),
                        chunk_path: chunk_path.to_path_buf(),
                        chunk_name: chunk_name.to_string(),
                        streamer_name: self.streamer_name.clone(),
                    })
                    .await;

                if send_res.is_ok() {
                    let _ = self
                        .event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "[{target}] {chunk_name} sealed. Pushed to cloud upload queue."
                        ))))
                        .await;
                } else {
                    let _ = self
                        .event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "[{target}] {chunk_name} sealed (saved locally)."
                        ))))
                        .await;
                }
            } else {
                let _ = self
                    .event_tx
                    .send(AppEvent::Log(LogEntry::rec(format!(
                        "[{target}] {chunk_name} sealed (saved locally)."
                    ))))
                    .await;
            }
        }
    }

    /// Detects newly sealed chunks using `SegmentWatcher` and dispatches each to the upload queue.
    pub(crate) async fn seal_and_enqueue(&self, watcher: &mut SegmentWatcher, is_finished: bool) {
        let sealed_chunks = watcher.detect_sealed(is_finished);
        for chunk_path in sealed_chunks {
            self.process_sealed_chunk(&chunk_path).await;
        }
    }
}

/// Executes the complete lifecycle of a single recording session for a live channel.
pub struct RecordingSession {
    params: RecordingSessionParams,
}

impl RecordingSession {
    pub fn new(params: RecordingSessionParams) -> Self {
        Self { params }
    }

    pub fn spawn(params: RecordingSessionParams) -> tokio::task::JoinHandle<()> {
        Self::new(params).run()
    }

    pub fn run(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let RecordingSessionParams {
                channel_id,
                info,
                upload_tx,
                settings,
                backend: backend_opt,
                chzzk,
                event_tx,
                registry,
                cancel_token,
                ffmpeg_bin,
                custodian,
            } = self.params;

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

            let (session_folder_name, initial_metadata_jsonl, session_cancel) = {
                let existing_session = registry.active_session(&channel_id);
                let (session, token) = match existing_session {
                    Some(s) => {
                        let token = registry
                            .get_cancel_token(&channel_id)
                            .unwrap_or_else(|| cancel_token.child_token());
                        (s, token)
                    }
                    None => {
                        let start_timestamp = Local::now().format("%Y-%m-%d_%H%M%S").to_string();
                        let s = ActiveSessionState::new(
                            start_timestamp,
                            info.streamer_name.clone(),
                            alias.clone(),
                            info.metadata.clone(),
                        );
                        let token = cancel_token.child_token();
                        registry.start_recording(&channel_id, s.clone(), token.clone());
                        (s, token)
                    }
                };

                (
                    session.folder_name(),
                    session.format_metadata_jsonl(),
                    token,
                )
            };

            let dispatcher = SessionChunkDispatcher::new(
                session_folder_name.clone(),
                channel_id.clone(),
                info.streamer_name.clone(),
                upload_tx.clone(),
                event_tx.clone(),
                backend_opt.is_some(),
            );

            let recordings_base = resolve_path(Path::new(&settings.general.recordings_dir));
            let session_dir = recordings_base.join(&session_folder_name);
            custodian.register_active(&session_dir, &channel_id, &info.streamer_name);

            if let Err(e) = tokio::fs::create_dir_all(&session_dir).await {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::error(format!(
                        "Failed to create session directory {}: {}",
                        session_dir.display(),
                        e
                    ))))
                    .await;
                registry.reset_to_idle(&channel_id);
                custodian.mark_concluded(&session_dir);
                let _ = custodian.try_purge(&session_dir).await;
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

            let (chat_sealed_tx, mut chat_sealed_rx) = tokio::sync::mpsc::channel::<PathBuf>(32);
            let chat_forward_handle = {
                let dispatcher = dispatcher.clone();

                tokio::spawn(async move {
                    while let Some(chat_path) = chat_sealed_rx.recv().await {
                        dispatcher.process_sealed_chunk(&chat_path).await;
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
            let env_ffmpeg_bin = std::env::var("CHZZK_LOAD_FFMPEG_BIN").ok();
            let effective_ffmpeg_bin = ffmpeg_bin.as_deref().or(env_ffmpeg_bin.as_deref());
            let mut ffmpeg_session = match FfmpegSession::spawn(
                &info.hls_url,
                &output_pattern,
                chunk_dur,
                cookie,
                effective_ffmpeg_bin,
            ) {
                Ok(session) => {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "Spawned FFmpeg segmenter ({}s TS chunks) -> {}",
                            chunk_dur,
                            session_dir.display()
                        ))))
                        .await;
                    session
                }
                Err(e) => {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::error(format!(
                            "Failed to spawn FFmpeg: {e}"
                        ))))
                        .await;
                    registry.reset_to_idle(&channel_id);
                    session_cancel.cancel();
                    if let Some(mut chat_handle) = chat_task
                        && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                            .await
                            .is_err()
                    {
                        chat_handle.abort();
                    }
                    custodian.mark_concluded(&session_dir);
                    let _ = custodian.try_purge(&session_dir).await;
                    let _ = event_tx
                        .send(AppEvent::RecordingEnded {
                            channel_id: channel_id.clone(),
                        })
                        .await;
                    return;
                }
            };

            let mut watcher = SegmentWatcher::new(session_dir.clone());
            let mut restricted_abort = false;

            loop {
                tokio::select! {
                    event = ffmpeg_session.recv_event() => {
                        match event {
                            Some(FfmpegEvent::KeyForbidden) => {
                                restricted_abort = true;
                                break;
                            }
                            Some(FfmpegEvent::Log(line)) => {
                                let _ = event_tx.try_send(AppEvent::Log(LogEntry::ffmpeg(line)));
                            }
                            Some(FfmpegEvent::Exited(_)) | None => {
                                break;
                            }
                        }
                    }
                    _ = session_cancel.cancelled() => {
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::rec(format!(
                                "Cancellation received for channel {channel_id}, stopping FFmpeg gracefully..."
                            ))))
                            .await;
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let min_disk = settings.general.min_free_disk_gb;
                        if min_disk > 0.0 {
                            if let Ok(space) = crate::disk::get_disk_space(&session_dir) {
                                let avail = space.available_gb();
                                if avail < min_disk {
                                    let _ = event_tx
                                        .send(AppEvent::Log(LogEntry::warn(format!(
                                            "[DISK] Disk space critically low ({:.2} GB < {:.2} GB). Recording paused to prevent disk exhaustion.",
                                            avail, min_disk
                                        ))))
                                        .await;
                                    break;
                                }
                            }
                        }

                        dispatcher.seal_and_enqueue(&mut watcher, false).await;
                    }
                }
            }

            if restricted_abort {
                let _ = ffmpeg_session.kill().await;

                registry.mark_restricted(
                    &channel_id,
                    info.live_id,
                    RestrictionReason::KeyForbidden,
                );

                session_cancel.cancel();
                if let Some(mut chat_handle) = chat_task
                    && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                        .await
                        .is_err()
                {
                    chat_handle.abort();
                }

                // Clean up any partial media files, then let custodian perform gated purge
                if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                    while let Ok(Some(entry)) = rd.next_entry().await {
                        if entry.file_name() != "metadata.jsonl" {
                            let _ = tokio::fs::remove_file(entry.path()).await;
                        }
                    }
                }
                custodian.mark_concluded(&session_dir);
                let _ = custodian.try_purge(&session_dir).await;

                let log_msg = format!(
                    "Recording unavailable for channel {} ({}): restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                    channel_id, info.streamer_name
                );
                let _ = event_tx.send(AppEvent::Log(LogEntry::error(log_msg))).await;

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

            match ffmpeg_session.stop_graceful(Duration::from_secs(3)).await {
                Ok(FfmpegExit::Clean(status)) => {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(format!(
                            "FFmpeg process exited cleanly: {status}"
                        ))))
                        .await;
                }
                Ok(FfmpegExit::Killed(_)) | Err(_) => {
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::rec(
                            "FFmpeg did not exit within timeout, terminating process...",
                        )))
                        .await;
                }
            }

            // Collect lingering chunks on stream conclusion, cancellation, or low disk space
            dispatcher.seal_and_enqueue(&mut watcher, true).await;

            session_cancel.cancel();
            let metadata_jsonl = registry
                .active_session(&channel_id)
                .map(|s| s.format_metadata_jsonl())
                .unwrap_or_default();

            let upload_metadata_fut = async {
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
            };

            let teardown_chat_fut = async {
                if let Some(mut chat_handle) = chat_task
                    && tokio::time::timeout(Duration::from_secs(5), &mut chat_handle)
                        .await
                        .is_err()
                {
                    chat_handle.abort();
                }
                let _ = tokio::time::timeout(Duration::from_secs(2), chat_forward_handle).await;
            };

            tokio::join!(upload_metadata_fut, teardown_chat_fut);

            registry.finish_recording(&channel_id, info.live_id);

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

            custodian.mark_concluded(&session_dir);
            let _ = custodian.try_purge(&session_dir).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_session_chunk_dispatcher_no_backend_saves_locally() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_scd_no_backend_{}", rand::random::<u32>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let chunk_path = temp_dir.join("chunk_0000.ts");
        fs::write(&chunk_path, b"dummy video bytes").unwrap();

        let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

        let dispatcher = SessionChunkDispatcher::new(
            "Streamer - Title".to_string(),
            "chan_local".to_string(),
            "Streamer".to_string(),
            upload_tx,
            event_tx,
            false,
        );

        dispatcher.process_sealed_chunk(&chunk_path).await;

        assert!(upload_rx.try_recv().is_err());

        let mut got_chunk_sealed = false;
        let mut got_saved_locally_log = false;

        while let Ok(ev) = event_rx.try_recv() {
            match ev {
                AppEvent::ChunkSealed {
                    chunk_name,
                    size_bytes,
                } => {
                    assert_eq!(chunk_name, "chunk_0000.ts");
                    assert_eq!(size_bytes, 17);
                    got_chunk_sealed = true;
                }
                AppEvent::Log(msg)
                    if msg == "[REC] [Streamer] chunk_0000.ts sealed (saved locally)." =>
                {
                    got_saved_locally_log = true;
                }
                _ => {}
            }
        }

        assert!(got_chunk_sealed);
        assert!(got_saved_locally_log);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_session_chunk_dispatcher_backend_active_pushes_task() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_scd_active_{}", rand::random::<u32>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let chunk_path = temp_dir.join("chunk_0001.ts");
        fs::write(&chunk_path, b"test chunk content").unwrap();

        let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

        let dispatcher = SessionChunkDispatcher::new(
            "Subfolder".to_string(),
            "chan_sub".to_string(),
            "StreamerSub".to_string(),
            upload_tx,
            event_tx,
            true,
        );

        dispatcher.process_sealed_chunk(&chunk_path).await;

        let task = upload_rx.recv().await.expect("Expected UploadTask");
        assert_eq!(task.remote_dir, "Subfolder");
        assert_eq!(task.session_folder_id, "Subfolder");
        assert_eq!(task.chunk_name, "chunk_0001.ts");
        assert_eq!(task.channel_id, "chan_sub");
        assert_eq!(task.streamer_name, "StreamerSub");

        let mut got_pushed_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::Log(msg) = ev
                && msg == "[REC] [StreamerSub] chunk_0001.ts sealed. Pushed to cloud upload queue."
            {
                got_pushed_log = true;
            }
        }

        assert!(got_pushed_log);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_session_chunk_dispatcher_upload_channel_closed_saves_locally() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_scd_closed_{}", rand::random::<u32>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let chunk_path = temp_dir.join("chunk_0002.ts");
        fs::write(&chunk_path, b"test video data").unwrap();

        let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
        let (upload_tx, upload_rx) = mpsc::channel::<UploadTask>(10);
        drop(upload_rx);

        let dispatcher = SessionChunkDispatcher::new(
            "Subfolder".to_string(),
            "chan_fail_test".to_string(),
            "StreamerFail".to_string(),
            upload_tx,
            event_tx,
            true,
        );

        dispatcher.process_sealed_chunk(&chunk_path).await;

        let mut got_saved_locally_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::Log(msg) = ev
                && msg == "[REC] [StreamerFail] chunk_0002.ts sealed (saved locally)."
            {
                got_saved_locally_log = true;
            }
        }

        assert!(got_saved_locally_log);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_session_chunk_dispatcher_existing_folder_id_skips_retry() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_scd_existing_{}", rand::random::<u32>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let chunk_path = temp_dir.join("chunk_0003.ts");
        fs::write(&chunk_path, b"another chunk data").unwrap();

        let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(20);
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

        let dispatcher = SessionChunkDispatcher::new(
            "Subfolder".to_string(),
            "chan_exist".to_string(),
            "StreamerExist".to_string(),
            upload_tx,
            event_tx,
            true,
        );

        dispatcher.process_sealed_chunk(&chunk_path).await;

        let task = upload_rx.recv().await.expect("Expected UploadTask");
        assert_eq!(task.remote_dir, "Subfolder");
        assert_eq!(task.session_folder_id, "Subfolder");
        assert_eq!(task.chunk_name, "chunk_0003.ts");

        let mut got_pushed_log = false;
        while let Ok(ev) = event_rx.try_recv() {
            if let AppEvent::Log(msg) = ev
                && msg
                    == "[REC] [StreamerExist] chunk_0003.ts sealed. Pushed to cloud upload queue."
            {
                got_pushed_log = true;
            }
        }
        assert!(got_pushed_log);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_session_chunk_dispatcher_seal_and_enqueue_with_watcher() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_scd_watcher_{}", rand::random::<u32>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let chunk0 = temp_dir.join("chunk_0000.ts");
        let chunk1 = temp_dir.join("chunk_0001.ts");
        fs::write(&chunk0, b"first chunk").unwrap();
        fs::write(&chunk1, b"second chunk").unwrap();

        let (event_tx, _event_rx) = mpsc::channel::<AppEvent>(20);
        let (upload_tx, mut upload_rx) = mpsc::channel::<UploadTask>(10);

        let dispatcher = SessionChunkDispatcher::new(
            "SessionWatcherFolder".to_string(),
            "chan_watcher".to_string(),
            "StreamerWatcher".to_string(),
            upload_tx,
            event_tx,
            true,
        );

        let mut watcher = SegmentWatcher::new(temp_dir.clone());

        // When not finished, chunk_0000.ts is sealed because chunk_0001.ts exists
        dispatcher.seal_and_enqueue(&mut watcher, false).await;
        let task0 = upload_rx.recv().await.expect("Expected chunk_0000 task");
        assert_eq!(task0.chunk_name, "chunk_0000.ts");
        assert!(upload_rx.try_recv().is_err());

        // When finished, remaining chunk_0001.ts is sealed
        dispatcher.seal_and_enqueue(&mut watcher, true).await;
        let task1 = upload_rx.recv().await.expect("Expected chunk_0001 task");
        assert_eq!(task1.chunk_name, "chunk_0001.ts");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_recording_session_params_construction() {
        use crate::chzzk::models_metadata::StreamMetadataState;
        use crate::chzzk::source::MockLiveStreamSource;

        let (upload_tx, _upload_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let registry = ChannelLifecycleRegistry::new();
        let cancel_token = CancellationToken::new();
        let settings = Settings::default();
        let chzzk = Arc::new(MockLiveStreamSource::new());

        let info = LiveStreamInfo {
            channel_id: "test_chan".to_string(),
            live_id: Some(12345),
            streamer_name: "TestStreamer".to_string(),
            title: "TestTitle".to_string(),
            hls_url: "https://example.com/live.m3u8".to_string(),
            chat_channel_id: Some("chat_123".to_string()),
            metadata: StreamMetadataState::default(),
        };

        let params = RecordingSessionParams {
            channel_id: "test_chan".to_string(),
            info: info.clone(),
            upload_tx,
            settings,
            backend: None,
            chzzk,
            event_tx,
            registry,
            cancel_token,
            ffmpeg_bin: Some("custom-ffmpeg".to_string()),
            custodian: Arc::new(SessionCustodian::without_events()),
        };

        assert_eq!(params.channel_id, "test_chan");
        assert_eq!(params.info.live_id, Some(12345));
        assert_eq!(params.ffmpeg_bin.as_deref(), Some("custom-ffmpeg"));
        assert!(params.backend.is_none());
    }

    #[tokio::test]
    async fn test_recording_session_spawn_with_params() {
        use crate::chzzk::models_metadata::StreamMetadataState;
        use crate::chzzk::source::MockLiveStreamSource;

        let temp_dir =
            std::env::temp_dir().join(format!("test_rs_spawn_{}", rand::random::<u32>()));
        let _ = fs::create_dir_all(&temp_dir);

        let (upload_tx, _upload_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(20);
        let registry = ChannelLifecycleRegistry::new();
        let cancel_token = CancellationToken::new();
        let mut settings = Settings::default();
        settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
        let chzzk = Arc::new(MockLiveStreamSource::new());

        let info = LiveStreamInfo {
            channel_id: "test_chan_spawn".to_string(),
            live_id: Some(54321),
            streamer_name: "SpawnStreamer".to_string(),
            title: "SpawnTitle".to_string(),
            hls_url: "http://127.0.0.1:0/dummy.m3u8".to_string(),
            chat_channel_id: None,
            metadata: StreamMetadataState::default(),
        };

        let params = RecordingSessionParams {
            channel_id: "test_chan_spawn".to_string(),
            info,
            upload_tx,
            settings,
            backend: None,
            chzzk,
            event_tx,
            registry: registry.clone(),
            cancel_token: cancel_token.clone(),
            ffmpeg_bin: Some("nonexistent_ffmpeg_bin_for_test".to_string()),
            custodian: Arc::new(SessionCustodian::without_events()),
        };

        let session_state = crate::engine::session::ActiveSessionState::new(
            "2026-10-04_120000".to_string(),
            "SpawnStreamer".to_string(),
            None,
            StreamMetadataState::default(),
        );
        registry.start_recording("test_chan_spawn", session_state, cancel_token.child_token());
        assert!(registry.is_recording("test_chan_spawn"));

        let handle = RecordingSession::spawn(params);
        let _ = handle.await;

        assert_eq!(
            registry.channel_state("test_chan_spawn").kind(),
            crate::engine::registry::ChannelLifecycleKind::Idle
        );
        assert!(!registry.is_recording("test_chan_spawn"));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_recording_session_receives_custodian_and_registers_active_then_purges() {
        use crate::chzzk::models_metadata::StreamMetadataState;
        use crate::chzzk::source::MockLiveStreamSource;

        let temp_dir =
            std::env::temp_dir().join(format!("test_rs_custodian_{}", rand::random::<u32>()));
        let _ = fs::create_dir_all(&temp_dir);

        let (upload_tx, _upload_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(20);
        let registry = ChannelLifecycleRegistry::new();
        let cancel_token = CancellationToken::new();
        let mut settings = Settings::default();
        settings.general.recordings_dir = temp_dir.to_str().unwrap().to_string();
        let chzzk = Arc::new(MockLiveStreamSource::new());
        let custodian = Arc::new(SessionCustodian::new(event_tx.clone()));

        let info = LiveStreamInfo {
            channel_id: "test_chan_custodian".to_string(),
            live_id: Some(99999),
            streamer_name: "CustodianStreamer".to_string(),
            title: "CustodianTitle".to_string(),
            hls_url: "http://127.0.0.1:0/dummy.m3u8".to_string(),
            chat_channel_id: None,
            metadata: StreamMetadataState::default(),
        };

        let params = RecordingSessionParams {
            channel_id: "test_chan_custodian".to_string(),
            info,
            upload_tx,
            settings,
            backend: None,
            chzzk,
            event_tx,
            registry: registry.clone(),
            cancel_token: cancel_token.clone(),
            ffmpeg_bin: Some("nonexistent_ffmpeg_bin_for_test".to_string()),
            custodian: custodian.clone(),
        };

        let session = RecordingSession::new(params);
        assert_eq!(
            Arc::as_ptr(&session.params.custodian),
            Arc::as_ptr(&custodian)
        );

        let session_state = crate::engine::session::ActiveSessionState::new(
            "2026-10-04_120000".to_string(),
            "CustodianStreamer".to_string(),
            None,
            StreamMetadataState::default(),
        );
        let session_dir = temp_dir.join(session_state.folder_name());
        registry.start_recording(
            "test_chan_custodian",
            session_state,
            cancel_token.child_token(),
        );

        let handle = session.run();
        let _ = handle.await;

        // Directory should be concluded and purged (empty because FFmpeg failed to spawn)
        assert!(custodian.active_paths().is_empty());
        assert!(custodian.tracked_state(&session_dir).is_none());
        assert!(!session_dir.exists());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
