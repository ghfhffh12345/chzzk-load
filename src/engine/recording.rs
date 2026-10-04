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
use crate::engine::dispatcher::{process_sealed_chunk, seal_and_enqueue_chunks};
use crate::engine::registry::{ChannelLifecycleRegistry, RestrictionReason};
use crate::engine::session::ActiveSessionState;
use crate::recorder::ffmpeg::{FfmpegEvent, FfmpegExit, FfmpegSession};
use crate::recorder::watcher::SegmentWatcher;
use crate::tui::event::{AppEvent, LogEntry};
use crate::uploader::{UploadBackend, UploadTask, broadcast_identifier};

/// Executes the complete lifecycle of a single recording session for a live channel.
pub struct RecordingSession;

impl RecordingSession {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        channel_id: String,
        info: LiveStreamInfo,
        upload_tx: Sender<UploadTask>,
        settings: Settings,
        backend_opt: Option<Arc<dyn UploadBackend>>,
        chzzk: Arc<dyn LiveStreamSource>,
        event_tx: Sender<AppEvent>,
        registry: ChannelLifecycleRegistry,
        cancel_token: CancellationToken,
        ffmpeg_bin: Option<String>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
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
                registry.reset_to_idle(&channel_id);
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
                let upload_tx = upload_tx.clone();
                let event_tx = event_tx.clone();
                let channel_id = channel_id.clone();
                let session_folder = session_folder_name.clone();
                let streamer = info.streamer_name.clone();
                let backend_active = backend_opt.is_some();

                tokio::spawn(async move {
                    while let Some(chat_path) = chat_sealed_rx.recv().await {
                        process_sealed_chunk(
                            &chat_path,
                            &session_folder,
                            &channel_id,
                            &streamer,
                            &upload_tx,
                            &event_tx,
                            backend_active,
                        )
                        .await;
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
                    if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                        while let Ok(Some(entry)) = rd.next_entry().await {
                            let _ = tokio::fs::remove_file(entry.path()).await;
                        }
                    }
                    let _ = tokio::fs::remove_dir(&session_dir).await;
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

                        seal_and_enqueue_chunks(
                            &mut watcher,
                            &session_folder_name,
                            &channel_id,
                            &info.streamer_name,
                            &upload_tx,
                            &event_tx,
                            backend_opt.is_some(),
                            false,
                        )
                        .await;
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

                // Clean up session directory and any empty/partial files
                if let Ok(mut rd) = tokio::fs::read_dir(&session_dir).await {
                    while let Ok(Some(entry)) = rd.next_entry().await {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                    }
                }
                let _ = tokio::fs::remove_dir(&session_dir).await;

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
            seal_and_enqueue_chunks(
                &mut watcher,
                &session_folder_name,
                &channel_id,
                &info.streamer_name,
                &upload_tx,
                &event_tx,
                backend_opt.is_some(),
                true,
            )
            .await;

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

            // Clean up session directory if empty (e.g. no chunks were saved or all chunks/chat were already uploaded)
            let target = broadcast_identifier(&info.streamer_name, &channel_id);
            if let Ok(true) =
                crate::engine::cleanup::cleanup_session_dir_if_empty(&session_dir).await
            {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::clean(format!(
                        "[{target}] Cleaned up empty session folder '{}'",
                        session_dir.display()
                    ))))
                    .await;
            }
        })
    }
}
