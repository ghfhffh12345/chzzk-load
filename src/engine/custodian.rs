use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;

use crate::tui::event::AppEvent;

/// Lifecycle tracking state for a monitored session directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTrackState {
    Active {
        channel_id: String,
        streamer_name: String,
    },
    Draining {
        channel_id: String,
        streamer_name: String,
    },
}

impl SessionTrackState {
    pub fn channel_id(&self) -> &str {
        match self {
            Self::Active { channel_id, .. } | Self::Draining { channel_id, .. } => channel_id,
        }
    }

    pub fn streamer_name(&self) -> &str {
        match self {
            Self::Active { streamer_name, .. } | Self::Draining { streamer_name, .. } => {
                streamer_name
            }
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn is_draining(&self) -> bool {
        matches!(self, Self::Draining { .. })
    }
}

#[derive(Debug, Default)]
struct CustodianInner {
    sessions: HashMap<PathBuf, SessionTrackState>,
    in_flight_purges: HashSet<PathBuf>,
}

/// Thread-safe coordinator managing recording session directory lifecycles.
#[derive(Debug, Clone)]
pub struct SessionCustodian {
    inner: Arc<Mutex<CustodianInner>>,
    event_tx: Option<Sender<AppEvent>>,
}

impl SessionCustodian {
    pub fn new(event_tx: Sender<AppEvent>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CustodianInner::default())),
            event_tx: Some(event_tx),
        }
    }

    pub fn without_events() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CustodianInner::default())),
            event_tx: None,
        }
    }

    pub fn register_active(
        &self,
        session_dir: impl Into<PathBuf>,
        channel_id: impl Into<String>,
        streamer_name: impl Into<String>,
    ) {
        let mut guard = self.inner.lock().unwrap();
        guard.sessions.insert(
            session_dir.into(),
            SessionTrackState::Active {
                channel_id: channel_id.into(),
                streamer_name: streamer_name.into(),
            },
        );
    }

    pub fn register_draining(
        &self,
        session_dir: impl Into<PathBuf>,
        channel_id: impl Into<String>,
        streamer_name: impl Into<String>,
    ) {
        let mut guard = self.inner.lock().unwrap();
        guard.sessions.insert(
            session_dir.into(),
            SessionTrackState::Draining {
                channel_id: channel_id.into(),
                streamer_name: streamer_name.into(),
            },
        );
    }

    pub fn mark_concluded(&self, session_dir: &Path) -> bool {
        let mut guard = self.inner.lock().unwrap();
        if let Some(state) = guard.sessions.get_mut(session_dir) {
            match state {
                SessionTrackState::Active {
                    channel_id,
                    streamer_name,
                } => {
                    *state = SessionTrackState::Draining {
                        channel_id: channel_id.clone(),
                        streamer_name: streamer_name.clone(),
                    };
                    true
                }
                SessionTrackState::Draining { .. } => true,
            }
        } else {
            false
        }
    }

    pub fn active_paths(&self) -> HashSet<PathBuf> {
        let guard = self.inner.lock().unwrap();
        guard
            .sessions
            .iter()
            .filter_map(|(path, state)| {
                if state.is_active() {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn tracked_state(&self, session_dir: &Path) -> Option<SessionTrackState> {
        let guard = self.inner.lock().unwrap();
        guard.sessions.get(session_dir).cloned()
    }

    pub async fn try_purge(&self, session_dir: &Path) -> std::io::Result<bool> {
        let (channel_id, streamer_name) = {
            let mut guard = self.inner.lock().unwrap();
            if guard.in_flight_purges.contains(session_dir) {
                return Ok(false);
            }
            match guard.sessions.get(session_dir) {
                Some(SessionTrackState::Active { .. }) => return Ok(false),
                Some(SessionTrackState::Draining {
                    channel_id,
                    streamer_name,
                }) => {
                    let id = channel_id.clone();
                    let name = streamer_name.clone();
                    guard.in_flight_purges.insert(session_dir.to_path_buf());
                    (id, name)
                }
                None => return Ok(false),
            }
        };

        if !session_dir.exists() {
            let mut guard = self.inner.lock().unwrap();
            guard.in_flight_purges.remove(session_dir);
            if let Some(state) = guard.sessions.get(session_dir) {
                if state.is_draining() {
                    guard.sessions.remove(session_dir);
                }
            }
            return Ok(false);
        }

        let cleanup_res = crate::engine::cleanup::cleanup_session_dir_if_empty(session_dir).await;

        let should_log = {
            let mut guard = self.inner.lock().unwrap();
            guard.in_flight_purges.remove(session_dir);
            if matches!(cleanup_res, Ok(true)) {
                if let Some(state) = guard.sessions.get(session_dir) {
                    if state.is_draining() {
                        guard.sessions.remove(session_dir);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        };

        match cleanup_res {
            Ok(true) => {
                if should_log {
                    let target = crate::uploader::broadcast_identifier(&streamer_name, &channel_id);
                    let log_msg = format!(
                        "[{target}] Cleaned up empty session folder '{}'",
                        session_dir.display()
                    );
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx.try_send(crate::tui::event::AppEvent::Log(
                            crate::tui::event::LogEntry::clean(log_msg),
                        ));
                    }
                }
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn try_purge_drained(&self) -> usize {
        let draining_dirs: Vec<PathBuf> = {
            let guard = self.inner.lock().unwrap();
            guard
                .sessions
                .iter()
                .filter_map(|(path, state)| {
                    if state.is_draining() {
                        Some(path.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut purged_count = 0;
        for dir in draining_dirs {
            if let Ok(true) = self.try_purge(&dir).await {
                purged_count += 1;
            }
        }
        purged_count
    }
}

impl Default for SessionCustodian {
    fn default() -> Self {
        Self::without_events()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_active_directory_is_strictly_protected_from_deletion() {
        let custodian = SessionCustodian::without_events();
        let temp_dir =
            std::env::temp_dir().join(format!("test_custodian_active_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        custodian.register_active(&temp_dir, "channel_1", "Streamer One");

        // Verify tracked state
        let state = custodian.tracked_state(&temp_dir);
        assert_eq!(
            state,
            Some(SessionTrackState::Active {
                channel_id: "channel_1".to_string(),
                streamer_name: "Streamer One".to_string(),
            })
        );
        assert!(custodian.active_paths().contains(&temp_dir));

        // Even if empty, an active session directory must NEVER be purged
        let purged = custodian.try_purge(&temp_dir).await.unwrap();
        assert!(!purged, "Active directory must not be purged");
        assert!(
            temp_dir.exists(),
            "Active directory must still exist on disk"
        );

        // Cleanup test directory
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_concluded_directory_with_chunks_is_preserved() {
        let custodian = SessionCustodian::without_events();
        let temp_dir =
            std::env::temp_dir().join(format!("test_custodian_chunks_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        custodian.register_active(&temp_dir, "channel_2", "Streamer Two");
        assert!(custodian.mark_concluded(&temp_dir));

        let state = custodian.tracked_state(&temp_dir);
        assert_eq!(
            state,
            Some(SessionTrackState::Draining {
                channel_id: "channel_2".to_string(),
                streamer_name: "Streamer Two".to_string(),
            })
        );

        // Populate with media chunks
        let video_chunk = temp_dir.join("chunk_0000.ts");
        let chat_chunk = temp_dir.join("chat_0000.jsonl");
        tokio::fs::write(&video_chunk, b"video data").await.unwrap();
        tokio::fs::write(&chat_chunk, b"{\"cmd\":93101}\n")
            .await
            .unwrap();

        let purged = custodian.try_purge(&temp_dir).await.unwrap();
        assert!(!purged, "Directory with chunks must not be purged");
        assert!(
            temp_dir.exists(),
            "Directory with chunks must still exist on disk"
        );
        assert!(video_chunk.exists(), "Video chunk must still exist on disk");
        assert!(chat_chunk.exists(), "Chat chunk must still exist on disk");

        // Directory must still remain tracked in Draining state
        assert_eq!(
            custodian.tracked_state(&temp_dir),
            Some(SessionTrackState::Draining {
                channel_id: "channel_2".to_string(),
                streamer_name: "Streamer Two".to_string(),
            })
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_concluded_empty_or_metadata_only_directory_is_purged_and_untracked() {
        let custodian = SessionCustodian::without_events();
        let base_dir = std::env::temp_dir().join(format!(
            "test_custodian_quiescent_{}",
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        // Subcase A: Directory with only metadata.jsonl
        let dir_a = base_dir.join("session_a");
        tokio::fs::create_dir_all(&dir_a).await.unwrap();
        let meta_file = dir_a.join("metadata.jsonl");
        tokio::fs::write(&meta_file, b"{\"event\":\"INITIAL_STATE\"}\n")
            .await
            .unwrap();

        custodian.register_active(&dir_a, "channel_a", "Streamer A");
        custodian.mark_concluded(&dir_a);

        // Under strict emptiness, presence of metadata.jsonl preserves the directory
        let purged_a = custodian.try_purge(&dir_a).await.unwrap();
        assert!(
            !purged_a,
            "Under strict emptiness, directory with metadata.jsonl must NOT be purged until empty"
        );
        assert!(
            dir_a.exists(),
            "Directory with metadata must still exist on disk"
        );

        // Once metadata.jsonl is removed (simulating confirmed upload & unlink), it is purged
        tokio::fs::remove_file(&meta_file).await.unwrap();
        let purged_a_empty = custodian.try_purge(&dir_a).await.unwrap();
        assert!(purged_a_empty, "Strictly empty directory must be purged");
        assert!(!dir_a.exists(), "Purged directory must not exist on disk");
        assert_eq!(
            custodian.tracked_state(&dir_a),
            None,
            "Purged directory must be untracked"
        );

        // Subcase B: Completely empty directory
        let dir_b = base_dir.join("session_b");
        tokio::fs::create_dir_all(&dir_b).await.unwrap();

        custodian.register_active(&dir_b, "channel_b", "Streamer B");
        custodian.mark_concluded(&dir_b);

        let purged_b = custodian.try_purge(&dir_b).await.unwrap();
        assert!(purged_b, "Completely empty directory must be purged");
        assert!(!dir_b.exists(), "Purged directory must not exist on disk");
        assert_eq!(
            custodian.tracked_state(&dir_b),
            None,
            "Purged directory must be untracked"
        );

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_purge_emits_streamer_attributed_clean_log() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(10);
        let custodian = SessionCustodian::new(event_tx);
        let base_dir =
            std::env::temp_dir().join(format!("test_custodian_logs_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        // 1. With streamer name
        let dir_1 = base_dir.join("streamer_dir");
        tokio::fs::create_dir_all(&dir_1).await.unwrap();
        custodian.register_active(&dir_1, "chan_123", "StreamerAlpha");
        custodian.mark_concluded(&dir_1);

        assert!(custodian.try_purge(&dir_1).await.unwrap());

        let event1 = event_rx.try_recv().expect("Expected clean log event");
        match event1 {
            AppEvent::Log(entry) => {
                assert_eq!(entry.kind, crate::tui::event::LogKind::Clean);
                assert!(
                    entry
                        .message
                        .contains("[StreamerAlpha] Cleaned up empty session folder"),
                    "Log message must contain streamer name attribute: {}",
                    entry.message
                );
            }
            _ => panic!("Expected Log event"),
        }

        // 2. With empty streamer name -> fallback to channel ID
        let dir_2 = base_dir.join("fallback_dir");
        tokio::fs::create_dir_all(&dir_2).await.unwrap();
        custodian.register_active(&dir_2, "chan_fallback", "");
        custodian.mark_concluded(&dir_2);

        assert!(custodian.try_purge(&dir_2).await.unwrap());

        let event2 = event_rx.try_recv().expect("Expected clean log event");
        match event2 {
            AppEvent::Log(entry) => {
                assert_eq!(entry.kind, crate::tui::event::LogKind::Clean);
                assert!(
                    entry
                        .message
                        .contains("[chan_fallback] Cleaned up empty session folder"),
                    "Log message must contain channel_id attribute when streamer name is empty: {}",
                    entry.message
                );
            }
            _ => panic!("Expected Log event"),
        }

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_try_purge_drained_batch_purges_only_quiescent_directories() {
        let custodian = SessionCustodian::without_events();
        let base_dir =
            std::env::temp_dir().join(format!("test_custodian_batch_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        // 1. Active directory (empty)
        let dir_active = base_dir.join("dir_active");
        tokio::fs::create_dir_all(&dir_active).await.unwrap();
        custodian.register_active(&dir_active, "chan_act", "StreamerAct");

        // 2. Draining with chunks
        let dir_chunks = base_dir.join("dir_chunks");
        tokio::fs::create_dir_all(&dir_chunks).await.unwrap();
        tokio::fs::write(dir_chunks.join("chunk_0000.ts"), b"data")
            .await
            .unwrap();
        custodian.register_active(&dir_chunks, "chan_chk", "StreamerChk");
        custodian.mark_concluded(&dir_chunks);

        // 3. Draining with metadata only
        let dir_meta = base_dir.join("dir_meta");
        tokio::fs::create_dir_all(&dir_meta).await.unwrap();
        tokio::fs::write(dir_meta.join("metadata.jsonl"), b"{}\n")
            .await
            .unwrap();
        custodian.register_active(&dir_meta, "chan_meta", "StreamerMeta");
        custodian.mark_concluded(&dir_meta);

        // 4. Draining completely empty
        let dir_empty = base_dir.join("dir_empty");
        tokio::fs::create_dir_all(&dir_empty).await.unwrap();
        custodian.register_active(&dir_empty, "chan_emp", "StreamerEmp");
        custodian.mark_concluded(&dir_empty);

        let purged_count = custodian.try_purge_drained().await;
        assert_eq!(
            purged_count, 1,
            "Expected exactly 1 strictly empty quiescent directory purged"
        );

        // Active, chunks, and metadata-containing directories must survive and stay tracked
        assert!(dir_active.exists());
        assert!(custodian.tracked_state(&dir_active).unwrap().is_active());

        assert!(dir_chunks.exists());
        assert!(custodian.tracked_state(&dir_chunks).unwrap().is_draining());

        assert!(dir_meta.exists());
        assert!(custodian.tracked_state(&dir_meta).unwrap().is_draining());

        // Strictly empty directory must be deleted and untracked
        assert!(!dir_empty.exists());
        assert_eq!(custodian.tracked_state(&dir_empty), None);

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_untracked_directory_and_idempotent_concluded() {
        let custodian = SessionCustodian::without_events();
        let base_dir =
            std::env::temp_dir().join(format!("test_custodian_edge_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        let dir_untracked = base_dir.join("untracked");
        tokio::fs::create_dir_all(&dir_untracked).await.unwrap();

        // 1. mark_concluded on untracked path returns false
        assert!(!custodian.mark_concluded(&dir_untracked));

        // 2. try_purge on untracked path returns Ok(false) and preserves directory
        let purged = custodian.try_purge(&dir_untracked).await.unwrap();
        assert!(!purged);
        assert!(dir_untracked.exists());

        // 3. mark_concluded is idempotent on tracked path
        let dir_tracked = base_dir.join("tracked");
        tokio::fs::create_dir_all(&dir_tracked).await.unwrap();
        custodian.register_active(&dir_tracked, "chan_x", "StreamerX");
        assert!(custodian.mark_concluded(&dir_tracked));
        assert!(custodian.mark_concluded(&dir_tracked));
        assert!(custodian.tracked_state(&dir_tracked).unwrap().is_draining());

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_register_draining_direct() {
        let custodian = SessionCustodian::without_events();
        let base_dir = std::env::temp_dir().join(format!(
            "test_custodian_reg_drain_{}",
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        custodian.register_draining(&base_dir, "chan_direct", "StreamerDirect");

        assert_eq!(
            custodian.tracked_state(&base_dir),
            Some(SessionTrackState::Draining {
                channel_id: "chan_direct".to_string(),
                streamer_name: "StreamerDirect".to_string(),
            })
        );
        assert!(!custodian.active_paths().contains(&base_dir));

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_concurrent_try_purge_deduplication() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(10);
        let custodian = SessionCustodian::new(event_tx);
        let base_dir = std::env::temp_dir().join(format!(
            "test_custodian_concurrent_{}",
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        custodian.register_draining(&base_dir, "chan_con", "StreamerCon");

        // Spawn multiple concurrent try_purge tasks
        let (res1, res2) = tokio::join!(
            custodian.try_purge(&base_dir),
            custodian.try_purge(&base_dir),
        );

        // At least one must succeed, but exactly one log event should be emitted
        assert!(res1.unwrap() || res2.unwrap());
        assert!(!base_dir.exists());

        let mut log_count = 0;
        while let Ok(AppEvent::Log(_)) = event_rx.try_recv() {
            log_count += 1;
        }
        assert_eq!(
            log_count, 1,
            "Exactly one clean log must be emitted despite concurrent calls"
        );
    }
}
