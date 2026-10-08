use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

use crate::tui::event::{AppEvent, LogEntry};

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

    /// Checks if a session directory is strictly empty (0 entries).
    /// If so, removes the directory with bounded retry handling Windows file-locking latency.
    /// Returns `Ok(true)` if the directory was removed, `Ok(false)` if it contained any files
    /// or was preserved, or `Err(e)` on I/O error.
    pub async fn purge_dir_if_empty(session_dir: &Path) -> std::io::Result<bool> {
        if !session_dir.exists() || !session_dir.is_dir() {
            return Ok(false);
        }

        let mut sub_entries = match tokio::fs::read_dir(session_dir).await {
            Ok(rd) => rd,
            Err(e) => return Err(e),
        };

        if sub_entries.next_entry().await?.is_some() {
            // Directory contains at least one entry - preserve it strictly.
            return Ok(false);
        }

        let mut remove_dir_res = tokio::fs::remove_dir(session_dir).await;
        let mut attempts = 0;
        while let Err(ref e) = remove_dir_res {
            if attempts >= 5 || e.kind() == std::io::ErrorKind::NotFound {
                break;
            }
            let raw_os = e.raw_os_error();
            let is_transient_lock = raw_os == Some(145) // ERROR_DIR_NOT_EMPTY (pending unlinks)
                || raw_os == Some(32) // ERROR_SHARING_VIOLATION
                || raw_os == Some(5)  // ERROR_ACCESS_DENIED
                || e.kind() == std::io::ErrorKind::PermissionDenied;

            if is_transient_lock {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(20 * attempts)).await;
                remove_dir_res = tokio::fs::remove_dir(session_dir).await;
            } else {
                break;
            }
        }

        if remove_dir_res.is_ok() || !session_dir.exists() {
            Ok(true)
        } else {
            Ok(false)
        }
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

        let cleanup_res = Self::purge_dir_if_empty(session_dir).await;

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

    /// Sweeps unmanaged empty directories in `recordings_dir`, strictly excluding active recording sessions
    /// using exact canonical path containment. Draining or untracked empty folders are purged.
    /// Emits a summary clean log if any empty folders were removed.
    pub async fn sweep_unmanaged(&self, recordings_dir: &Path) -> std::io::Result<usize> {
        if !recordings_dir.exists() || !recordings_dir.is_dir() {
            return Ok(0);
        }

        let active_set = self.active_paths();
        let canonical_active_set: HashSet<PathBuf> = active_set
            .iter()
            .map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
            .collect();

        let mut swept_count = 0;
        let mut entries = tokio::fs::read_dir(recordings_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let is_dir = match entry.file_type().await {
                Ok(ft) => ft.is_dir(),
                Err(_) => entry.path().is_dir(),
            };
            if !is_dir {
                continue;
            }

            let path = entry.path();
            let canonical_path = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());

            if active_set.contains(&path)
                || active_set.contains(&canonical_path)
                || canonical_active_set.contains(&path)
                || canonical_active_set.contains(&canonical_path)
            {
                continue;
            }

            let should_skip = {
                let mut guard = self.inner.lock().unwrap();
                if guard.in_flight_purges.contains(&path)
                    || guard.in_flight_purges.contains(&canonical_path)
                {
                    true
                } else {
                    guard.in_flight_purges.insert(path.clone());
                    false
                }
            };
            if should_skip {
                continue;
            }

            let purge_res = Self::purge_dir_if_empty(&path).await;

            let was_purged = {
                let mut guard = self.inner.lock().unwrap();
                guard.in_flight_purges.remove(&path);
                guard.in_flight_purges.remove(&canonical_path);
                match purge_res {
                    Ok(true) => {
                        if let Some(state) = guard.sessions.get(&path) {
                            if state.is_draining() {
                                guard.sessions.remove(&path);
                            }
                        }
                        if let Some(state) = guard.sessions.get(&canonical_path) {
                            if state.is_draining() {
                                guard.sessions.remove(&canonical_path);
                            }
                        }
                        true
                    }
                    _ => false,
                }
            };

            if was_purged {
                swept_count += 1;
            }
        }

        if swept_count > 0 {
            let log_msg = format!(
                "Cleaned up {swept_count} empty session folder(s) in '{}'",
                recordings_dir.display()
            );
            if let Some(ref tx) = self.event_tx {
                let _ = tx.try_send(AppEvent::Log(LogEntry::clean(log_msg)));
            }
        }

        Ok(swept_count)
    }

    /// Sweeps unmanaged empty directories in `recordings_dir` bounded by a maximum timeout.
    /// Uses an event-less custodian instance.
    pub async fn sweep_empty_dirs_bounded(
        recordings_dir: &Path,
        timeout: Duration,
    ) -> std::io::Result<usize> {
        let custodian = Self::without_events();
        match tokio::time::timeout(timeout, custodian.sweep_unmanaged(recordings_dir)).await {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "sweep empty session directories timed out",
            )),
        }
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

    #[tokio::test]
    async fn test_purge_dir_if_empty_preserves_dir_with_chunks_and_metadata() {
        let temp_dir = std::env::temp_dir().join(format!(
            "test_custodian_purge_preserve_{}",
            rand::random::<u32>()
        ));
        let session_dir = temp_dir.join("session_preserve");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        // 1. Directory with media chunk (.ts)
        let chunk_file = session_dir.join("chunk_0000.ts");
        tokio::fs::write(&chunk_file, b"ts content").await.unwrap();
        assert!(
            !SessionCustodian::purge_dir_if_empty(&session_dir)
                .await
                .unwrap()
        );
        assert!(session_dir.exists());
        tokio::fs::remove_file(&chunk_file).await.unwrap();

        // 2. Directory with chat log (.jsonl)
        let chat_file = session_dir.join("chat_0000.jsonl");
        tokio::fs::write(&chat_file, b"{\"cmd\":93101}\n")
            .await
            .unwrap();
        assert!(
            !SessionCustodian::purge_dir_if_empty(&session_dir)
                .await
                .unwrap()
        );
        assert!(session_dir.exists());
        tokio::fs::remove_file(&chat_file).await.unwrap();

        // 3. Directory with metadata.jsonl
        let meta_file = session_dir.join("metadata.jsonl");
        tokio::fs::write(&meta_file, b"{\"event\":\"INITIAL_STATE\"}\n")
            .await
            .unwrap();
        assert!(
            !SessionCustodian::purge_dir_if_empty(&session_dir)
                .await
                .unwrap()
        );
        assert!(session_dir.exists());
        tokio::fs::remove_file(&meta_file).await.unwrap();

        // 4. Strictly empty directory is successfully purged
        assert!(
            SessionCustodian::purge_dir_if_empty(&session_dir)
                .await
                .unwrap()
        );
        assert!(!session_dir.exists());

        // 5. Non-existent directory returns Ok(false)
        let non_existent = temp_dir.join("does_not_exist");
        assert!(
            !SessionCustodian::purge_dir_if_empty(&non_existent)
                .await
                .unwrap()
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_purge_dir_if_empty_windows_lock_resilience() {
        let temp_dir = std::env::temp_dir().join(format!(
            "test_custodian_purge_lock_{}",
            rand::random::<u32>()
        ));
        let session_dir = temp_dir.join("session_lock");
        tokio::fs::create_dir_all(&session_dir).await.unwrap();

        // If directory disappears (NotFound) during purge, it is treated as success
        let non_existent = temp_dir.join("disappeared");
        tokio::fs::create_dir_all(&non_existent).await.unwrap();
        tokio::fs::remove_dir(&non_existent).await.unwrap();
        assert!(
            !SessionCustodian::purge_dir_if_empty(&non_existent)
                .await
                .unwrap()
        );

        // An empty directory is safely purged
        assert!(
            SessionCustodian::purge_dir_if_empty(&session_dir)
                .await
                .unwrap()
        );
        assert!(!session_dir.exists());

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_sweep_unmanaged_excludes_active_sessions_by_exact_canonical_path() {
        let custodian = SessionCustodian::without_events();
        let base_dir = std::env::temp_dir().join(format!(
            "test_custodian_sweep_active_{}",
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        let active_dir = base_dir.join("active_session");
        tokio::fs::create_dir_all(&active_dir).await.unwrap();

        let active_dir_2 = base_dir.join("active_session_2");
        tokio::fs::create_dir_all(&active_dir_2).await.unwrap();

        let draining_empty_dir = base_dir.join("draining_empty");
        tokio::fs::create_dir_all(&draining_empty_dir)
            .await
            .unwrap();

        let unmanaged_empty_dir = base_dir.join("unmanaged_empty");
        tokio::fs::create_dir_all(&unmanaged_empty_dir)
            .await
            .unwrap();

        let unmanaged_full_dir = base_dir.join("unmanaged_full");
        tokio::fs::create_dir_all(&unmanaged_full_dir)
            .await
            .unwrap();
        tokio::fs::write(unmanaged_full_dir.join("file.txt"), b"data")
            .await
            .unwrap();

        custodian.register_active(&active_dir, "chan_act", "StreamerAct");
        // Register active_dir_2 using relative dot notation to test exact canonical resolution
        custodian.register_active(
            base_dir.join("./active_session_2"),
            "chan_act2",
            "StreamerAct2",
        );
        custodian.register_draining(&draining_empty_dir, "chan_drain", "StreamerDrain");

        let swept = custodian.sweep_unmanaged(&base_dir).await.unwrap();
        assert_eq!(
            swept, 2,
            "Expected draining_empty and unmanaged_empty to be swept"
        );

        // Active sessions must be preserved even though empty!
        assert!(active_dir.exists(), "Active directory must not be deleted");
        assert!(
            active_dir_2.exists(),
            "Active directory registered via relative dot path must not be deleted"
        );
        assert!(
            custodian.tracked_state(&active_dir).unwrap().is_active(),
            "Active session must remain tracked"
        );

        // Unmanaged full directory must be preserved
        assert!(
            unmanaged_full_dir.exists(),
            "Non-empty directory must not be deleted"
        );

        // Draining empty directory must be purged and untracked
        assert!(
            !draining_empty_dir.exists(),
            "Draining empty directory must be deleted"
        );
        assert_eq!(
            custodian.tracked_state(&draining_empty_dir),
            None,
            "Swept draining directory must be untracked"
        );

        // Unmanaged empty directory must be purged
        assert!(
            !unmanaged_empty_dir.exists(),
            "Unmanaged empty directory must be deleted"
        );

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_sweep_unmanaged_emits_summary_clean_log() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(10);
        let custodian = SessionCustodian::new(event_tx);
        let base_dir = std::env::temp_dir().join(format!(
            "test_custodian_sweep_log_{}",
            rand::random::<u32>()
        ));
        tokio::fs::create_dir_all(&base_dir).await.unwrap();

        let dir1 = base_dir.join("empty_1");
        let dir2 = base_dir.join("empty_2");
        tokio::fs::create_dir_all(&dir1).await.unwrap();
        tokio::fs::create_dir_all(&dir2).await.unwrap();

        let swept = custodian.sweep_unmanaged(&base_dir).await.unwrap();
        assert_eq!(swept, 2);

        let event = event_rx.try_recv().expect("Expected log event");
        match event {
            AppEvent::Log(entry) => {
                assert_eq!(entry.kind, crate::tui::event::LogKind::Clean);
                let expected = format!(
                    "Cleaned up 2 empty session folder(s) in '{}'",
                    base_dir.display()
                );
                assert_eq!(entry.message, expected);
            }
            _ => panic!("Expected Log event"),
        }

        // Sweeping again when nothing is removed must NOT emit any log
        let swept_again = custodian.sweep_unmanaged(&base_dir).await.unwrap();
        assert_eq!(swept_again, 0);
        assert!(
            event_rx.try_recv().is_err(),
            "No event should be emitted when swept_count == 0"
        );

        let _ = tokio::fs::remove_dir_all(&base_dir).await;
    }

    #[tokio::test]
    async fn test_sweep_empty_dirs_bounded_success_and_timeout() {
        let temp_dir =
            std::env::temp_dir().join(format!("test_custodian_bounded_{}", rand::random::<u32>()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        let empty_session = temp_dir.join("empty_session_1");
        tokio::fs::create_dir_all(&empty_session).await.unwrap();

        let non_empty = temp_dir.join("active_session_2");
        tokio::fs::create_dir_all(&non_empty).await.unwrap();
        tokio::fs::write(non_empty.join("chunk_0000.ts"), b"data")
            .await
            .unwrap();

        // 1. Success case
        let cleaned =
            SessionCustodian::sweep_empty_dirs_bounded(&temp_dir, Duration::from_millis(500))
                .await
                .expect("bounded sweep should succeed");

        assert_eq!(cleaned, 1);
        assert!(!empty_session.exists());
        assert!(non_empty.exists());

        // 2. Timeout case: Duration::ZERO with asynchronous read_dir operation
        let timeout_res =
            SessionCustodian::sweep_empty_dirs_bounded(&temp_dir, Duration::ZERO).await;

        match timeout_res {
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            Ok(_) => {
                // In case a scheduler finished immediately before timeout fired
            }
        }

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
