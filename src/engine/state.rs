use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::engine::session::{ActiveSessionState, FinishedSession};

/// Encapsulates the synchronized shared state of all monitored channels and active recording sessions.
#[derive(Clone)]
pub struct EngineState {
    pub active_recordings: Arc<Mutex<HashSet<String>>>,
    pub active_sessions: Arc<Mutex<HashMap<String, ActiveSessionState>>>,
    pub finished_sessions: Arc<Mutex<HashMap<String, FinishedSession>>>,
    pub restricted_channels: Arc<Mutex<HashSet<String>>>,
    pub restricted_live_ids: Arc<Mutex<HashMap<String, u64>>>,
    pub api_restricted_channels: Arc<Mutex<HashSet<String>>>,
    pub channel_names: Arc<RwLock<HashMap<String, String>>>,
    pub session_cancel_tokens: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            active_recordings: Arc::new(Mutex::new(HashSet::new())),
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            finished_sessions: Arc::new(Mutex::new(HashMap::new())),
            restricted_channels: Arc::new(Mutex::new(HashSet::new())),
            restricted_live_ids: Arc::new(Mutex::new(HashMap::new())),
            api_restricted_channels: Arc::new(Mutex::new(HashSet::new())),
            channel_names: Arc::new(RwLock::new(HashMap::new())),
            session_cancel_tokens: Arc::new(Mutex::new(HashMap::new())),
        }
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
}
