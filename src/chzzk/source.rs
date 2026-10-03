use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::chzzk::models::LiveDetail;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Trait defining the live broadcast stream intake contract.
///
/// Encapsulates polling channel broadcast status, acquiring chat access tokens,
/// and resolving optional chat WebSocket endpoints.
pub trait LiveStreamSource: Send + Sync {
    /// Polls live broadcast details for the specified channel ID.
    fn get_live_detail<'a>(
        &'a self,
        channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<LiveDetail>>;

    /// Fetches the chat session access token for the given chat channel ID.
    fn get_chat_access_token<'a>(
        &'a self,
        chat_channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<String>>;

    /// Returns an optional chat WebSocket URL override if configured.
    fn chat_ws_url(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug, Default)]
struct MockState {
    default_channel_state: Option<LiveDetail>,
    sticky_states: HashMap<String, LiveDetail>,
    sequence_queues: HashMap<String, VecDeque<LiveDetail>>,
    channel_errors: HashMap<String, String>,
    global_error: Option<String>,
    chat_tokens: HashMap<String, String>,
    default_chat_token: Option<String>,
    chat_token_errors: HashMap<String, String>,
    channel_call_counts: HashMap<String, usize>,
    chat_token_call_counts: HashMap<String, usize>,
}

/// In-memory fake implementation of [`LiveStreamSource`] for deterministic unit and integration tests.
#[derive(Clone, Default)]
pub struct MockLiveStreamSource {
    state: Arc<Mutex<MockState>>,
    chat_ws_url: Option<String>,
}

impl MockLiveStreamSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_chat_ws_url(mut self, url: impl Into<String>) -> Self {
        self.chat_ws_url = Some(url.into());
        self
    }

    /// Sets the default broadcast state returned when no channel-specific state or queue item exists.
    pub fn set_default_channel_state(&self, detail: LiveDetail) {
        let mut state = self.state.lock().unwrap();
        state.default_channel_state = Some(detail);
    }

    /// Builder helper to configure the default broadcast state.
    pub fn with_default_channel_state(self, detail: LiveDetail) -> Self {
        self.set_default_channel_state(detail);
        self
    }

    /// Configures a sticky broadcast state for the specified channel ID.
    pub fn set_channel_state(&self, channel_id: impl Into<String>, detail: LiveDetail) {
        let mut state = self.state.lock().unwrap();
        state.sticky_states.insert(channel_id.into(), detail);
    }

    /// Builder helper to configure a sticky broadcast state for the specified channel ID.
    pub fn with_channel_state(self, channel_id: impl Into<String>, detail: LiveDetail) -> Self {
        self.set_channel_state(channel_id, detail);
        self
    }

    /// Enqueues a single sequential state transition for the specified channel ID.
    pub fn enqueue_channel_state(&self, channel_id: impl Into<String>, detail: LiveDetail) {
        self.enqueue_channel_states(channel_id, [detail]);
    }

    /// Enqueues multiple sequential state transitions for the specified channel ID.
    pub fn enqueue_channel_states<I>(&self, channel_id: impl Into<String>, details: I)
    where
        I: IntoIterator<Item = LiveDetail>,
    {
        let mut state = self.state.lock().unwrap();
        let queue = state.sequence_queues.entry(channel_id.into()).or_default();
        for detail in details {
            queue.push_back(detail);
        }
    }

    /// Injects an error for a specific channel ID, causing subsequent polling to fail.
    pub fn inject_channel_error(&self, channel_id: impl Into<String>, error: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        state.channel_errors.insert(channel_id.into(), error.into());
    }

    /// Clears any previously injected error for the specified channel ID.
    pub fn clear_channel_error(&self, channel_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.channel_errors.remove(channel_id);
    }

    /// Injects a global error affecting all channel and chat token queries.
    pub fn inject_global_error(&self, error: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        state.global_error = Some(error.into());
    }

    /// Clears any global error currently set.
    pub fn clear_global_error(&self) {
        let mut state = self.state.lock().unwrap();
        state.global_error = None;
    }

    /// Configures the chat access token returned for a specific chat channel ID.
    pub fn set_chat_token(&self, chat_channel_id: impl Into<String>, token: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        state
            .chat_tokens
            .insert(chat_channel_id.into(), token.into());
    }

    /// Builder helper to configure a chat access token for a specific chat channel ID.
    pub fn with_chat_token(
        self,
        chat_channel_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.set_chat_token(chat_channel_id, token);
        self
    }

    /// Sets the default chat access token returned when no channel-specific token is configured.
    pub fn set_default_chat_token(&self, token: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        state.default_chat_token = Some(token.into());
    }

    /// Builder helper to configure the default chat access token.
    pub fn with_default_chat_token(self, token: impl Into<String>) -> Self {
        self.set_default_chat_token(token);
        self
    }

    /// Injects an error for a specific chat channel token request.
    pub fn inject_chat_token_error(
        &self,
        chat_channel_id: impl Into<String>,
        error: impl Into<String>,
    ) {
        let mut state = self.state.lock().unwrap();
        state
            .chat_token_errors
            .insert(chat_channel_id.into(), error.into());
    }

    /// Clears any injected error for the specified chat channel token request.
    pub fn clear_chat_token_error(&self, chat_channel_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.chat_token_errors.remove(chat_channel_id);
    }

    /// Returns the number of times `get_live_detail` was invoked for the specified channel ID.
    pub fn call_count(&self, channel_id: &str) -> usize {
        let state = self.state.lock().unwrap();
        state
            .channel_call_counts
            .get(channel_id)
            .copied()
            .unwrap_or(0)
    }

    /// Returns the total number of `get_live_detail` calls across all channels.
    pub fn total_call_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.channel_call_counts.values().sum()
    }

    /// Returns the number of times `get_chat_access_token` was invoked for the specified chat channel ID.
    pub fn chat_token_call_count(&self, chat_channel_id: &str) -> usize {
        let state = self.state.lock().unwrap();
        state
            .chat_token_call_counts
            .get(chat_channel_id)
            .copied()
            .unwrap_or(0)
    }

    /// Returns the total number of `get_chat_access_token` calls across all chat channels.
    pub fn total_chat_token_call_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.chat_token_call_counts.values().sum()
    }

    /// Resets all channel detail and chat token invocation counters to zero.
    pub fn reset_call_counts(&self) {
        let mut state = self.state.lock().unwrap();
        state.channel_call_counts.clear();
        state.chat_token_call_counts.clear();
    }
}

impl LiveStreamSource for MockLiveStreamSource {
    fn get_live_detail<'a>(
        &'a self,
        channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<LiveDetail>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();

            *state
                .channel_call_counts
                .entry(channel_id.to_string())
                .or_insert(0) += 1;

            if let Some(err) = &state.global_error {
                anyhow::bail!("{err}");
            }

            if let Some(err) = state.channel_errors.get(channel_id) {
                anyhow::bail!("{err}");
            }

            if let Some(queue) = state.sequence_queues.get_mut(channel_id) {
                if let Some(next_state) = queue.pop_front() {
                    return Ok(next_state);
                }
            }

            if let Some(detail) = state.sticky_states.get(channel_id) {
                return Ok(detail.clone());
            }

            if let Some(detail) = &state.default_channel_state {
                return Ok(detail.clone());
            }

            Ok(LiveDetail::Close {
                streamer_name: None,
            })
        })
    }

    fn get_chat_access_token<'a>(
        &'a self,
        chat_channel_id: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<String>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();

            *state
                .chat_token_call_counts
                .entry(chat_channel_id.to_string())
                .or_insert(0) += 1;

            if let Some(err) = &state.global_error {
                anyhow::bail!("{err}");
            }

            if let Some(err) = state.chat_token_errors.get(chat_channel_id) {
                anyhow::bail!("{err}");
            }

            if let Some(token) = state.chat_tokens.get(chat_channel_id) {
                return Ok(token.clone());
            }

            if let Some(token) = &state.default_chat_token {
                return Ok(token.clone());
            }

            Ok("mock_access_token".to_string())
        })
    }

    fn chat_ws_url(&self) -> Option<&str> {
        self.chat_ws_url.as_deref()
    }
}
