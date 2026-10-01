use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::chzzk::models::{LiveDetail, LiveStreamInfo};
use crate::chzzk::models_metadata::{MetadataDelta, MetadataEvent};
use crate::engine::session::ActiveSessionState;

/// Autonomous polling decision returned by [`ChannelLifecycleRegistry::evaluate_poll`].
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum PollAction {
    /// Channel is live and ready for a new recording session to be spawned.
    ReadyToRecord {
        info: LiveStreamInfo,
        display_name: String,
        was_api_restricted: bool,
    },
    /// Channel is currently recording and incoming metadata differs from current state.
    RecordingMetadataChanged {
        delta: MetadataDelta,
        event: MetadataEvent,
        remote_dir: String,
        full_jsonl: String,
        display_name: String,
        title: String,
    },
    /// Channel is already recording and metadata is unchanged.
    AlreadyRecording { display_name: String, title: String },
    /// Stream recently concluded and is currently within the post-broadcast cooldown window.
    InCooldown {
        display_name: String,
        live_id: Option<u64>,
        elapsed: Duration,
        remaining: Duration,
    },
    /// Stream is access-restricted (19+ adult, membership/paywall, or 403 key forbidden).
    Restricted {
        display_name: String,
        title: String,
        reason: RestrictionReason,
        is_newly_restricted: bool,
    },
    /// Stream is closed (channel offline).
    StreamClosed {
        display_name: String,
        was_recording: bool,
    },
}

/// Categorical discriminant for [`ChannelLifecycleState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelLifecycleKind {
    Idle,
    Recording,
    Cooldown,
    Restricted,
}

/// Reasons why a channel or stream is access-restricted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RestrictionReason {
    AgeRestricted,
    RequiresCredentials,
    KeyForbidden,
    Custom(String),
}

impl RestrictionReason {
    pub fn is_adult(&self) -> bool {
        matches!(self, Self::AgeRestricted)
    }

    pub fn display_message(&self, channel_id: &str, display_name: &str) -> String {
        match self {
            Self::AgeRestricted => format!(
                "Recording unavailable for channel {} ({}): 19+ age-restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                channel_id, display_name
            ),
            Self::RequiresCredentials | Self::KeyForbidden => format!(
                "Recording unavailable for channel {} ({}): restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                channel_id, display_name
            ),
            Self::Custom(msg) => format!(
                "Recording unavailable for channel {} ({}): {}",
                channel_id, display_name, msg
            ),
        }
    }
}

/// Mutually exclusive discrete states for a monitored channel.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum ChannelLifecycleState {
    Idle,
    Recording {
        session: ActiveSessionState,
        cancel_token: CancellationToken,
    },
    Cooldown {
        live_id: Option<u64>,
        finished_at: Instant,
    },
    Restricted {
        live_id: Option<u64>,
        reason: RestrictionReason,
    },
}

impl ChannelLifecycleState {
    pub fn kind(&self) -> ChannelLifecycleKind {
        match self {
            Self::Idle => ChannelLifecycleKind::Idle,
            Self::Recording { .. } => ChannelLifecycleKind::Recording,
            Self::Cooldown { .. } => ChannelLifecycleKind::Cooldown,
            Self::Restricted { .. } => ChannelLifecycleKind::Restricted,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub fn is_recording(&self) -> bool {
        matches!(self, Self::Recording { .. })
    }

    pub fn is_cooldown(&self) -> bool {
        matches!(self, Self::Cooldown { .. })
    }

    pub fn is_restricted(&self) -> bool {
        matches!(self, Self::Restricted { .. })
    }
}

#[derive(Default)]
struct RegistryInner {
    channels: HashMap<String, ChannelLifecycleState>,
    streamer_names: HashMap<String, String>,
}

impl RegistryInner {
    fn transition_state(
        &mut self,
        channel_id: &str,
        new_state: ChannelLifecycleState,
    ) -> ChannelLifecycleState {
        let old = self
            .channels
            .insert(channel_id.to_string(), new_state)
            .unwrap_or(ChannelLifecycleState::Idle);

        if let ChannelLifecycleState::Recording {
            ref cancel_token, ..
        } = old
        {
            cancel_token.cancel();
        }

        old
    }
}

/// A self-contained, thread-safe Channel Lifecycle Registry module that coordinates
/// channel states and atomic transitions behind a single mutex lock.
#[derive(Clone, Default)]
pub struct ChannelLifecycleRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

impl ChannelLifecycleRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner::default())),
        }
    }

    pub fn channel_state(&self, channel_id: &str) -> ChannelLifecycleState {
        let guard = self.inner.lock().unwrap();
        guard
            .channels
            .get(channel_id)
            .cloned()
            .unwrap_or(ChannelLifecycleState::Idle)
    }

    pub fn is_recording(&self, channel_id: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        guard
            .channels
            .get(channel_id)
            .map(|s| s.is_recording())
            .unwrap_or(false)
    }

    pub fn is_restricted(&self, channel_id: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        guard
            .channels
            .get(channel_id)
            .map(|s| s.is_restricted())
            .unwrap_or(false)
    }

    pub fn active_session(&self, channel_id: &str) -> Option<ActiveSessionState> {
        let guard = self.inner.lock().unwrap();
        match guard.channels.get(channel_id) {
            Some(ChannelLifecycleState::Recording { session, .. }) => Some(session.clone()),
            _ => None,
        }
    }

    pub fn active_recording_ids(&self) -> Vec<String> {
        let guard = self.inner.lock().unwrap();
        let mut ids: Vec<String> = guard
            .channels
            .iter()
            .filter_map(|(id, state)| {
                if state.is_recording() {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        ids.sort();
        ids
    }

    pub fn start_recording(
        &self,
        channel_id: &str,
        session: ActiveSessionState,
        cancel_token: CancellationToken,
    ) {
        let mut guard = self.inner.lock().unwrap();
        guard.transition_state(
            channel_id,
            ChannelLifecycleState::Recording {
                session,
                cancel_token,
            },
        );
    }

    pub fn finish_recording(&self, channel_id: &str, live_id: Option<u64>) {
        self.mark_cooldown(channel_id, live_id, Instant::now());
    }

    pub fn mark_cooldown(&self, channel_id: &str, live_id: Option<u64>, finished_at: Instant) {
        let mut guard = self.inner.lock().unwrap();
        guard.transition_state(
            channel_id,
            ChannelLifecycleState::Cooldown {
                live_id,
                finished_at,
            },
        );
    }

    pub fn mark_restricted(
        &self,
        channel_id: &str,
        live_id: Option<u64>,
        reason: RestrictionReason,
    ) {
        let mut guard = self.inner.lock().unwrap();
        guard.transition_state(
            channel_id,
            ChannelLifecycleState::Restricted { live_id, reason },
        );
    }

    pub fn reset_to_idle(&self, channel_id: &str) {
        let mut guard = self.inner.lock().unwrap();
        guard.transition_state(channel_id, ChannelLifecycleState::Idle);
    }

    pub fn cancel_channel(&self, channel_id: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        if let Some(ChannelLifecycleState::Recording { cancel_token, .. }) =
            guard.channels.get(channel_id)
        {
            cancel_token.cancel();
            true
        } else {
            false
        }
    }

    pub fn cancel_all(&self) {
        let guard = self.inner.lock().unwrap();
        for state in guard.channels.values() {
            if let ChannelLifecycleState::Recording { cancel_token, .. } = state {
                cancel_token.cancel();
            }
        }
    }

    pub fn get_cancel_token(&self, channel_id: &str) -> Option<CancellationToken> {
        let guard = self.inner.lock().unwrap();
        match guard.channels.get(channel_id) {
            Some(ChannelLifecycleState::Recording { cancel_token, .. }) => {
                Some(cancel_token.clone())
            }
            _ => None,
        }
    }

    pub fn cache_streamer_name(&self, channel_id: impl Into<String>, name: impl Into<String>) {
        let mut guard = self.inner.lock().unwrap();
        guard.streamer_names.insert(channel_id.into(), name.into());
    }

    pub fn cached_streamer_name(&self, channel_id: &str) -> Option<String> {
        let guard = self.inner.lock().unwrap();
        guard.streamer_names.get(channel_id).cloned()
    }

    pub fn cached_streamer_names(&self) -> HashMap<String, String> {
        let guard = self.inner.lock().unwrap();
        guard.streamer_names.clone()
    }

    pub fn all_channel_states(&self) -> HashMap<String, ChannelLifecycleState> {
        let guard = self.inner.lock().unwrap();
        guard.channels.clone()
    }

    pub fn active_sessions(&self) -> HashMap<String, ActiveSessionState> {
        let guard = self.inner.lock().unwrap();
        guard
            .channels
            .iter()
            .filter_map(|(id, state)| match state {
                ChannelLifecycleState::Recording { session, .. } => {
                    Some((id.clone(), session.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn resolve_name_from_guard(
        guard: &RegistryInner,
        channel_id: &str,
        alias: Option<&str>,
    ) -> String {
        if let Some(alias) = alias.map(|s| s.trim()).filter(|s| !s.is_empty()) {
            alias.to_string()
        } else {
            guard
                .streamer_names
                .get(channel_id)
                .cloned()
                .unwrap_or_else(|| channel_id.to_string())
        }
    }

    pub fn resolve_display_name(&self, channel_id: &str, alias: Option<&str>) -> String {
        let guard = self.inner.lock().unwrap();
        Self::resolve_name_from_guard(&guard, channel_id, alias)
    }

    pub fn evaluate_poll(
        &self,
        channel_id: &str,
        alias: Option<&str>,
        detail: &LiveDetail,
        cooldown_window: Duration,
    ) -> PollAction {
        let mut guard = self.inner.lock().unwrap();

        // 1. Cache streamer name if provided in live detail
        let incoming_streamer_name = match detail {
            LiveDetail::Open(info) => Some(info.streamer_name.as_str()),
            LiveDetail::Restricted { streamer_name, .. } => Some(streamer_name.as_str()),
            LiveDetail::Close { streamer_name } => streamer_name.as_deref(),
        };
        if let Some(streamer_name) = incoming_streamer_name {
            guard
                .streamer_names
                .insert(channel_id.to_string(), streamer_name.to_string());
        }

        // 2. Resolve display name using cached names and optional channel alias
        let display_name = Self::resolve_name_from_guard(&guard, channel_id, alias);

        // 3. Deterministically evaluate action and discrete state transitions
        match detail {
            LiveDetail::Close { .. } => {
                let was_recording = match guard.channels.get(channel_id) {
                    Some(ChannelLifecycleState::Recording { .. }) => {
                        guard.transition_state(channel_id, ChannelLifecycleState::Idle);
                        true
                    }
                    Some(ChannelLifecycleState::Cooldown { finished_at, .. }) => {
                        if finished_at.elapsed() >= cooldown_window {
                            guard.transition_state(channel_id, ChannelLifecycleState::Idle);
                        }
                        false
                    }
                    _ => {
                        guard.transition_state(channel_id, ChannelLifecycleState::Idle);
                        false
                    }
                };

                PollAction::StreamClosed {
                    display_name,
                    was_recording,
                }
            }
            LiveDetail::Restricted {
                live_id,
                title,
                adult,
                ..
            } => {
                let reason = if *adult {
                    RestrictionReason::AgeRestricted
                } else {
                    RestrictionReason::RequiresCredentials
                };

                let is_newly_restricted = !matches!(
                    guard.channels.get(channel_id),
                    Some(ChannelLifecycleState::Restricted { reason: r, .. }) if *r == reason
                );

                guard.transition_state(
                    channel_id,
                    ChannelLifecycleState::Restricted {
                        live_id: *live_id,
                        reason: reason.clone(),
                    },
                );

                PollAction::Restricted {
                    display_name,
                    title: title.clone(),
                    reason,
                    is_newly_restricted,
                }
            }
            LiveDetail::Open(info) => match guard.channels.get_mut(channel_id) {
                Some(ChannelLifecycleState::Recording { session, .. }) => {
                    let current_live_id = session.current_metadata.live_id;
                    let is_different_broadcast = match (info.live_id, current_live_id) {
                        (Some(new_id), Some(old_id)) => new_id != old_id,
                        _ => false,
                    };

                    if is_different_broadcast {
                        guard.transition_state(channel_id, ChannelLifecycleState::Idle);
                        PollAction::ReadyToRecord {
                            info: info.clone(),
                            display_name,
                            was_api_restricted: false,
                        }
                    } else if let Some((delta, event)) =
                        session.record_metadata_change(info.metadata.clone())
                    {
                        let remote_dir = session.folder_name();
                        let full_jsonl = session.format_metadata_jsonl();
                        PollAction::RecordingMetadataChanged {
                            delta,
                            event,
                            remote_dir,
                            full_jsonl,
                            display_name,
                            title: info.title.clone(),
                        }
                    } else {
                        PollAction::AlreadyRecording {
                            display_name,
                            title: info.title.clone(),
                        }
                    }
                }
                Some(ChannelLifecycleState::Restricted {
                    live_id: restricted_live_id,
                    reason,
                }) => {
                    let is_same_live_id = match (info.live_id, *restricted_live_id) {
                        (Some(curr), Some(r)) => curr == r,
                        (None, None) => true,
                        _ => false,
                    };

                    if *reason == RestrictionReason::KeyForbidden && is_same_live_id {
                        PollAction::Restricted {
                            display_name,
                            title: info.title.clone(),
                            reason: reason.clone(),
                            is_newly_restricted: false,
                        }
                    } else {
                        let was_api_restricted =
                            reason.is_adult() || *reason == RestrictionReason::RequiresCredentials;
                        PollAction::ReadyToRecord {
                            info: info.clone(),
                            display_name,
                            was_api_restricted,
                        }
                    }
                }
                Some(ChannelLifecycleState::Cooldown {
                    live_id: previous_live_id,
                    finished_at,
                }) => {
                    let previous_live_id = *previous_live_id;
                    let finished_at = *finished_at;
                    match (&info.live_id, &previous_live_id) {
                        (Some(curr), Some(prev)) if curr != prev => PollAction::ReadyToRecord {
                            info: info.clone(),
                            display_name,
                            was_api_restricted: false,
                        },
                        _ => {
                            let elapsed = finished_at.elapsed();
                            if elapsed < cooldown_window {
                                let remaining = cooldown_window.saturating_sub(elapsed);
                                PollAction::InCooldown {
                                    display_name,
                                    live_id: info.live_id.or(previous_live_id),
                                    elapsed,
                                    remaining,
                                }
                            } else {
                                PollAction::ReadyToRecord {
                                    info: info.clone(),
                                    display_name,
                                    was_api_restricted: false,
                                }
                            }
                        }
                    }
                }
                Some(ChannelLifecycleState::Idle) | None => PollAction::ReadyToRecord {
                    info: info.clone(),
                    display_name,
                    was_api_restricted: false,
                },
            },
        }
    }
}
