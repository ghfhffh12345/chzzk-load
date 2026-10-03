use serde::{Deserialize, Deserializer, Serialize};

/// Mutually exclusive access gating tier required to view the live broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamAccessTier {
    #[default]
    Public,
    AdultOnly,
    CheatKey,
    ChannelSubscription,
    NaverPlus,
    PayPerView,
}

/// Broad category grouping, forward-compatible with future additions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CategoryType {
    Game,
    Sports,
    Etc,
    Talk,
    #[serde(other)]
    Unknown,
}

/// Official co-streaming and watch-along state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchPartyState {
    pub is_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub party_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paid_product_id: Option<serde_json::Value>,
}

/// Geo-blocking, moderation enforcement, and playback feature flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BroadcastPolicies {
    pub kr_only_viewing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub playable_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blind_type: Option<String>,
    pub time_machine_active: bool,
    pub clip_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tv_app_viewing_policy_type: Option<String>,
}

/// Channel chat interaction rules and access requirements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatRulesState {
    #[serde(default = "default_true")]
    pub chat_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_available_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_available_condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_follower_minute: Option<u32>,
    pub allow_subscriber_in_follower_mode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_slow_mode_sec: Option<u32>,
    pub chat_emoji_mode: bool,
    #[serde(default = "default_true")]
    pub chat_donation_ranking_exposure: bool,
}

const fn default_true() -> bool {
    true
}

impl Default for ChatRulesState {
    fn default() -> Self {
        Self {
            chat_active: true,
            chat_available_group: None,
            chat_available_condition: None,
            min_follower_minute: None,
            allow_subscriber_in_follower_mode: false,
            chat_slow_mode_sec: None,
            chat_emoji_mode: false,
            chat_donation_ranking_exposure: true,
        }
    }
}

/// Complete normalized snapshot of broadcast state at a specific point in time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMetadataState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_date: Option<String>,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default, alias = "streamer_name")]
    pub channel_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_image_url: Option<String>,
    #[serde(default)]
    pub verified_mark: bool,
    #[serde(default, alias = "title")]
    pub live_title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_type: Option<CategoryType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category_value: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub access_tier: StreamAccessTier,
    #[serde(default)]
    pub policies: BroadcastPolicies,
    #[serde(default)]
    pub watch_party: WatchPartyState,
    #[serde(default)]
    pub chat_rules: ChatRulesState,
    #[serde(default)]
    pub paid_promotion: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drops_campaign_no: Option<String>,
    #[serde(default)]
    pub log_power_active: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "live_image_url"
    )]
    pub live_thumbnail_image_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_thumbnail_image_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrent_user_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accumulate_count: Option<u64>,
}

/// Lean stream metadata state snapshot (version 2).
///
/// Contains essential broadcast identifiers, lifecycles, classification, and
/// flattened boolean flags (`is_kr_only`, `is_chat_active`, `is_watch_party`,
/// `paid_promotion`, `drops_campaign_no`). Volatile viewer counters, CDN image URLs,
/// and marginal flags are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMetadataStateV2 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_date: Option<String>,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default, alias = "streamer_name")]
    pub channel_name: String,
    #[serde(default, alias = "title")]
    pub live_title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_type: Option<CategoryType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_category_value: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub access_tier: StreamAccessTier,
    #[serde(default)]
    pub is_kr_only: bool,
    #[serde(default = "default_true")]
    pub is_chat_active: bool,
    #[serde(default)]
    pub is_watch_party: bool,
    #[serde(default)]
    pub paid_promotion: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drops_campaign_no: Option<String>,
}

impl Default for StreamMetadataStateV2 {
    fn default() -> Self {
        Self {
            live_id: None,
            open_date: None,
            close_date: None,
            channel_id: String::new(),
            channel_name: String::new(),
            live_title: String::new(),
            category_type: None,
            live_category: None,
            live_category_value: None,
            tags: Vec::new(),
            access_tier: StreamAccessTier::Public,
            is_kr_only: false,
            is_chat_active: true,
            is_watch_party: false,
            paid_promotion: false,
            drops_campaign_no: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataEventType {
    InitialState,
    MetadataChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDiff<T> {
    pub old: T,
    pub new: T,
}

impl<T> FieldDiff<T> {
    pub fn new(old: T, new: T) -> Self {
        Self { old, new }
    }
}

/// Detailed field-level diff between state transitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_title: Option<FieldDiff<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_name: Option<FieldDiff<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_mark: Option<FieldDiff<bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category_type: Option<FieldDiff<Option<CategoryType>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_category_value: Option<FieldDiff<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_category: Option<FieldDiff<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<FieldDiff<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_tier: Option<FieldDiff<StreamAccessTier>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<FieldDiff<BroadcastPolicies>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_party: Option<FieldDiff<WatchPartyState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_rules: Option<FieldDiff<ChatRulesState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_promotion: Option<FieldDiff<bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drops_campaign_no: Option<FieldDiff<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_power_active: Option<FieldDiff<bool>>,
}

impl MetadataDelta {
    pub fn is_empty(&self) -> bool {
        self.live_title.is_none()
            && self.channel_name.is_none()
            && self.verified_mark.is_none()
            && self.category_type.is_none()
            && self.live_category_value.is_none()
            && self.live_category.is_none()
            && self.tags.is_none()
            && self.access_tier.is_none()
            && self.policies.is_none()
            && self.watch_party.is_none()
            && self.chat_rules.is_none()
            && self.paid_promotion.is_none()
            && self.drops_campaign_no.is_none()
            && self.log_power_active.is_none()
    }
}

/// A single JSON Lines record in `metadata.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEvent {
    pub version: u8,
    pub event: MetadataEventType,
    pub timestamp: String,
    pub time_local: String,
    pub stream_offset_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<MetadataDelta>,
    pub state: StreamMetadataState,
}

const fn default_version_2() -> u8 {
    2
}

/// A version 2 JSON Lines record in `metadata.jsonl`.
///
/// Contains an RFC 3339 UTC timestamp, monotonic stream offset in milliseconds,
/// and a full snapshot of the lean metadata state. Redundant local timestamps (`time_local`)
/// and delta diff objects (`changes`) are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEventV2 {
    #[serde(default = "default_version_2")]
    pub version: u8,
    pub event: MetadataEventType,
    pub timestamp: String,
    pub stream_offset_ms: u64,
    pub state: StreamMetadataStateV2,
}

pub type LeanMetadataEvent = MetadataEventV2;

impl MetadataEventV2 {
    pub fn new(
        event: MetadataEventType,
        timestamp: String,
        stream_offset_ms: u64,
        state: StreamMetadataStateV2,
    ) -> Self {
        Self {
            version: 2,
            event,
            timestamp,
            stream_offset_ms,
            state,
        }
    }

    pub fn initial(timestamp: String, state: StreamMetadataStateV2) -> Self {
        Self::new(MetadataEventType::InitialState, timestamp, 0, state)
    }

    pub fn changed(timestamp: String, stream_offset_ms: u64, state: StreamMetadataStateV2) -> Self {
        Self::new(
            MetadataEventType::MetadataChanged,
            timestamp,
            stream_offset_ms,
            state,
        )
    }

    /// Serializes the event as a newline-terminated JSON Lines record.
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        let mut line = serde_json::to_string(self)?;
        line.push('\n');
        Ok(line)
    }
}

impl From<&StreamMetadataState> for StreamMetadataStateV2 {
    fn from(v1: &StreamMetadataState) -> Self {
        Self {
            live_id: v1.live_id,
            open_date: v1.open_date.clone(),
            close_date: v1.close_date.clone(),
            channel_id: v1.channel_id.clone(),
            channel_name: v1.channel_name.clone(),
            live_title: v1.live_title.clone(),
            category_type: v1.category_type.clone(),
            live_category: v1.live_category.clone(),
            live_category_value: v1.live_category_value.clone(),
            tags: v1.tags.clone(),
            access_tier: v1.access_tier,
            is_kr_only: v1.policies.kr_only_viewing,
            is_chat_active: v1.chat_rules.chat_active,
            is_watch_party: v1.watch_party.is_active,
            paid_promotion: v1.paid_promotion,
            drops_campaign_no: v1.drops_campaign_no.clone(),
        }
    }
}

impl From<StreamMetadataState> for StreamMetadataStateV2 {
    fn from(v1: StreamMetadataState) -> Self {
        (&v1).into()
    }
}

impl From<&MetadataEvent> for MetadataEventV2 {
    fn from(v1: &MetadataEvent) -> Self {
        Self {
            version: 2,
            event: v1.event,
            timestamp: v1.timestamp.clone(),
            stream_offset_ms: v1.stream_offset_ms,
            state: (&v1.state).into(),
        }
    }
}

impl From<MetadataEvent> for MetadataEventV2 {
    fn from(v1: MetadataEvent) -> Self {
        (&v1).into()
    }
}

impl StreamMetadataState {
    /// Computes discrete field diffs between self (old) and incoming (new).
    /// Telemetry churn (viewers, thumbnails) is ignored and will not generate a delta.
    pub fn compute_delta(&self, new: &Self) -> Option<MetadataDelta> {
        let mut delta = MetadataDelta::default();
        let mut changed = false;

        if self.live_title != new.live_title {
            delta.live_title = Some(FieldDiff::new(
                self.live_title.clone(),
                new.live_title.clone(),
            ));
            changed = true;
        }
        if self.channel_name != new.channel_name {
            delta.channel_name = Some(FieldDiff::new(
                self.channel_name.clone(),
                new.channel_name.clone(),
            ));
            changed = true;
        }
        if self.verified_mark != new.verified_mark {
            delta.verified_mark = Some(FieldDiff::new(self.verified_mark, new.verified_mark));
            changed = true;
        }
        if self.category_type != new.category_type {
            delta.category_type = Some(FieldDiff::new(
                self.category_type.clone(),
                new.category_type.clone(),
            ));
            changed = true;
        }
        if self.live_category_value != new.live_category_value {
            delta.live_category_value = Some(FieldDiff::new(
                self.live_category_value.clone(),
                new.live_category_value.clone(),
            ));
            changed = true;
        }
        if self.live_category != new.live_category {
            delta.live_category = Some(FieldDiff::new(
                self.live_category.clone(),
                new.live_category.clone(),
            ));
            changed = true;
        }
        if self.tags != new.tags {
            delta.tags = Some(FieldDiff::new(self.tags.clone(), new.tags.clone()));
            changed = true;
        }
        if self.access_tier != new.access_tier {
            delta.access_tier = Some(FieldDiff::new(self.access_tier, new.access_tier));
            changed = true;
        }
        if self.policies != new.policies {
            delta.policies = Some(FieldDiff::new(self.policies.clone(), new.policies.clone()));
            changed = true;
        }
        if self.watch_party != new.watch_party {
            delta.watch_party = Some(FieldDiff::new(
                self.watch_party.clone(),
                new.watch_party.clone(),
            ));
            changed = true;
        }
        if self.chat_rules != new.chat_rules {
            delta.chat_rules = Some(FieldDiff::new(
                self.chat_rules.clone(),
                new.chat_rules.clone(),
            ));
            changed = true;
        }
        if self.paid_promotion != new.paid_promotion {
            delta.paid_promotion = Some(FieldDiff::new(self.paid_promotion, new.paid_promotion));
            changed = true;
        }
        if self.drops_campaign_no != new.drops_campaign_no {
            delta.drops_campaign_no = Some(FieldDiff::new(
                self.drops_campaign_no.clone(),
                new.drops_campaign_no.clone(),
            ));
            changed = true;
        }
        if self.log_power_active != new.log_power_active {
            delta.log_power_active =
                Some(FieldDiff::new(self.log_power_active, new.log_power_active));
            changed = true;
        }

        if changed { Some(delta) } else { None }
    }
}

pub fn deserialize_optional_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrNum {
        Str(String),
        Int(i64),
        UInt(u64),
        Float(f64),
    }

    match Option::<StrOrNum>::deserialize(deserializer)? {
        Some(StrOrNum::Str(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(StrOrNum::Int(i)) => Ok(Some(i.to_string())),
        Some(StrOrNum::UInt(u)) => Ok(Some(u.to_string())),
        Some(StrOrNum::Float(f)) => Ok(Some(f.to_string())),
        None => Ok(None),
    }
}
