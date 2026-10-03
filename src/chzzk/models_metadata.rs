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

const fn default_true() -> bool {
    true
}

const fn default_metadata_version() -> u8 {
    2
}

/// Lean stream metadata state snapshot (version 2).
///
/// Contains essential broadcast identifiers, lifecycles, classification, and
/// flattened boolean flags (`is_kr_only`, `is_chat_active`, `is_watch_party`,
/// `paid_promotion`, `drops_campaign_no`). Volatile viewer counters, CDN image URLs,
/// and marginal flags are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Default for StreamMetadataState {
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

pub type StreamMetadataStateV2 = StreamMetadataState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MetadataEventType {
    InitialState,
    MetadataChanged,
}

/// A version 2 JSON Lines record in `metadata.jsonl`.
///
/// Contains an RFC 3339 UTC timestamp, monotonic stream offset in milliseconds,
/// and a full snapshot of the lean metadata state. Redundant local timestamps (`time_local`)
/// and delta diff objects (`changes`) are omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEvent {
    #[serde(default = "default_metadata_version")]
    pub version: u8,
    pub event: MetadataEventType,
    pub timestamp: String,
    pub stream_offset_ms: u64,
    pub state: StreamMetadataState,
}

pub type MetadataEventV2 = MetadataEvent;

impl MetadataEvent {
    pub fn new(
        event: MetadataEventType,
        timestamp: String,
        stream_offset_ms: u64,
        state: StreamMetadataState,
    ) -> Self {
        Self {
            version: 2,
            event,
            timestamp,
            stream_offset_ms,
            state,
        }
    }

    pub fn initial(timestamp: String, state: StreamMetadataState) -> Self {
        Self::new(MetadataEventType::InitialState, timestamp, 0, state)
    }

    pub fn changed(timestamp: String, stream_offset_ms: u64, state: StreamMetadataState) -> Self {
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
