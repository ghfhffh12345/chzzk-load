use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneralConfig {
    #[serde(default = "default_chunk_duration")]
    pub chunk_duration_seconds: u64,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_stream_cooldown")]
    pub stream_cooldown_seconds: u64,
    #[serde(default = "default_recordings_dir")]
    pub recordings_dir: String,
    #[serde(default = "default_min_free_disk_gb")]
    pub min_free_disk_gb: f64,
    #[serde(default = "default_record_chat")]
    pub record_chat: bool,
    #[serde(default = "default_chat_flush_interval")]
    pub chat_flush_interval_seconds: u64,
}

fn default_chunk_duration() -> u64 {
    600
}
fn default_poll_interval() -> u64 {
    20
}
fn default_stream_cooldown() -> u64 {
    60
}
fn default_recordings_dir() -> String {
    "recordings".to_string()
}
fn default_min_free_disk_gb() -> f64 {
    2.0
}
fn default_record_chat() -> bool {
    true
}
fn default_chat_flush_interval() -> u64 {
    30
}

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            chunk_duration_seconds: default_chunk_duration(),
            poll_interval_seconds: default_poll_interval(),
            stream_cooldown_seconds: default_stream_cooldown(),
            recordings_dir: default_recordings_dir(),
            min_free_disk_gb: default_min_free_disk_gb(),
            record_chat: default_record_chat(),
            chat_flush_interval_seconds: default_chat_flush_interval(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RcloneConfig {
    #[serde(default = "default_remote_path")]
    pub remote_path: String,
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
    #[serde(default = "default_rclone_bin")]
    pub rclone_bin: String,
    #[serde(default)]
    pub extra_args: Vec<String>,
}

fn default_remote_path() -> String {
    "remote:chzzk".to_string()
}
fn default_upload_concurrency() -> usize {
    3
}
fn default_rclone_bin() -> String {
    "rclone".to_string()
}

impl Default for RcloneConfig {
    fn default() -> Self {
        Self {
            remote_path: default_remote_path(),
            upload_concurrency: default_upload_concurrency(),
            rclone_bin: default_rclone_bin(),
            extra_args: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ChzzkConfig {
    #[serde(default)]
    pub nid_aut: String,
    #[serde(default)]
    pub nid_ses: String,
}

pub const DEFAULT_SETTINGS_TOML: &str = r#"# chzzk-load configuration

[general]
# Chunk duration for MPEG-TS video segments in seconds
chunk_duration_seconds = 600
# Interval between stream polling cycles in seconds
poll_interval_seconds = 20
# Cooldown window in seconds to prevent duplicate sessions from CDN caching
stream_cooldown_seconds = 60
# Directory to store local session recordings
recordings_dir = "recordings"
# Minimum required free disk space in GB before pausing/warning
min_free_disk_gb = 2.0
# Record live chat messages concurrently into JSON Lines chunks
record_chat = true
# Buffer flush interval for chat writer in seconds
chat_flush_interval_seconds = 30

[rclone]
# Target remote path (e.g. "remote:chzzk" or "" for local-only mode)
remote_path = "remote:chzzk"
# Maximum concurrent uploads across different channels
upload_concurrency = 3
# Path to rclone binary
rclone_bin = "rclone"
# Additional arguments passed to rclone child process
extra_args = []

[chzzk]
# Optional Naver session cookies for age-restricted (19+) or subscriber streams
nid_aut = ""
nid_ses = ""

# Monitored Channels:
# Channels can be specified as a list of strings (shorthand ID) or an array of tables.
#
# Option A: Shorthand string array:
# channels = ["4c3b44869c9b1399723ec28ec236f736"]
#
# Option B: Array of tables with optional alias:
[[channels]]
id = "4c3b44869c9b1399723ec28ec236f736"
alias = "SampleStreamer"
"#;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChannelConfig {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl ChannelConfig {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            alias: None,
        }
    }

    pub fn with_alias(id: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            alias: Some(alias.into()),
        }
    }

    pub fn display_label(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.id)
    }
}

impl<'de> serde::Deserialize<'de> for ChannelConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum RawChannel {
            Id(String),
            Table {
                id: String,
                #[serde(default)]
                alias: Option<String>,
            },
        }

        match RawChannel::deserialize(deserializer)? {
            RawChannel::Id(id) => Ok(ChannelConfig::new(id)),
            RawChannel::Table { id, alias } => Ok(ChannelConfig { id, alias }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub rclone: RcloneConfig,
    #[serde(default)]
    pub chzzk: ChzzkConfig,
    #[serde(default = "default_channels")]
    pub channels: Vec<ChannelConfig>,
}

fn default_channels() -> Vec<ChannelConfig> {
    vec![ChannelConfig::with_alias(
        "4c3b44869c9b1399723ec28ec236f736",
        "SampleStreamer",
    )]
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            general: GeneralConfig::default(),
            rclone: RcloneConfig::default(),
            chzzk: ChzzkConfig::default(),
            channels: default_channels(),
        }
    }
}

impl Settings {
    pub fn load_or_create_default(path: &Path) -> anyhow::Result<Self> {
        match fs::read_to_string(path) {
            Ok(content) => {
                let settings: Settings = toml::from_str(&content)
                    .with_context(|| format!("Failed to parse TOML in {}", path.display()))?;
                Ok(settings)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("Failed to create parent directory for {}", parent.display())
                    })?;
                }
                fs::write(path, DEFAULT_SETTINGS_TOML).with_context(|| {
                    format!("Failed to write default settings to {}", path.display())
                })?;
                let settings: Settings = toml::from_str(DEFAULT_SETTINGS_TOML)
                    .with_context(|| "Failed to parse built-in default settings template")?;
                Ok(settings)
            }
            Err(e) => {
                Err(e).with_context(|| format!("Failed to read settings from {}", path.display()))
            }
        }
    }
}
