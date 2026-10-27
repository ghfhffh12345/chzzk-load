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
    #[serde(default)]
    pub consolidate_extra_args: Vec<String>,
    #[serde(default = "default_skip_connection_check")]
    pub skip_connection_check: bool,
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
fn default_skip_connection_check() -> bool {
    false
}

impl Default for RcloneConfig {
    fn default() -> Self {
        Self {
            remote_path: default_remote_path(),
            upload_concurrency: default_upload_concurrency(),
            rclone_bin: default_rclone_bin(),
            extra_args: Vec::new(),
            consolidate_extra_args: Vec::new(),
            skip_connection_check: default_skip_connection_check(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub nid_aut: String,
    #[serde(default)]
    pub nid_ses: String,
}

impl AuthConfig {
    pub fn new(nid_aut: impl Into<String>, nid_ses: impl Into<String>) -> Self {
        Self {
            nid_aut: nid_aut.into(),
            nid_ses: nid_ses.into(),
        }
    }

    /// Formats the session credentials into a standard Cookie header string.
    /// Returns None if both credentials are empty or whitespace-only.
    pub fn to_cookie_header(&self) -> Option<String> {
        let aut = self.nid_aut.trim();
        let ses = self.nid_ses.trim();
        let has_aut = !aut.is_empty();
        let has_ses = !ses.is_empty();

        if has_aut && has_ses {
            Some(format!("NID_AUT={aut}; NID_SES={ses}"))
        } else if has_aut {
            Some(format!("NID_AUT={aut}"))
        } else if has_ses {
            Some(format!("NID_SES={ses}"))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ChzzkConfig {
    #[serde(default)]
    pub nid_aut: String,
    #[serde(default)]
    pub nid_ses: String,
}

impl From<AuthConfig> for ChzzkConfig {
    fn from(auth: AuthConfig) -> Self {
        Self {
            nid_aut: auth.nid_aut,
            nid_ses: auth.nid_ses,
        }
    }
}

impl From<&AuthConfig> for ChzzkConfig {
    fn from(auth: &AuthConfig) -> Self {
        Self {
            nid_aut: auth.nid_aut.clone(),
            nid_ses: auth.nid_ses.clone(),
        }
    }
}

impl From<ChzzkConfig> for AuthConfig {
    fn from(chzzk: ChzzkConfig) -> Self {
        Self {
            nid_aut: chzzk.nid_aut,
            nid_ses: chzzk.nid_ses,
        }
    }
}

impl From<&ChzzkConfig> for AuthConfig {
    fn from(chzzk: &ChzzkConfig) -> Self {
        Self {
            nid_aut: chzzk.nid_aut.clone(),
            nid_ses: chzzk.nid_ses.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Aria2cConfig {
    #[serde(default = "default_aria2c_bin")]
    pub aria2c_bin: String,
}

fn default_aria2c_bin() -> String {
    "aria2c".to_string()
}

impl Default for Aria2cConfig {
    fn default() -> Self {
        Self {
            aria2c_bin: default_aria2c_bin(),
        }
    }
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
# Skip rclone connection check at startup
skip_connection_check = false
# Additional arguments passed to rclone child process
extra_args = []
# Additional arguments passed specifically during consolidation cloud uploads
consolidate_extra_args = []

[auth]
# Optional Naver session cookies for age-restricted (19+) or subscriber streams / VODs
nid_aut = ""
nid_ses = ""

[chzzk]
# Optional Naver session cookies (legacy alias for [auth])
nid_aut = ""
nid_ses = ""

[aria2c]
# Path to aria2c binary for high-speed VOD downloads
aria2c_bin = "aria2c"

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
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub aria2c: Aria2cConfig,
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
            auth: AuthConfig::default(),
            aria2c: Aria2cConfig::default(),
            channels: default_channels(),
        }
    }
}

impl Settings {
    /// Resolves authentication credentials with optional CLI overrides.
    /// Priority: CLI flags > [auth] > [chzzk]
    pub fn resolve_auth(&self, cli_nid_aut: Option<&str>, cli_nid_ses: Option<&str>) -> AuthConfig {
        let nid_aut = match cli_nid_aut {
            Some(val) => val.trim().to_string(),
            None => {
                let s = self.auth.nid_aut.trim();
                if !s.is_empty() {
                    s.to_string()
                } else {
                    self.chzzk.nid_aut.trim().to_string()
                }
            }
        };

        let nid_ses = match cli_nid_ses {
            Some(val) => val.trim().to_string(),
            None => {
                let s = self.auth.nid_ses.trim();
                if !s.is_empty() {
                    s.to_string()
                } else {
                    self.chzzk.nid_ses.trim().to_string()
                }
            }
        };

        AuthConfig { nid_aut, nid_ses }
    }
    /// Loads and parses settings from a TOML file path without creating files if missing.
    pub fn load_from_path(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read settings from '{}'", path.display()))?;
        toml::from_str(&content)
            .with_context(|| format!("Failed to parse TOML in '{}'", path.display()))
    }

    /// Resolves configuration for consolidation.
    ///
    /// Priority:
    /// 1. Explicit path in `config_path` (e.g., from `--config <path>`)
    /// 2. Auto-detected `settings.toml` in current directory or executable directory
    /// 3. If neither exists, returns default settings (with empty `consolidate_extra_args` and default `rclone_bin`)
    pub fn resolve_for_consolidation(config_path: Option<&Path>) -> anyhow::Result<Self> {
        if let Some(custom) = config_path {
            let resolved = crate::app_path::resolve_path(custom);
            Self::load_from_path(&resolved)
        } else {
            let default_path = crate::app_path::resolve_path(Path::new("settings.toml"));
            if default_path.is_file() {
                Self::load_from_path(&default_path)
            } else {
                Ok(Self::default())
            }
        }
    }

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

    pub const SAMPLE_CHANNEL_ID: &'static str = "4c3b44869c9b1399723ec28ec236f736";

    /// Validates configuration values against minimum operational constraints.
    /// Returns Ok(warnings) if configuration is valid (with optional advisory warnings),
    /// or Err(errors) if fatal configuration errors are detected.
    pub fn validate(&self) -> Result<Vec<String>, Vec<String>> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        if self.general.chunk_duration_seconds < 10 {
            errors.push(format!(
                "general.chunk_duration_seconds must be at least 10 seconds (got {})",
                self.general.chunk_duration_seconds
            ));
        }

        if self.general.poll_interval_seconds < 1 {
            errors.push(format!(
                "general.poll_interval_seconds must be at least 1 second (got {})",
                self.general.poll_interval_seconds
            ));
        }

        if self.general.min_free_disk_gb < 0.1 && self.general.min_free_disk_gb > 0.0 {
            errors.push(format!(
                "general.min_free_disk_gb must be at least 0.1 GB or 0.0 to disable (got {})",
                self.general.min_free_disk_gb
            ));
        }

        if self.rclone.upload_concurrency < 1 {
            errors.push(format!(
                "rclone.upload_concurrency must be at least 1 (got {})",
                self.rclone.upload_concurrency
            ));
        }

        for ch in &self.channels {
            if ch.id == Self::SAMPLE_CHANNEL_ID {
                warnings.push(format!(
                    "Monitored channels contain default unconfigured sample channel ID '{}'. Please update settings.toml with your target streamer channel IDs.",
                    Self::SAMPLE_CHANNEL_ID
                ));
            }
            if ch.id.trim().is_empty() {
                errors.push("Channel ID cannot be empty".to_string());
            }
        }

        if errors.is_empty() {
            Ok(warnings)
        } else {
            Err(errors)
        }
    }
}
