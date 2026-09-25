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
pub struct GoogleDriveConfig {
    #[serde(default = "default_credentials_path")]
    pub credentials_path: String,
    #[serde(default = "default_token_path")]
    pub token_path: String,
    #[serde(default = "default_root_folder")]
    pub root_folder_name: String,
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
}

fn default_credentials_path() -> String {
    "credentials.json".to_string()
}
fn default_token_path() -> String {
    "token.json".to_string()
}
fn default_root_folder() -> String {
    "Chzzk_Recordings".to_string()
}
fn default_upload_concurrency() -> usize {
    3
}

impl Default for GoogleDriveConfig {
    fn default() -> Self {
        Self {
            credentials_path: default_credentials_path(),
            token_path: default_token_path(),
            root_folder_name: default_root_folder(),
            upload_concurrency: default_upload_concurrency(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelConfig {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Settings {
    #[serde(default)]
    pub general: GeneralConfig,
    #[serde(default)]
    pub google_drive: GoogleDriveConfig,
    #[serde(default)]
    pub chzzk: ChzzkConfig,
    #[serde(default = "default_channels")]
    pub channels: Vec<ChannelConfig>,
}

fn default_channels() -> Vec<ChannelConfig> {
    vec![ChannelConfig {
        id: "4c3b44869c9b1399723ec28ec236f736".to_string(),
        name: "SampleStreamer".to_string(),
    }]
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            general: GeneralConfig::default(),
            google_drive: GoogleDriveConfig::default(),
            chzzk: ChzzkConfig::default(),
            channels: default_channels(),
        }
    }
}

impl Settings {
    pub fn load_or_create_default(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read settings from {}", path.display()))?;
            let settings: Settings = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse JSON in {}", path.display()))?;
            Ok(settings)
        } else {
            let settings = Settings::default();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create parent directory for {}", parent.display())
                })?;
            }
            let content = serde_json::to_string_pretty(&settings)?;
            fs::write(path, content).with_context(|| {
                format!("Failed to write default settings to {}", path.display())
            })?;
            Ok(settings)
        }
    }
}
