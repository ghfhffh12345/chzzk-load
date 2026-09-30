use std::path::PathBuf;

pub mod backend;
pub mod rclone;

pub use backend::{BoxFuture, MockUploadBackend, ProgressCallback, UploadBackend};
pub use rclone::{RcloneBackend, RcloneStats, format_destination, parse_rclone_log_line};

/// Returns the broadcast identifier for logging, prioritizing the streamer/channel name
/// and falling back to channel_id if empty or whitespace-only.
#[inline]
pub fn broadcast_identifier<'a>(streamer_name: &'a str, channel_id: &'a str) -> &'a str {
    let trimmed = streamer_name.trim();
    if !trimmed.is_empty() {
        trimmed
    } else {
        channel_id
    }
}

/// Represents an upload task for a single video chunk or metadata file.
#[derive(Debug, Clone)]
pub struct UploadTask {
    pub channel_id: String,
    pub session_folder_id: String,
    pub remote_dir: String,
    pub chunk_path: PathBuf,
    pub chunk_name: String,
    pub streamer_name: String,
}

impl UploadTask {
    /// Returns the broadcast identifier for this upload task.
    #[inline]
    pub fn broadcast_identifier(&self) -> &str {
        broadcast_identifier(&self.streamer_name, &self.channel_id)
    }
}
