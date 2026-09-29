use std::path::PathBuf;

pub mod backend;
pub mod rclone;

pub use backend::{BoxFuture, MockUploadBackend, ProgressCallback, UploadBackend};
pub use rclone::{RcloneBackend, RcloneStats, format_destination, parse_rclone_log_line};

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
