use std::path::PathBuf;

pub mod backend;
pub mod rclone;
pub mod worker;

pub use backend::{BoxFuture, MockUploadBackend, ProgressCallback, UploadBackend};
pub use rclone::{RcloneBackend, RcloneStats, format_destination, parse_rclone_log_line};
pub use worker::{
    DiskSpaceProvider, DlqConfig, PairedChunkPaths, UploadWorker, resolve_paired_paths,
    unlink_local_file_with_retry,
};

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
    pub delete_on_success: bool,
}

impl UploadTask {
    /// Creates a media chunk upload task (video segment `.ts` or chat `.jsonl`),
    /// defaulting `delete_on_success` to `true`.
    pub fn chunk(
        channel_id: impl Into<String>,
        session_folder_id: impl Into<String>,
        remote_dir: impl Into<String>,
        chunk_path: impl Into<PathBuf>,
        chunk_name: impl Into<String>,
        streamer_name: impl Into<String>,
    ) -> Self {
        Self {
            channel_id: channel_id.into(),
            session_folder_id: session_folder_id.into(),
            remote_dir: remote_dir.into(),
            chunk_path: chunk_path.into(),
            chunk_name: chunk_name.into(),
            streamer_name: streamer_name.into(),
            delete_on_success: true,
        }
    }

    /// Creates a metadata snapshot upload task (`metadata.jsonl`),
    /// with an explicit `delete_on_success` retention policy (`false` for live sync,
    /// `true` for stream conclusion or crash recovery).
    pub fn metadata(
        channel_id: impl Into<String>,
        session_folder_id: impl Into<String>,
        remote_dir: impl Into<String>,
        chunk_path: impl Into<PathBuf>,
        streamer_name: impl Into<String>,
        delete_on_success: bool,
    ) -> Self {
        Self {
            channel_id: channel_id.into(),
            session_folder_id: session_folder_id.into(),
            remote_dir: remote_dir.into(),
            chunk_path: chunk_path.into(),
            chunk_name: "metadata.jsonl".to_string(),
            streamer_name: streamer_name.into(),
            delete_on_success,
        }
    }

    /// Returns true if this upload task represents the broadcast metadata snapshot (`metadata.jsonl`).
    #[inline]
    pub fn is_metadata(&self) -> bool {
        self.chunk_name == "metadata.jsonl"
    }

    /// Returns the broadcast identifier for this upload task.
    #[inline]
    pub fn broadcast_identifier(&self) -> &str {
        broadcast_identifier(&self.streamer_name, &self.channel_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_task_chunk_constructor() {
        let task = UploadTask::chunk(
            "chan_1",
            "session_1",
            "remote/dir",
            "recordings/chunk_0001.ts",
            "chunk_0001.ts",
            "Streamer A",
        );
        assert_eq!(task.channel_id, "chan_1");
        assert_eq!(task.session_folder_id, "session_1");
        assert_eq!(task.remote_dir, "remote/dir");
        assert_eq!(task.chunk_path, PathBuf::from("recordings/chunk_0001.ts"));
        assert_eq!(task.chunk_name, "chunk_0001.ts");
        assert_eq!(task.streamer_name, "Streamer A");
        assert!(
            task.delete_on_success,
            "media chunk must default delete_on_success to true"
        );
    }

    #[test]
    fn test_upload_task_metadata_constructor() {
        let live_sync_task = UploadTask::metadata(
            "chan_2",
            "session_2",
            "remote/dir",
            "recordings/metadata.jsonl",
            "Streamer B",
            false,
        );
        assert_eq!(live_sync_task.chunk_name, "metadata.jsonl");
        assert!(
            !live_sync_task.delete_on_success,
            "live sync metadata must have delete_on_success false"
        );

        let teardown_task = UploadTask::metadata(
            "chan_2",
            "session_2",
            "remote/dir",
            "recordings/metadata.jsonl",
            "Streamer B",
            true,
        );
        assert_eq!(teardown_task.chunk_name, "metadata.jsonl");
        assert!(
            teardown_task.delete_on_success,
            "teardown metadata must have delete_on_success true"
        );
    }
}
