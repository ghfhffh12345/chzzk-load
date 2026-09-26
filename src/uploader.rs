use std::path::{Path, PathBuf};

use crate::drive::client::DriveClient;

/// Represents an upload task for a single video chunk or metadata file.
#[derive(Debug, Clone)]
pub struct UploadTask {
    pub channel_id: String,
    pub session_folder_id: String,
    pub chunk_path: PathBuf,
    pub chunk_name: String,
    pub streamer_name: String,
}

/// Worker responsible for executing resumable file uploads and cleaning up local files.
pub struct UploadWorker;

impl UploadWorker {
    /// Uploads a file at `chunk_path` to Google Drive and deletes it locally upon confirmation.
    pub async fn upload_path_and_delete(
        client: &DriveClient,
        chunk_path: &Path,
        session_folder_id: &str,
        on_progress: impl Fn(u64, u64) + Send + Sync + 'static,
    ) -> anyhow::Result<u64> {
        let size = tokio::fs::metadata(chunk_path).await?.len();
        client
            .upload_file_resumable(chunk_path, session_folder_id, on_progress)
            .await?;
        // Deletion only runs upon successful upload confirmation
        tokio::fs::remove_file(chunk_path).await?;
        Ok(size)
    }

    /// Uploads the given [`UploadTask`] to Google Drive and deletes the file locally upon confirmation.
    pub async fn upload_and_delete(
        client: &DriveClient,
        task: UploadTask,
        on_progress: impl Fn(u64, u64) + Send + Sync + 'static,
    ) -> anyhow::Result<u64> {
        Self::upload_path_and_delete(
            client,
            &task.chunk_path,
            &task.session_folder_id,
            on_progress,
        )
        .await
    }
}
