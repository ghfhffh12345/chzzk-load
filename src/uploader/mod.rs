use crate::drive::client::DriveClient;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct UploadTask {
    pub channel_id: String,
    pub session_folder_id: String,
    pub chunk_path: PathBuf,
    pub chunk_name: String,
    pub streamer_name: String,
}

pub struct UploadWorker;

impl UploadWorker {
    pub async fn upload_and_delete(
        client: &DriveClient,
        task: UploadTask,
        on_progress: impl Fn(u64, u64) + Send + Sync + 'static,
    ) -> anyhow::Result<u64> {
        let size = tokio::fs::metadata(&task.chunk_path).await?.len();
        client
            .upload_file_resumable(&task.chunk_path, &task.session_folder_id, on_progress)
            .await?;
        // Deletion only runs upon successful upload confirmation
        tokio::fs::remove_file(&task.chunk_path).await?;
        Ok(size)
    }
}
