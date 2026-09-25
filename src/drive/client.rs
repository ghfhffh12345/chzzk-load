use crate::drive::auth::DriveAuth;
use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

pub fn build_resumable_init_body(filename: &str, parent_id: Option<&str>) -> String {
    let parents = match parent_id {
        Some(id) if !id.is_empty() => vec![id.to_string()],
        _ => vec![],
    };
    serde_json::json!({
        "name": filename,
        "parents": parents
    })
    .to_string()
}

#[derive(Debug, Deserialize)]
pub struct DriveFileList {
    pub files: Vec<DriveFileItem>,
}

#[derive(Debug, Deserialize)]
pub struct DriveFileItem {
    pub id: String,
    pub name: String,
}

#[derive(Clone)]
pub struct DriveClient {
    auth: Arc<DriveAuth>,
    client: reqwest::Client,
    base_url: String,
    upload_base_url: String,
}

impl DriveClient {
    pub fn new(auth: Arc<DriveAuth>) -> Self {
        let client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            auth,
            client,
            base_url: "https://www.googleapis.com".to_string(),
            upload_base_url: "https://www.googleapis.com".to_string(),
        }
    }

    pub fn with_base_urls(
        mut self,
        base_url: impl Into<String>,
        upload_base_url: impl Into<String>,
    ) -> Self {
        self.base_url = base_url.into();
        self.upload_base_url = upload_base_url.into();
        self
    }

    pub async fn get_or_create_folder(
        &self,
        folder_name: &str,
        parent_id: Option<&str>,
    ) -> Result<String> {
        let token = self.auth.get_valid_access_token().await?;

        // Query if exists
        let escaped_name = folder_name.replace('\\', "\\\\").replace('\'', "\\'");
        let mut query = format!(
            "mimeType = 'application/vnd.google-apps.folder' and name = '{}' and trashed = false",
            escaped_name
        );
        if let Some(pid) = parent_id.filter(|p| !p.is_empty()) {
            query.push_str(&format!(" and '{}' in parents", pid));
        }

        let url = format!("{}/drive/v3/files", self.base_url);
        let resp: DriveFileList = self
            .client
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("q", query.as_str()), ("fields", "files(id, name)")])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if let Some(first) = resp.files.first() {
            return Ok(first.id.clone());
        }

        // Create folder
        let mut meta = serde_json::json!({
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder"
        });
        if let Some(pid) = parent_id.filter(|p| !p.is_empty()) {
            meta["parents"] = serde_json::json!([pid]);
        }

        let created: DriveFileItem = self
            .client
            .post(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(meta.to_string())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(created.id)
    }

    pub async fn rename_folder(&self, folder_id: &str, new_name: &str) -> Result<()> {
        let token = self.auth.get_valid_access_token().await?;
        let url = format!("{}/drive/v3/files/{}", self.base_url, folder_id);

        let body = serde_json::json!({
            "name": new_name
        });

        self.client
            .patch(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(body.to_string())
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn upload_file_resumable<F>(
        &self,
        file_path: &Path,
        parent_folder_id: &str,
        progress_cb: F,
    ) -> Result<String>
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        let filename = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        let meta = tokio::fs::metadata(file_path).await?;
        let file_size = meta.len();

        let token = self.auth.get_valid_access_token().await?;

        // 1. Initiate resumable upload
        let parent_opt = if parent_folder_id.is_empty() {
            None
        } else {
            Some(parent_folder_id)
        };
        let init_body = build_resumable_init_body(filename, parent_opt);
        let init_url = format!(
            "{}/upload/drive/v3/files?uploadType=resumable",
            self.upload_base_url
        );
        let init_resp = self
            .client
            .post(&init_url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header("X-Upload-Content-Type", "video/mp2t")
            .header("X-Upload-Content-Length", file_size.to_string())
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(init_body)
            .send()
            .await?
            .error_for_status()?;

        let location = init_resp
            .headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| anyhow!("Missing Location header in Google Drive resumable init"))?
            .to_string();

        let content_range = if file_size == 0 {
            "bytes */0".to_string()
        } else {
            format!("bytes 0-{}/{}", file_size - 1, file_size)
        };

        let progress_cb = Arc::new(progress_cb);
        let max_retries = 3;
        let mut last_error = None;

        // 2. Stream chunk with progress using 256KB buffer, retrying transient errors
        for attempt in 0..=max_retries {
            if attempt > 0 {
                let backoff_ms = 50 * (1 << (attempt - 1));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            let file = match File::open(file_path).await {
                Ok(f) => f,
                Err(e) => return Err(anyhow!("Failed to open file for upload: {}", e)),
            };

            let stream = FramedRead::with_capacity(file, BytesCodec::new(), 256 * 1024);
            let mut uploaded = 0u64;
            let cb = Arc::clone(&progress_cb);

            let progress_stream = stream.map(move |chunk_result| {
                if let Ok(ref bytes) = chunk_result {
                    uploaded += bytes.len() as u64;
                    cb(uploaded, file_size);
                }
                chunk_result
            });

            let upload_res = self
                .client
                .put(&location)
                .header(CONTENT_LENGTH, file_size.to_string())
                .header(CONTENT_TYPE, "video/mp2t")
                .header("Content-Range", &content_range)
                .body(reqwest::Body::wrap_stream(progress_stream))
                .send()
                .await;

            match upload_res {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let created: DriveFileItem = resp.json().await?;
                        return Ok(created.id);
                    } else if status.as_u16() == 429 || status.is_server_error() {
                        let err_msg = format!("HTTP error: status {}", status);
                        last_error = Some(anyhow!(err_msg));
                        continue;
                    } else {
                        let err = resp.error_for_status().unwrap_err();
                        return Err(anyhow!(err));
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow!(e));
                    continue;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Upload failed after retries")))
    }
}
