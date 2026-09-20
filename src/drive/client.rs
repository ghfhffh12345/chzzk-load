use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use crate::drive::auth::DriveAuth;

pub fn build_resumable_init_body(filename: &str, parent_id: Option<&str>) -> String {
    let parents = match parent_id {
        Some(id) if !id.is_empty() => vec![id.to_string()],
        _ => vec![],
    };
    serde_json::json!({
        "name": filename,
        "parents": parents
    }).to_string()
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
        Self {
            auth,
            client: reqwest::Client::new(),
            base_url: "https://www.googleapis.com".to_string(),
            upload_base_url: "https://www.googleapis.com".to_string(),
        }
    }

    pub fn with_base_urls(mut self, base_url: impl Into<String>, upload_base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self.upload_base_url = upload_base_url.into();
        self
    }

    pub async fn get_or_create_folder(&self, folder_name: &str, parent_id: Option<&str>) -> Result<String> {
        let token = self.auth.get_valid_access_token().await?;

        // Query if exists
        let mut query = format!("mimeType = 'application/vnd.google-apps.folder' and name = '{}' and trashed = false", folder_name);
        if let Some(pid) = parent_id.filter(|p| !p.is_empty()) {
            query.push_str(&format!(" and '{}' in parents", pid));
        }

        let url = format!("{}/drive/v3/files", self.base_url);
        let resp: DriveFileList = self.client.get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("q", query.as_str()), ("fields", "files(id, name)")])
            .send().await?
            .error_for_status()?
            .json().await?;

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

        let created: DriveFileItem = self.client.post(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(meta.to_string())
            .send().await?
            .error_for_status()?
            .json().await?;

        Ok(created.id)
    }

    pub async fn upload_file_resumable<F>(
        &self,
        file_path: &Path,
        parent_folder_id: &str,
        progress_cb: F,
    ) -> Result<String>
    where
        F: Fn(u64, u64) + Send + 'static,
    {
        let filename = file_path.file_name()
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
        let init_url = format!("{}/upload/drive/v3/files?uploadType=resumable", self.upload_base_url);
        let init_resp = self.client.post(&init_url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header("X-Upload-Content-Type", "video/mp2t")
            .header("X-Upload-Content-Length", file_size.to_string())
            .header(CONTENT_TYPE, "application/json; charset=UTF-8")
            .body(init_body)
            .send().await?
            .error_for_status()?;

        let location = init_resp.headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| anyhow!("Missing Location header in Google Drive resumable init"))?
            .to_string();

        // 2. Stream chunk with progress
        let file = File::open(file_path).await?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let mut uploaded = 0u64;

        let progress_stream = stream.map(move |chunk_result| {
            if let Ok(ref bytes) = chunk_result {
                uploaded += bytes.len() as u64;
                progress_cb(uploaded, file_size);
            }
            chunk_result
        });

        let upload_resp = self.client.put(&location)
            .header(CONTENT_LENGTH, file_size.to_string())
            .header(CONTENT_TYPE, "video/mp2t")
            .body(reqwest::Body::wrap_stream(progress_stream))
            .send().await?
            .error_for_status()?;

        let created: DriveFileItem = upload_resp.json().await?;
        Ok(created.id)
    }
}
