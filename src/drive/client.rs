use crate::drive::auth::DriveAuth;
use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::sync::RwLock;
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

/// Streaming buffer size for resumable Google Drive uploads.
/// Must be an exact multiple of 256 KiB (262,144 bytes) to comply with Google Drive API.
/// 8 MiB (32 * 256 KiB) minimizes syscall, context switching, and TLS framing overhead.
pub const RESUMABLE_UPLOAD_BUFFER_SIZE: usize = 8 * 1024 * 1024;

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
    root_folder_cache: Arc<RwLock<HashMap<String, String>>>,
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
            root_folder_cache: Arc::new(RwLock::new(HashMap::new())),
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

    pub async fn send_with_retry<F, Fut>(&self, mut req_builder: F) -> Result<reqwest::Response>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
    {
        let max_retries = 3;
        let mut last_error = None;
        for attempt in 0..=max_retries {
            if attempt > 0 {
                let backoff_ms = 100 * (1 << (attempt - 1));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
            match req_builder().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    } else if status.as_u16() == 429 || status.is_server_error() {
                        let err_msg = format!("HTTP error: status {status}");
                        last_error = Some(anyhow!(err_msg));
                        continue;
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow!(e));
                    continue;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("Request failed after retries")))
    }

    pub async fn get_or_create_folder(
        &self,
        folder_name: &str,
        parent_id: Option<&str>,
    ) -> Result<String> {
        let is_root = parent_id.is_none() || parent_id == Some("");
        if is_root {
            let cache = self.root_folder_cache.read().await;
            if let Some(id) = cache.get(folder_name) {
                return Ok(id.clone());
            }
        }

        let token = self.auth.get_valid_access_token().await?;

        // Query if exists
        let escaped_name = folder_name.replace('\\', "\\\\").replace('\'', "\\'");
        let mut query = format!(
            "mimeType = 'application/vnd.google-apps.folder' and name = '{escaped_name}' and trashed = false"
        );
        if let Some(pid) = parent_id.filter(|p| !p.is_empty()) {
            query.push_str(&format!(" and '{pid}' in parents"));
        }

        let url = format!("{}/drive/v3/files", self.base_url);
        let resp_res = self
            .send_with_retry(|| {
                let token = token.clone();
                let url = url.clone();
                let query = query.clone();
                async move {
                    self.client
                        .get(&url)
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .query(&[("q", query.as_str()), ("fields", "files(id, name)")])
                        .send()
                        .await
                }
            })
            .await?;

        let resp: DriveFileList = resp_res.error_for_status()?.json().await?;

        if let Some(first) = resp.files.first() {
            if is_root {
                self.root_folder_cache
                    .write()
                    .await
                    .insert(folder_name.to_string(), first.id.clone());
            }
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
        let meta_str = meta.to_string();

        let created_res = self
            .send_with_retry(|| {
                let token = token.clone();
                let url = url.clone();
                let meta_str = meta_str.clone();
                async move {
                    self.client
                        .post(&url)
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .header(CONTENT_TYPE, "application/json; charset=UTF-8")
                        .body(meta_str)
                        .send()
                        .await
                }
            })
            .await?;

        let created: DriveFileItem = created_res.error_for_status()?.json().await?;

        if is_root {
            self.root_folder_cache
                .write()
                .await
                .insert(folder_name.to_string(), created.id.clone());
        }

        Ok(created.id)
    }

    pub async fn rename_folder(&self, folder_id: &str, new_name: &str) -> Result<()> {
        let token = self.auth.get_valid_access_token().await?;
        let url = format!("{}/drive/v3/files/{}", self.base_url, folder_id);

        let body = serde_json::json!({
            "name": new_name
        });
        let body_str = body.to_string();

        let resp = self
            .send_with_retry(|| {
                let token = token.clone();
                let url = url.clone();
                let body_str = body_str.clone();
                async move {
                    self.client
                        .patch(&url)
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .header(CONTENT_TYPE, "application/json; charset=UTF-8")
                        .body(body_str)
                        .send()
                        .await
                }
            })
            .await?;

        resp.error_for_status()?;
        Ok(())
    }

    pub async fn upload_text_file(
        &self,
        parent_folder_id: &str,
        file_name: &str,
        content: &str,
        existing_file_id: Option<&str>,
    ) -> Result<String> {
        let token = self.auth.get_valid_access_token().await?;

        let file_id = if let Some(fid) = existing_file_id.filter(|id| !id.is_empty()) {
            fid.to_string()
        } else {
            let escaped_name = file_name.replace('\\', "\\\\").replace('\'', "\\'");
            let mut query = format!("name = '{escaped_name}' and trashed = false");
            if !parent_folder_id.is_empty() {
                query.push_str(&format!(" and '{parent_folder_id}' in parents"));
            }

            let url = format!("{}/drive/v3/files", self.base_url);
            let resp_res = self
                .send_with_retry(|| {
                    let token = token.clone();
                    let url = url.clone();
                    let query = query.clone();
                    async move {
                        self.client
                            .get(&url)
                            .header(AUTHORIZATION, format!("Bearer {token}"))
                            .query(&[("q", query.as_str()), ("fields", "files(id, name)")])
                            .send()
                            .await
                    }
                })
                .await?;

            let resp: DriveFileList = resp_res.error_for_status()?.json().await?;

            if let Some(first) = resp.files.first() {
                first.id.clone()
            } else {
                let mut meta = serde_json::json!({
                    "name": file_name,
                    "mimeType": "text/plain"
                });
                if !parent_folder_id.is_empty() {
                    meta["parents"] = serde_json::json!([parent_folder_id]);
                }
                let meta_str = meta.to_string();

                let created_res = self
                    .send_with_retry(|| {
                        let token = token.clone();
                        let url = url.clone();
                        let meta_str = meta_str.clone();
                        async move {
                            self.client
                                .post(&url)
                                .header(AUTHORIZATION, format!("Bearer {token}"))
                                .header(CONTENT_TYPE, "application/json; charset=UTF-8")
                                .body(meta_str)
                                .send()
                                .await
                        }
                    })
                    .await?;

                let created: DriveFileItem = created_res.error_for_status()?.json().await?;
                created.id
            }
        };

        let upload_url = format!(
            "{}/upload/drive/v3/files/{}?uploadType=media",
            self.upload_base_url, file_id
        );
        let content_str = content.to_string();

        let patch_res = self
            .send_with_retry(|| {
                let token = token.clone();
                let upload_url = upload_url.clone();
                let content_str = content_str.clone();
                async move {
                    self.client
                        .patch(&upload_url)
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .header(CONTENT_TYPE, "text/plain; charset=UTF-8")
                        .body(content_str)
                        .send()
                        .await
                }
            })
            .await?;

        patch_res.error_for_status()?;

        Ok(file_id)
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

        let mime_type = match file_path.extension().and_then(|e| e.to_str()) {
            Some(ext) if ext.eq_ignore_ascii_case("jsonl") => "application/x-ndjson",
            Some(ext) if ext.eq_ignore_ascii_case("txt") => "text/plain; charset=UTF-8",
            _ => "video/mp2t",
        };

        // 1. Initiate resumable upload with retry
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
            .send_with_retry(|| {
                let token = token.clone();
                let init_url = init_url.clone();
                let mime_type = mime_type.to_string();
                let file_size_str = file_size.to_string();
                let init_body = init_body.clone();
                async move {
                    self.client
                        .post(&init_url)
                        .header(AUTHORIZATION, format!("Bearer {token}"))
                        .header("X-Upload-Content-Type", mime_type)
                        .header("X-Upload-Content-Length", file_size_str)
                        .header(CONTENT_TYPE, "application/json; charset=UTF-8")
                        .body(init_body)
                        .send()
                        .await
                }
            })
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

        let buffer_size = (file_size as usize).clamp(64 * 1024, RESUMABLE_UPLOAD_BUFFER_SIZE);

        // 2. Stream chunk with progress, retrying transient errors with exponential backoff
        for attempt in 0..=max_retries {
            if attempt > 0 {
                let backoff_ms = 100 * (1 << (attempt - 1));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }

            let file = match File::open(file_path).await {
                Ok(f) => f,
                Err(e) => return Err(anyhow!("Failed to open file for upload: {e}")),
            };

            let stream = FramedRead::with_capacity(file, BytesCodec::new(), buffer_size);
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
                .header(CONTENT_TYPE, mime_type)
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
                        let err_msg = format!("HTTP error: status {status}");
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
