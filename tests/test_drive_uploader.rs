use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Response, Server, StatusCode};

use chzzk_load::drive::auth::{DriveAuth, StoredToken};
use chzzk_load::drive::client::{DriveClient, build_resumable_init_body};
use chzzk_load::uploader::{UploadTask, UploadWorker};

async fn create_mock_drive_auth(temp_dir: &Path) -> Arc<DriveAuth> {
    let cred_path = temp_dir.join("mock_credentials.json");
    let token_path = temp_dir.join("mock_token.json");

    let cred_json = r#"{
        "installed": {
            "client_id": "mock_client_id",
            "client_secret": "mock_client_secret",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token"
        }
    }"#;
    fs::write(&cred_path, cred_json).unwrap();

    let future_expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;

    let token_data = StoredToken {
        access_token: "mock_test_token_xyz".to_string(),
        refresh_token: Some("mock_refresh_xyz".to_string()),
        expires_at_epoch_sec: future_expiry,
    };
    fs::write(
        &token_path,
        serde_json::to_string_pretty(&token_data).unwrap(),
    )
    .unwrap();

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("Failed to initialize mock DriveAuth");

    Arc::new(auth)
}

#[test]
fn test_resumable_metadata_payload() {
    let json = build_resumable_init_body("chunk_0000.ts", Some("folder_123"));
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(val["name"], "chunk_0000.ts");
    assert_eq!(val["parents"][0], "folder_123");
}

#[test]
fn test_resumable_metadata_payload_no_parent() {
    let json = build_resumable_init_body("chunk_0001.ts", None);
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(val["name"], "chunk_0001.ts");
    assert_eq!(val["parents"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_get_or_create_folder_existing() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_client_exist_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            assert_eq!(request.method().as_str(), "GET");
            assert!(request.url().contains("/drive/v3/files"));

            let auth_hdr = request.headers().iter().find(|h| {
                h.field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("authorization")
            });
            assert!(auth_hdr.is_some());
            assert_eq!(
                auth_hdr.unwrap().value.as_str(),
                "Bearer mock_test_token_xyz"
            );

            let mock_response = serde_json::json!({
                "files": [
                    { "id": "existing_folder_id_111", "name": "chzzk_records" }
                ]
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let folder_id = client
        .get_or_create_folder("chzzk_records", Some("parent_root"))
        .await
        .unwrap();
    assert_eq!(folder_id, "existing_folder_id_111");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_get_or_create_folder_with_quotes() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_drive_client_quotes_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            assert_eq!(request.method().as_str(), "GET");
            let req_url = format!("http://dummy{}", request.url());
            let parsed_url = url::Url::parse(&req_url).unwrap();
            let q_param = parsed_url
                .query_pairs()
                .find(|(k, _)| k == "q")
                .map(|(_, v)| v.into_owned());
            assert!(q_param.is_some(), "Missing q query parameter");
            let q = q_param.unwrap();
            assert!(
                q.contains("name = 'Streamer\\'s Stream'"),
                "Query q should contain escaped single quote: {}",
                q
            );

            let mock_response = serde_json::json!({
                "files": [
                    { "id": "quote_folder_id_456", "name": "Streamer's Stream" }
                ]
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let folder_id = client
        .get_or_create_folder("Streamer's Stream", Some("parent_root"))
        .await
        .unwrap();
    assert_eq!(folder_id, "quote_folder_id_456");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_get_or_create_folder_creates_new() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_drive_client_create_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        // 1. Query files returns empty
        if let Ok(request) = server.recv() {
            assert_eq!(request.method().as_str(), "GET");
            let mock_response = serde_json::json!({ "files": [] });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }

        // 2. Post files creates new
        if let Ok(mut request) = server.recv() {
            assert_eq!(request.method().as_str(), "POST");
            let mut body = String::new();
            request.as_reader().read_to_string(&mut body).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["name"], "new_stream_folder");
            assert_eq!(parsed["mimeType"], "application/vnd.google-apps.folder");
            assert_eq!(parsed["parents"][0], "parent_root_99");

            let mock_response = serde_json::json!({
                "id": "new_created_folder_222",
                "name": "new_stream_folder"
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let folder_id = client
        .get_or_create_folder("new_stream_folder", Some("parent_root_99"))
        .await
        .unwrap();
    assert_eq!(folder_id, "new_created_folder_222");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_rename_folder_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_rename_ok_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        if let Ok(mut request) = server.recv() {
            assert_eq!(request.method().as_str(), "PATCH");
            assert!(request.url().contains("/drive/v3/files/target_folder_123"));

            let auth_hdr = request.headers().iter().find(|h| {
                h.field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("authorization")
            });
            assert!(auth_hdr.is_some());
            assert_eq!(
                auth_hdr.unwrap().value.as_str(),
                "Bearer mock_test_token_xyz"
            );

            let mut body_str = String::new();
            request.as_reader().read_to_string(&mut body_str).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&body_str).unwrap();
            assert_eq!(
                parsed["name"],
                "[2026-09-22_1000] Streamer - New Stream Title"
            );

            let mock_response = serde_json::json!({
                "id": "target_folder_123",
                "name": "[2026-09-22_1000] Streamer - New Stream Title"
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let res = client
        .rename_folder(
            "target_folder_123",
            "[2026-09-22_1000] Streamer - New Stream Title",
        )
        .await;
    assert!(res.is_ok());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_rename_folder_error() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_rename_err_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            assert_eq!(request.method().as_str(), "PATCH");
            let response =
                Response::from_string(r#"{"error":"Not Found"}"#).with_status_code(StatusCode(404));
            let _ = request.respond(response);
        }
    });

    let res = client
        .rename_folder("non_existent_folder", "New Name")
        .await;
    assert!(res.is_err());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_upload_file_resumable_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_upload_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_file = temp_dir.join("chunk_0000.ts");
    let test_data = b"MPEGTS_STREAM_DUMMY_PAYLOAD_FOR_TESTING_CHUNKS_1234567890";
    fs::write(&chunk_file, test_data).unwrap();
    let file_len = test_data.len() as u64;

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        // 1. Resumable Init POST
        if let Ok(mut request) = server.recv() {
            assert_eq!(request.method().as_str(), "POST");
            assert!(request.url().contains("uploadType=resumable"));

            let x_type = request.headers().iter().find(|h| {
                h.field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("x-upload-content-type")
            });
            assert_eq!(x_type.unwrap().value.as_str(), "video/mp2t");

            let x_len = request.headers().iter().find(|h| {
                h.field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("x-upload-content-length")
            });
            assert_eq!(x_len.unwrap().value.as_str(), file_len.to_string());

            let mut body = String::new();
            request.as_reader().read_to_string(&mut body).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(parsed["name"], "chunk_0000.ts");
            assert_eq!(parsed["parents"][0], "folder_target_55");

            let upload_url = format!("http://127.0.0.1:{}/resumable_upload_target_session", port);
            let response = Response::empty(200)
                .with_header(Header::from_bytes(&b"Location"[..], upload_url.as_bytes()).unwrap());
            let _ = request.respond(response);
        }

        // 2. Resumable Upload PUT
        if let Ok(mut request) = server.recv() {
            assert_eq!(request.method().as_str(), "PUT");
            assert_eq!(request.url(), "/resumable_upload_target_session");

            let c_type = request.headers().iter().find(|h| {
                h.field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("content-type")
            });
            assert_eq!(c_type.unwrap().value.as_str(), "video/mp2t");

            let mut received_bytes = Vec::new();
            request
                .as_reader()
                .read_to_end(&mut received_bytes)
                .unwrap();
            assert_eq!(received_bytes, test_data);

            let mock_response = serde_json::json!({
                "id": "uploaded_file_id_999",
                "name": "chunk_0000.ts"
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let progress_uploaded = Arc::new(AtomicU64::new(0));
    let progress_total = Arc::new(AtomicU64::new(0));
    let p_up = Arc::clone(&progress_uploaded);
    let p_tot = Arc::clone(&progress_total);

    let file_id = client
        .upload_file_resumable(&chunk_file, "folder_target_55", move |up, tot| {
            p_up.store(up, Ordering::SeqCst);
            p_tot.store(tot, Ordering::SeqCst);
        })
        .await
        .unwrap();

    assert_eq!(file_id, "uploaded_file_id_999");
    assert_eq!(progress_uploaded.load(Ordering::SeqCst), file_len);
    assert_eq!(progress_total.load(Ordering::SeqCst), file_len);
    // File must NOT be deleted by upload_file_resumable itself
    assert!(chunk_file.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_upload_worker_upload_and_delete_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_uploader_delete_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_file = temp_dir.join("chunk_0002.ts");
    let test_data = b"STREAMING_VIDEO_BYTES_TO_BE_DELETED_ON_SUCCESS";
    fs::write(&chunk_file, test_data).unwrap();
    let file_len = test_data.len() as u64;

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        // Init
        if let Ok(request) = server.recv() {
            let upload_url = format!("http://127.0.0.1:{}/session_delete", port);
            let response = Response::empty(200)
                .with_header(Header::from_bytes(&b"Location"[..], upload_url.as_bytes()).unwrap());
            let _ = request.respond(response);
        }
        // Upload
        if let Ok(mut request) = server.recv() {
            let mut buf = Vec::new();
            request.as_reader().read_to_end(&mut buf).unwrap();
            let mock_response = serde_json::json!({
                "id": "file_deleted_from_local_333",
                "name": "chunk_0002.ts"
            });
            let response = Response::from_string(mock_response.to_string()).with_header(
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
            );
            let _ = request.respond(response);
        }
    });

    let task = UploadTask {
        channel_id: "chan_test".to_string(),
        session_folder_id: "session_dir_123".to_string(),
        chunk_path: chunk_file.clone(),
        chunk_name: "chunk_0002.ts".to_string(),
        streamer_name: "Streamer 1".to_string(),
    };

    assert!(chunk_file.exists());
    let deleted_size = UploadWorker::upload_and_delete(&client, task, |_, _| {})
        .await
        .unwrap();
    assert_eq!(deleted_size, file_len);
    // Crucial requirement: chunk file must be deleted upon confirmed upload!
    assert!(
        !chunk_file.exists(),
        "Chunk file must be deleted after confirmed upload"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_upload_worker_preserves_file_on_upload_failure() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_uploader_fail_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk_file = temp_dir.join("chunk_failed.ts");
    let test_data = b"CRITICAL_PAYLOAD_MUST_NOT_BE_DELETED_ON_ERROR";
    fs::write(&chunk_file, test_data).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();

    let auth = create_mock_drive_auth(&temp_dir).await;
    let client = DriveClient::new(auth).with_base_urls(
        format!("http://127.0.0.1:{}", port),
        format!("http://127.0.0.1:{}", port),
    );

    std::thread::spawn(move || {
        // Return 500 error on init
        if let Ok(request) = server.recv() {
            let response =
                Response::from_string("Internal Server Error").with_status_code(StatusCode(500));
            let _ = request.respond(response);
        }
    });

    let task = UploadTask {
        channel_id: "chan_test".to_string(),
        session_folder_id: "session_dir_123".to_string(),
        chunk_path: chunk_file.clone(),
        chunk_name: "chunk_failed.ts".to_string(),
        streamer_name: "Streamer 1".to_string(),
    };

    assert!(chunk_file.exists());
    let result = UploadWorker::upload_and_delete(&client, task, |_, _| {}).await;
    assert!(
        result.is_err(),
        "upload_and_delete must fail when Drive returns 500"
    );
    // Crucial requirement: Chunk file must NOT be deleted if upload fails!
    assert!(
        chunk_file.exists(),
        "Chunk file must NOT be deleted if upload failed"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_root_credentials_not_touched() {
    // If credentials.json exists in root, ensure it is readable and intact
    let root_cred = Path::new("credentials.json");
    if root_cred.exists() {
        assert!(root_cred.is_file());
        let meta = fs::metadata(root_cred).unwrap();
        assert!(meta.len() > 0);
    }
}
