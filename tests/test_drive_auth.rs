use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Response, Server};

use chzzk_load::drive::auth::{
    DriveAuth, StoredToken, generate_pkce_codes, parse_oauth_redirect_query,
};

#[test]
fn test_pkce_generation() {
    let (verifier, challenge) = generate_pkce_codes();
    assert!(verifier.len() >= 43);
    assert!(!challenge.is_empty());
    // Also verify another invocation produces different verifiers (randomness)
    let (verifier2, challenge2) = generate_pkce_codes();
    assert_ne!(verifier, verifier2);
    assert_ne!(challenge, challenge2);
}

#[test]
fn test_stored_token_serde() {
    let token = StoredToken {
        access_token: "mock_access".to_string(),
        refresh_token: Some("mock_refresh".to_string()),
        expires_at_epoch_sec: 1700000000,
    };

    let json = serde_json::to_string(&token).unwrap();
    let deserialized: StoredToken = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.access_token, "mock_access");
    assert_eq!(deserialized.refresh_token.as_deref(), Some("mock_refresh"));
    assert_eq!(deserialized.expires_at_epoch_sec, 1700000000);
}

#[test]
fn test_parse_oauth_redirect_query_code() {
    let url = "http://localhost:8085/oauth2callback?code=mock_auth_code_12345&scope=drive";
    let res = parse_oauth_redirect_query(url);
    assert_eq!(res, Some(Ok("mock_auth_code_12345".to_string())));
}

#[test]
fn test_parse_oauth_redirect_query_error() {
    let url = "http://localhost:8085/oauth2callback?error=access_denied&error_description=User+denied+access";
    let res = parse_oauth_redirect_query(url);
    assert_eq!(
        res,
        Some(Err((
            "access_denied".to_string(),
            "User denied access".to_string()
        )))
    );
}

#[test]
fn test_parse_oauth_redirect_query_error_no_description() {
    let url = "http://localhost:8085/oauth2callback?error=invalid_request";
    let res = parse_oauth_redirect_query(url);
    assert_eq!(
        res,
        Some(Err(("invalid_request".to_string(), String::new())))
    );
}

#[test]
fn test_parse_oauth_redirect_query_unrelated() {
    let url = "http://localhost:8085/favicon.ico";
    let res = parse_oauth_redirect_query(url);
    assert_eq!(res, None);
}

#[tokio::test]
async fn test_load_cached_token_valid() {
    let temp_dir = std::env::temp_dir().join(format!("test_drive_auth_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let cred_path = temp_dir.join("mock_credentials.json");
    let token_path = temp_dir.join("mock_token.json");

    let cred_json = r#"{
        "installed": {
            "client_id": "test-client-id.apps.googleusercontent.com",
            "client_secret": "test-client-secret",
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
        access_token: "valid_cached_token_123".to_string(),
        refresh_token: Some("refresh_token_abc".to_string()),
        expires_at_epoch_sec: future_expiry,
    };
    fs::write(
        &token_path,
        serde_json::to_string_pretty(&token_data).unwrap(),
    )
    .unwrap();

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("DriveAuth::load_or_authorize should succeed with valid cached token");

    let token = auth
        .get_valid_access_token()
        .await
        .expect("should return valid access token without refresh");

    assert_eq!(token, "valid_cached_token_123");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_refresh_expired_token() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_refresh_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let server = Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let token_uri = format!("http://127.0.0.1:{port}/token");

    let cred_path = temp_dir.join("mock_credentials.json");
    let token_path = temp_dir.join("mock_token.json");

    let cred_json = format!(
        r#"{{
            "web": {{
                "client_id": "mock_id",
                "client_secret": "mock_secret",
                "auth_uri": "http://127.0.0.1:{port}/auth",
                "token_uri": "{token_uri}"
            }}
        }}"#
    );
    fs::write(&cred_path, cred_json).unwrap();

    // Expired token (epoch = 1000)
    let token_data = StoredToken {
        access_token: "expired_token".to_string(),
        refresh_token: Some("my_refresh_token".to_string()),
        expires_at_epoch_sec: 1000,
    };
    fs::write(
        &token_path,
        serde_json::to_string_pretty(&token_data).unwrap(),
    )
    .unwrap();

    // Spawn mock HTTP server to handle token refresh request
    tokio::task::spawn_blocking(move || {
        if let Ok(request) = server.recv() {
            assert_eq!(request.method().as_str(), "POST");
            let resp = serde_json::json!({
                "access_token": "new_refreshed_token_456",
                "expires_in": 3600,
                "refresh_token": "updated_refresh_token_789"
            });
            let response = Response::from_string(resp.to_string())
                .with_status_code(200)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                        .unwrap(),
                );
            let _ = request.respond(response);
        }
    });

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("load_or_authorize should succeed");

    let new_token = auth
        .get_valid_access_token()
        .await
        .expect("get_valid_access_token should trigger refresh and succeed");

    assert_eq!(new_token, "new_refreshed_token_456");

    // Verify token file on disk was updated
    let disk_token_str = fs::read_to_string(&token_path).unwrap();
    let disk_token: StoredToken = serde_json::from_str(&disk_token_str).unwrap();
    assert_eq!(disk_token.access_token, "new_refreshed_token_456");
    assert_eq!(
        disk_token.refresh_token.as_deref(),
        Some("updated_refresh_token_789")
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(disk_token.expires_at_epoch_sec > now + 3000);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_credentials_missing_error() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_missing_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let cred_path = temp_dir.join("non_existent_credentials.json");
    let token_path = temp_dir.join("token.json");

    let result = DriveAuth::load_or_authorize(&cred_path, &token_path).await;
    assert!(result.is_err());
    let err_msg = result.err().unwrap().to_string();
    assert!(err_msg.contains("Google credentials file not found"));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_credentials_invalid_json_error() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_invalid_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let cred_path = temp_dir.join("invalid_credentials.json");
    let token_path = temp_dir.join("token.json");

    fs::write(&cred_path, "not a valid json").unwrap();

    let result = DriveAuth::load_or_authorize(&cred_path, &token_path).await;
    assert!(result.is_err());
    let err_msg = result.err().unwrap().to_string();
    assert!(err_msg.contains("Invalid Google OAuth credentials.json format"));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_expired_token_without_refresh_token_fails() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_drive_norefresh_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let cred_path = temp_dir.join("credentials.json");
    let token_path = temp_dir.join("token.json");

    let cred_json = r#"{
        "installed": {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token"
        }
    }"#;
    fs::write(&cred_path, cred_json).unwrap();

    // Expired token with NO refresh token
    let token_data = StoredToken {
        access_token: "expired_token_no_refresh".to_string(),
        refresh_token: None,
        expires_at_epoch_sec: 1000,
    };
    fs::write(
        &token_path,
        serde_json::to_string_pretty(&token_data).unwrap(),
    )
    .unwrap();

    let auth = DriveAuth::load_or_authorize(&cred_path, &token_path)
        .await
        .expect("load_or_authorize should succeed");

    let result = auth.get_valid_access_token().await;
    assert!(
        result.is_err(),
        "get_valid_access_token must return error when token is expired and no refresh token is present"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
