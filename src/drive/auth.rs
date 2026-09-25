use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Response, Server};
use url::Url;

/// Browser open helper that works cross-platform without external crate dependencies.
mod open {
    pub fn that(url: &str) -> std::io::Result<()> {
        #[cfg(target_os = "windows")]
        {
            std::process::Command::new("rundll32")
                .args(["url.dll,FileProtocolHandler", url])
                .spawn()?;
            Ok(())
        }
        #[cfg(target_os = "macos")]
        {
            std::process::Command::new("open").arg(url).spawn()?;
            Ok(())
        }
        #[cfg(not(any(target_os = "windows", target_os = "macos")))]
        {
            std::process::Command::new("xdg-open").arg(url).spawn()?;
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredToken {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at_epoch_sec: u64,
}

#[derive(Debug, Deserialize)]
struct ClientSecretFile {
    installed: Option<ClientSecretDetails>,
    web: Option<ClientSecretDetails>,
}

#[derive(Debug, Deserialize)]
struct ClientSecretDetails {
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    refresh_token: Option<String>,
}

pub fn generate_pkce_codes() -> (String, String) {
    let mut random_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut random_bytes);
    let verifier = URL_SAFE_NO_PAD.encode(random_bytes);

    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(hasher.finalize());

    (verifier, challenge)
}

pub fn parse_oauth_redirect_query(url_str: &str) -> Option<Result<String, (String, String)>> {
    let parsed = Url::parse(url_str).ok()?;
    if let Some((_, err)) = parsed.query_pairs().find(|(k, _)| k == "error") {
        let desc = parsed
            .query_pairs()
            .find(|(k, _)| k == "error_description")
            .map(|(_, v)| v.to_string())
            .unwrap_or_default();
        return Some(Err((err.to_string(), desc)));
    }
    if let Some((_, code)) = parsed.query_pairs().find(|(k, _)| k == "code") {
        return Some(Ok(code.to_string()));
    }
    None
}

pub struct DriveAuth {
    token_path: PathBuf,
    client_id: String,
    client_secret: String,
    token_uri: String,
    token: tokio::sync::Mutex<StoredToken>,
}

impl DriveAuth {
    pub async fn load_or_authorize(credentials_path: &Path, token_path: &Path) -> Result<Self> {
        let cred_content = fs::read_to_string(credentials_path).with_context(|| {
            format!(
                "Google credentials file not found at {}",
                credentials_path.display()
            )
        })?;
        let cred_file: ClientSecretFile = serde_json::from_str(&cred_content)
            .context("Invalid Google OAuth credentials.json format")?;

        let details = cred_file.installed.or(cred_file.web).ok_or_else(|| {
            anyhow!("credentials.json must contain 'installed' or 'web' client settings")
        })?;

        if let Ok(token_str) = fs::read_to_string(token_path)
            && let Ok(token) = serde_json::from_str::<StoredToken>(&token_str)
        {
            return Ok(Self {
                token_path: token_path.to_path_buf(),
                client_id: details.client_id,
                client_secret: details.client_secret,
                token_uri: details.token_uri,
                token: tokio::sync::Mutex::new(token),
            });
        }

        // Run one-time browser OAuth flow via loopback
        let (verifier, challenge) = generate_pkce_codes();
        let port = 8085;
        let redirect_uri = format!("http://127.0.0.1:{}/oauth2callback", port);

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&scope=https://www.googleapis.com/auth/drive.file&code_challenge={}&code_challenge_method=S256&access_type=offline&prompt=consent",
            details.auth_uri,
            details.client_id,
            urlencoding_encode(&redirect_uri),
            challenge
        );

        println!("Starting browser authorization for Google Drive...");
        println!(
            "If your browser did not open automatically, visit:\n{}",
            auth_url
        );
        let _ = open::that(&auth_url);

        let server = Server::http(format!("127.0.0.1:{}", port))
            .map_err(|e| anyhow!("Failed to bind local OAuth server: {}", e))?;

        let code = tokio::task::spawn_blocking(move || -> Result<String> {
            for request in server.incoming_requests() {
                let url = format!("http://localhost{}", request.url());
                match parse_oauth_redirect_query(&url) {
                    Some(Ok(code)) => {
                        let response = Response::from_string(
                            "Authentication successful! You can close this tab and return to chzzk-load.",
                        );
                        let _ = request.respond(response);
                        return Ok(code);
                    }
                    Some(Err((err, desc))) => {
                        let response = Response::from_string(format!(
                            "Authentication failed: {} ({}). You can close this tab.",
                            err, desc
                        ));
                        let _ = request.respond(response);
                        return Err(anyhow!("Google OAuth error: {} ({})", err, desc));
                    }
                    None => {
                        let _ = request.respond(Response::from_string("Waiting for Google authorization..."));
                    }
                }
            }
            Err(anyhow!("OAuth server terminated without receiving authorization code"))
        }).await??;

        // Exchange code for tokens
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let token_resp: TokenResponse = client
            .post(&details.token_uri)
            .form(&[
                ("code", code.as_str()),
                ("client_id", details.client_id.as_str()),
                ("client_secret", details.client_secret.as_str()),
                ("redirect_uri", redirect_uri.as_str()),
                ("grant_type", "authorization_code"),
                ("code_verifier", verifier.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let stored_token = StoredToken {
            access_token: token_resp.access_token,
            refresh_token: token_resp.refresh_token,
            expires_at_epoch_sec: now + token_resp.expires_in,
        };

        let json = serde_json::to_string_pretty(&stored_token)?;
        if let Some(parent) = token_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(token_path, json)?;

        Ok(Self {
            token_path: token_path.to_path_buf(),
            client_id: details.client_id,
            client_secret: details.client_secret,
            token_uri: details.token_uri,
            token: tokio::sync::Mutex::new(stored_token),
        })
    }

    pub async fn get_valid_access_token(&self) -> Result<String> {
        let mut guard = self.token.lock().await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        // Refresh if within 5 minutes of expiration
        if guard.expires_at_epoch_sec <= now + 300 {
            if let Some(ref refresh) = guard.refresh_token {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()?;
                let resp: TokenResponse = client
                    .post(&self.token_uri)
                    .form(&[
                        ("client_id", self.client_id.as_str()),
                        ("client_secret", self.client_secret.as_str()),
                        ("refresh_token", refresh.as_str()),
                        ("grant_type", "refresh_token"),
                    ])
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;

                guard.access_token = resp.access_token;
                guard.expires_at_epoch_sec = now + resp.expires_in;
                if resp.refresh_token.is_some() {
                    guard.refresh_token = resp.refresh_token;
                }

                let json = serde_json::to_string_pretty(&*guard)?;
                if let Some(parent) = self.token_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                let _ = fs::write(&self.token_path, json);
            } else {
                return Err(anyhow!(
                    "Google Drive access token is expired and no refresh_token is present in {}",
                    self.token_path.display()
                ));
            }
        }

        Ok(guard.access_token.clone())
    }
}

fn urlencoding_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
