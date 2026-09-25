use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChatAccessTokenResponse {
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[serde(rename = "extraToken")]
    pub extra_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedChatMessage {
    pub time_ms: u64,
    pub datetime: String,
    pub msg_type: String,
    pub nickname: String,
    pub user_id_hash: Option<String>,
    pub content: String,
    pub donation_amount: Option<u64>,
    pub extras: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}
