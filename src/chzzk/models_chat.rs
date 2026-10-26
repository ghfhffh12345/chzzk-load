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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "playerMessageTime"
    )]
    pub player_message_time: Option<u64>,
    pub extras: Option<serde_json::Value>,
    pub raw: serde_json::Value,
}

impl Default for RecordedChatMessage {
    fn default() -> Self {
        Self {
            time_ms: 0,
            datetime: String::new(),
            msg_type: "TEXT".to_string(),
            nickname: String::new(),
            user_id_hash: None,
            content: String::new(),
            donation_amount: None,
            player_message_time: None,
            extras: None,
            raw: serde_json::Value::Null,
        }
    }
}
