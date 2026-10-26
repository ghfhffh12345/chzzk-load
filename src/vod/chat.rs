use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::chzzk::models_chat::RecordedChatMessage;
use crate::chzzk::models_vod::VodChatError;
use crate::chzzk::source::VodSource;
use crate::uploader::rename_local_file_with_retry;

/// Summary of paginated VOD chat ingestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodChatArchivalSummary {
    pub path: PathBuf,
    pub message_count: usize,
}

/// Staged summary before atomic promotion to final destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodChatStagedSummary {
    pub part_path: PathBuf,
    pub final_path: PathBuf,
    pub message_count: usize,
}

/// Bundled session target context for VOD chat pagination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VodChatTarget {
    pub video_no: u64,
    pub duration_ms: u64,
    pub anchor_time_ms: Option<u64>,
    pub output_dir: PathBuf,
}

/// Execution options for VOD chat pagination, adaptive pacing, and backoff.
#[derive(Debug, Clone)]
pub struct VodChatPaginationOptions {
    pub initial_pacing: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries_per_page: usize,
}

impl Default for VodChatPaginationOptions {
    fn default() -> Self {
        Self {
            initial_pacing: Duration::from_millis(50),
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            max_retries_per_page: 10,
        }
    }
}

/// Normalizes a raw Chzzk VOD chat item into the canonical [`RecordedChatMessage`] schema.
///
/// Ensures relative `playerMessageTime`, UTC timestamp `time_ms`, user ID hash, nickname,
/// badges/profile, content, and donation amount are consistently populated.
pub fn normalize_vod_chat_message(
    mut raw: serde_json::Value,
    fallback_player_time: u64,
    anchor_time_ms: Option<u64>,
) -> RecordedChatMessage {
    let player_message_time = raw
        .get("playerMessageTime")
        .and_then(|v| v.as_u64())
        .unwrap_or(fallback_player_time);

    // Sync playerMessageTime inside raw as well if missing or mismatched
    if let serde_json::Value::Object(ref mut map) = raw {
        if !map.contains_key("playerMessageTime") {
            map.insert(
                "playerMessageTime".to_string(),
                serde_json::json!(player_message_time),
            );
        }
    }

    let time_ms = raw
        .get("messageTime")
        .or_else(|| raw.get("msgTime"))
        .or_else(|| raw.get("time"))
        .and_then(|v| v.as_u64())
        .or_else(|| anchor_time_ms.map(|anchor| anchor.saturating_add(player_message_time)))
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64);

    let datetime = chrono::DateTime::from_timestamp_millis(time_ms as i64)
        .map(|dt| {
            chrono::DateTime::<chrono::Local>::from(dt)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());

    let (mut nickname, mut user_id_hash) = if let Some(profile_val) = raw.get("profile") {
        let obj: Option<serde_json::Value> = if let Some(s) = profile_val.as_str() {
            serde_json::from_str(s).ok()
        } else if profile_val.is_object() {
            Some(profile_val.clone())
        } else {
            None
        };
        if let Some(obj) = obj {
            let nick = obj
                .get("nickname")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let hash = obj
                .get("userIdHash")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            (nick, hash)
        } else {
            (String::new(), None)
        }
    } else {
        (String::new(), None)
    };

    if nickname.is_empty()
        && let Some(nick) = raw
            .get("nickname")
            .or_else(|| raw.get("senderNickname"))
            .and_then(|v| v.as_str())
    {
        nickname = nick.to_string();
    }
    if user_id_hash.is_none()
        && let Some(hash) = raw
            .get("userIdHash")
            .or_else(|| raw.get("senderChannelId"))
            .and_then(|v| v.as_str())
    {
        user_id_hash = Some(hash.to_string());
    }

    let extras = match raw.get_mut("extras").map(serde_json::Value::take) {
        Some(serde_json::Value::String(s)) => serde_json::from_str::<serde_json::Value>(&s).ok(),
        Some(val @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => Some(val),
        _ => None,
    };

    let donation_amount = raw
        .get("payAmount")
        .or_else(|| raw.get("donationAmount"))
        .and_then(|v| v.as_u64())
        .or_else(|| {
            extras.as_ref().and_then(|ext| {
                ext.get("payAmount")
                    .or_else(|| ext.get("donationAmount"))
                    .and_then(|v| v.as_u64())
            })
        });

    let msg_type_code = raw.get("msgTypeCode").and_then(|v| v.as_u64());
    let msg_type = match msg_type_code {
        Some(1) => "TEXT".to_string(),
        Some(10) => "DONATION".to_string(),
        Some(11) => "SUBSCRIPTION".to_string(),
        Some(30) => "SYSTEM_MESSAGE".to_string(),
        Some(other) => format!("TYPE_{other}"),
        None => {
            if donation_amount.is_some() {
                "DONATION".to_string()
            } else {
                "TEXT".to_string()
            }
        }
    };

    let content = raw
        .get("content")
        .or_else(|| raw.get("msg"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    RecordedChatMessage {
        time_ms,
        datetime,
        msg_type,
        nickname,
        user_id_hash,
        content,
        donation_amount,
        player_message_time: Some(player_message_time),
        extras,
        raw,
    }
}

/// Probes the last recorded chat message in an existing `vod_chat.jsonl.part` file.
///
/// Returns the highest `playerMessageTime` recorded in the file to enable cursor-resumable staging.
pub async fn probe_last_chat_player_time(part_path: &Path) -> Result<Option<u64>> {
    if !tokio::fs::try_exists(part_path).await.unwrap_or(false) {
        return Ok(None);
    }

    let file = match tokio::fs::File::open(part_path).await {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };

    let mut lines = tokio::io::BufReader::new(file).lines();
    let mut last_valid_time = None;

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Ok(msg) = serde_json::from_str::<RecordedChatMessage>(trimmed) {
            if let Some(ts) = msg.player_message_time {
                last_valid_time = Some(ts);
            } else if let Some(ts) = msg.raw.get("playerMessageTime").and_then(|v| v.as_u64()) {
                last_valid_time = Some(ts);
            }
        } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed)
            && let Some(ts) = val.get("playerMessageTime").and_then(|v| v.as_u64())
        {
            last_valid_time = Some(ts);
        }
    }

    Ok(last_valid_time)
}

async fn sleep_with_cancellation(
    writer: &mut tokio::io::BufWriter<tokio::fs::File>,
    duration: Duration,
    cancel_token: &CancellationToken,
    context: &str,
) -> Result<()> {
    tokio::select! {
        _ = cancel_token.cancelled() => {
            let _ = writer.flush().await;
            bail!("VOD chat pagination cancelled {context}");
        }
        _ = tokio::time::sleep(duration) => Ok(()),
    }
}

/// Concurrently paginates Chzzk VOD chat endpoints across the video timeline,
/// writing normalized chat messages into `vod_chat.jsonl.part`.
pub async fn paginate_vod_chat_to_staged(
    source: Arc<dyn VodSource>,
    target: VodChatTarget,
    options: VodChatPaginationOptions,
    cancel_token: CancellationToken,
) -> Result<VodChatStagedSummary> {
    let final_path = target.output_dir.join("vod_chat.jsonl");
    let part_path = target.output_dir.join("vod_chat.jsonl.part");

    // If final file already exists, verify and return early
    if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
        let count = count_lines_in_file(&final_path).await.unwrap_or(0);
        return Ok(VodChatStagedSummary {
            part_path: final_path.clone(),
            final_path,
            message_count: count,
        });
    }

    // Cursor-resumable staging: probe last recorded offset from .part file
    let resume_ts = probe_last_chat_player_time(&part_path).await?;
    let mut current_ts = match resume_ts {
        Some(ts) => ts.saturating_add(1),
        None => 0,
    };

    let mut open_options = tokio::fs::OpenOptions::new();
    open_options.create(true).write(true).append(true);
    let file = open_options.open(&part_path).await.with_context(|| {
        format!(
            "Failed to open staged chat part file: '{}'",
            part_path.display()
        )
    })?;
    let mut writer = tokio::io::BufWriter::new(file);

    let mut total_messages = 0;
    if resume_ts.is_some() {
        total_messages = count_lines_in_file(&part_path).await.unwrap_or(0);
    }

    let mut consecutive_429 = 0;
    let mut current_backoff = options.initial_backoff;

    while current_ts <= target.duration_ms {
        if cancel_token.is_cancelled() {
            let _ = writer.flush().await;
            bail!("VOD chat pagination cancelled");
        }

        let page_result = source.get_chat_page(target.video_no, current_ts).await;
        let page = match page_result {
            Ok(p) => {
                consecutive_429 = 0;
                current_backoff = options.initial_backoff;
                p
            }
            Err(VodChatError::RateLimited) => {
                consecutive_429 += 1;
                if consecutive_429 > options.max_retries_per_page {
                    let _ = writer.flush().await;
                    bail!(
                        "Exceeded maximum retries ({}) due to rate limiting (HTTP 429) at playerMessageTime {current_ts}",
                        options.max_retries_per_page
                    );
                }
                let delay = current_backoff;
                current_backoff = (current_backoff * 2).min(options.max_backoff);
                sleep_with_cancellation(&mut writer, delay, &cancel_token, "during 429 backoff")
                    .await?;
                continue;
            }
            Err(VodChatError::Unavailable(reason)) => {
                eprintln!(
                    "[INFO] [VOD] Chat replay unavailable for video #{}: {}; finalizing empty vod_chat.jsonl",
                    target.video_no, reason
                );
                break;
            }
            Err(VodChatError::Transport(err)) => {
                let _ = writer.flush().await;
                return Err(err).with_context(|| {
                    format!("Failed to retrieve chat page at playerMessageTime {current_ts}")
                });
            }
        };

        let message_count_in_page = page.video_chats.len();
        let mut highest_ts_in_page = None;

        for raw_item in page.video_chats {
            let msg = normalize_vod_chat_message(raw_item, current_ts, target.anchor_time_ms);
            if let Some(ts) = msg.player_message_time {
                highest_ts_in_page = Some(highest_ts_in_page.map_or(ts, |prev: u64| prev.max(ts)));
            }

            let line = serde_json::to_string(&msg)
                .context("Failed to serialize normalized RecordedChatMessage")?;
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            total_messages += 1;
        }

        // Determine next pagination offset
        if let Some(next_ts) = page.next_player_message_time {
            if next_ts > current_ts {
                current_ts = next_ts;
            } else if let Some(highest_ts) = highest_ts_in_page {
                current_ts = highest_ts.saturating_add(1);
            } else {
                current_ts = current_ts.saturating_add(1000);
            }
        } else if let Some(highest_ts) = highest_ts_in_page {
            if highest_ts >= current_ts {
                current_ts = highest_ts.saturating_add(1);
            } else {
                break;
            }
        } else {
            // 0 messages in page and next_player_message_time is None
            break;
        }

        if message_count_in_page == 0 && page.next_player_message_time.is_none() {
            break;
        }

        // Adaptive pacing between successful requests
        if options.initial_pacing > Duration::ZERO {
            sleep_with_cancellation(
                &mut writer,
                options.initial_pacing,
                &cancel_token,
                "during pacing delay",
            )
            .await?;
        }
    }

    writer
        .flush()
        .await
        .context("Failed to flush staged VOD chat file")?;
    drop(writer);

    Ok(VodChatStagedSummary {
        part_path,
        final_path,
        message_count: total_messages,
    })
}

/// Promotes a staged VOD chat part file (`vod_chat.jsonl.part`) to its finalized destination (`vod_chat.jsonl`).
pub async fn promote_vod_chat(part_path: &Path, final_path: &Path) -> Result<()> {
    if !tokio::fs::try_exists(part_path).await.unwrap_or(false) {
        return Ok(());
    }
    rename_local_file_with_retry(part_path, final_path)
        .await
        .with_context(|| {
            format!(
                "Failed to promote staged chat part file '{}' to final destination '{}'",
                part_path.display(),
                final_path.display()
            )
        })
}

/// Standalone convenience wrapper that paginates VOD chat and promotes to final destination upon completion.
pub async fn paginate_vod_chat(
    source: Arc<dyn VodSource>,
    target: VodChatTarget,
    options: VodChatPaginationOptions,
    cancel_token: CancellationToken,
) -> Result<VodChatArchivalSummary> {
    let staged = paginate_vod_chat_to_staged(source, target, options, cancel_token).await?;

    if staged.part_path != staged.final_path {
        promote_vod_chat(&staged.part_path, &staged.final_path).await?;
    }

    Ok(VodChatArchivalSummary {
        path: staged.final_path,
        message_count: staged.message_count,
    })
}

async fn count_lines_in_file(path: &Path) -> Result<usize> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = tokio::io::BufReader::new(file).lines();
    let mut count = 0;
    while let Some(line) = reader.next_line().await? {
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chzzk::models_vod::VodChatPage;
    use crate::chzzk::source::FakeVodSource;

    #[test]
    fn test_normalize_vod_chat_message_schema() {
        let raw = serde_json::json!({
            "playerMessageTime": 15420,
            "messageTime": 1711234567890_u64,
            "profile": {
                "nickname": "TestUser",
                "userIdHash": "hash_abc",
                "badges": [{"imageUrl": "https://example.com/badge.png"}]
            },
            "content": "Cheering for the win!",
            "msgTypeCode": 1,
            "extras": {
                "emojis": {}
            }
        });

        let msg = normalize_vod_chat_message(raw, 0, None);
        assert_eq!(msg.player_message_time, Some(15420));
        assert_eq!(msg.time_ms, 1711234567890);
        assert_eq!(msg.nickname, "TestUser");
        assert_eq!(msg.user_id_hash.as_deref(), Some("hash_abc"));
        assert_eq!(msg.content, "Cheering for the win!");
        assert_eq!(msg.msg_type, "TEXT");
        assert_eq!(msg.raw["playerMessageTime"], 15420);

        // Verify serializability and deserializability into RecordedChatMessage
        let json_line = serde_json::to_string(&msg).unwrap();
        assert!(json_line.contains("\"playerMessageTime\":15420"));
        assert!(json_line.contains("\"nickname\":\"TestUser\""));

        let deserialized: RecordedChatMessage = serde_json::from_str(&json_line).unwrap();
        assert_eq!(deserialized.player_message_time, Some(15420));
        assert_eq!(deserialized.content, "Cheering for the win!");
    }

    #[test]
    fn test_normalize_donation_chat_message() {
        let raw = serde_json::json!({
            "playerMessageTime": 45000,
            "time": 1711234600000_u64,
            "senderNickname": "GenerousDonor",
            "senderChannelId": "donor_chan_1",
            "msg": "Enjoy the stream!",
            "payAmount": 5000,
            "msgTypeCode": 10
        });

        let msg = normalize_vod_chat_message(raw, 0, None);
        assert_eq!(msg.player_message_time, Some(45000));
        assert_eq!(msg.nickname, "GenerousDonor");
        assert_eq!(msg.user_id_hash.as_deref(), Some("donor_chan_1"));
        assert_eq!(msg.donation_amount, Some(5000));
        assert_eq!(msg.msg_type, "DONATION");
    }

    #[tokio::test]
    async fn test_probe_last_chat_player_time() {
        let temp_dir = std::env::temp_dir().join(format!("test_probe_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let part_file = temp_dir.join("vod_chat.jsonl.part");

        // Non-existent file
        assert_eq!(probe_last_chat_player_time(&part_file).await.unwrap(), None);

        // Write 2 lines
        let raw1 = serde_json::json!({"playerMessageTime": 1000, "content": "first"});
        let raw2 = serde_json::json!({"playerMessageTime": 5000, "content": "second"});
        let msg1 = normalize_vod_chat_message(raw1, 1000, None);
        let msg2 = normalize_vod_chat_message(raw2, 5000, None);

        let content = format!(
            "{}\n{}\n",
            serde_json::to_string(&msg1).unwrap(),
            serde_json::to_string(&msg2).unwrap()
        );
        tokio::fs::write(&part_file, content.as_bytes())
            .await
            .unwrap();

        assert_eq!(
            probe_last_chat_player_time(&part_file).await.unwrap(),
            Some(5000)
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_paginate_vod_chat_success_and_promotion() {
        let temp_dir = std::env::temp_dir().join(format!("test_pag_ok_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let source = Arc::new(FakeVodSource::new());
        let video_no = 54321;
        let duration_ms = 60_000;

        // Page 1 at ts=0 -> 2 messages, next ts = 30000
        source.set_chat_page(
            video_no,
            0,
            VodChatPage {
                video_chats: vec![
                    serde_json::json!({"playerMessageTime": 5000, "content": "hi"}),
                    serde_json::json!({"playerMessageTime": 12000, "content": "chat 2"}),
                ],
                next_player_message_time: Some(30000),
            },
        );

        // Page 2 at ts=30000 -> 1 message, next ts = 60000
        source.set_chat_page(
            video_no,
            30000,
            VodChatPage {
                video_chats: vec![
                    serde_json::json!({"playerMessageTime": 45000, "content": "chat 3"}),
                ],
                next_player_message_time: Some(60000),
            },
        );

        // Page 3 at ts=60000 -> 0 messages, next ts = None
        source.set_chat_page(
            video_no,
            60000,
            VodChatPage {
                video_chats: vec![],
                next_player_message_time: None,
            },
        );

        let options = VodChatPaginationOptions {
            initial_pacing: Duration::from_millis(1),
            ..Default::default()
        };
        let cancel = CancellationToken::new();

        let target = VodChatTarget {
            video_no,
            duration_ms,
            anchor_time_ms: None,
            output_dir: temp_dir.clone(),
        };

        let summary = paginate_vod_chat(source, target, options, cancel)
            .await
            .unwrap();

        assert_eq!(summary.message_count, 3);
        assert!(summary.path.exists());
        assert_eq!(summary.path.file_name().unwrap(), "vod_chat.jsonl");
        assert!(!temp_dir.join("vod_chat.jsonl.part").exists());

        let lines: Vec<String> = tokio::fs::read_to_string(&summary.path)
            .await
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        assert_eq!(lines.len(), 3);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_paginate_vod_chat_resumes_from_part_file() {
        let temp_dir = std::env::temp_dir().join(format!("test_pag_res_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let part_file = temp_dir.join("vod_chat.jsonl.part");

        // Seed part file with message at playerMessageTime = 10000
        let seed_msg = normalize_vod_chat_message(
            serde_json::json!({"playerMessageTime": 10000, "content": "seed"}),
            10000,
            None,
        );
        tokio::fs::write(
            &part_file,
            format!("{}\n", serde_json::to_string(&seed_msg).unwrap()),
        )
        .await
        .unwrap();

        let source = Arc::new(FakeVodSource::new());
        let video_no = 777;
        let duration_ms = 50_000;

        // Resume call must ask for current_ts = 10001
        source.set_chat_page(
            video_no,
            10001,
            VodChatPage {
                video_chats: vec![
                    serde_json::json!({"playerMessageTime": 25000, "content": "resumed msg"}),
                ],
                next_player_message_time: None,
            },
        );

        let options = VodChatPaginationOptions {
            initial_pacing: Duration::from_millis(1),
            ..Default::default()
        };
        let cancel = CancellationToken::new();

        let target = VodChatTarget {
            video_no,
            duration_ms,
            anchor_time_ms: None,
            output_dir: temp_dir.clone(),
        };

        let summary = paginate_vod_chat(source.clone(), target, options, cancel)
            .await
            .unwrap();

        assert_eq!(summary.message_count, 2);
        assert_eq!(
            source.chat_page_calls(),
            vec![(video_no, 10001), (video_no, 25001)]
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_paginate_vod_chat_rate_limit_backoff() {
        let temp_dir = std::env::temp_dir().join(format!("test_pag_429_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let source = Arc::new(FakeVodSource::new());
        let video_no = 888;
        let duration_ms = 10_000;

        // Queue: 429 error first, then success
        source.enqueue_chat_page_response(video_no, 0, Err(VodChatError::RateLimited));
        source.enqueue_chat_page_response(
            video_no,
            0,
            Ok(VodChatPage {
                video_chats: vec![
                    serde_json::json!({"playerMessageTime": 5000, "content": "recovered"}),
                ],
                next_player_message_time: None,
            }),
        );

        let options = VodChatPaginationOptions {
            initial_pacing: Duration::from_millis(1),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            max_retries_per_page: 3,
        };
        let cancel = CancellationToken::new();

        let target = VodChatTarget {
            video_no,
            duration_ms,
            anchor_time_ms: None,
            output_dir: temp_dir.clone(),
        };

        let summary = paginate_vod_chat(source.clone(), target, options, cancel)
            .await
            .unwrap();

        assert_eq!(summary.message_count, 1);
        assert_eq!(
            source.chat_page_calls(),
            vec![(video_no, 0), (video_no, 0), (video_no, 5001)]
        );

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_paginate_vod_chat_graceful_degradation_on_disabled() {
        let temp_dir = std::env::temp_dir().join(format!("test_pag_dis_{}", rand::random::<u32>()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let source = Arc::new(FakeVodSource::new());
        let video_no = 999;
        let duration_ms = 10_000;

        // Simulate disabled / not found chat endpoint
        source.set_chat_error(
            video_no,
            0,
            VodChatError::Unavailable("Chat is disabled for this stream".to_string()),
        );

        let options = VodChatPaginationOptions {
            initial_pacing: Duration::from_millis(1),
            ..Default::default()
        };
        let cancel = CancellationToken::new();

        let target = VodChatTarget {
            video_no,
            duration_ms,
            anchor_time_ms: None,
            output_dir: temp_dir.clone(),
        };

        let summary = paginate_vod_chat(source, target, options, cancel)
            .await
            .unwrap();

        // Finalized empty file
        assert_eq!(summary.message_count, 0);
        assert!(summary.path.exists());
        assert_eq!(summary.path.file_name().unwrap(), "vod_chat.jsonl");

        let content = tokio::fs::read_to_string(&summary.path).await.unwrap();
        assert!(content.is_empty());

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
