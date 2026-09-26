use chzzk_load::chzzk::models_chat::RecordedChatMessage;
use chzzk_load::recorder::chat_writer::ChatWriter;
use std::time::Duration;

#[tokio::test]
async fn test_chat_writer_batches_and_flushes_on_capacity() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat.jsonl");

    // Capacity threshold of 3 messages, long duration
    let mut writer = ChatWriter::new(chat_file.clone(), Duration::from_secs(3600), 3);

    for i in 1..=2 {
        let msg = RecordedChatMessage {
            time_ms: 1000 * i,
            datetime: "2026-09-25 00:00:00".to_string(),
            msg_type: "TEXT".to_string(),
            nickname: format!("User{i}"),
            user_id_hash: None,
            content: format!("Message {i}"),
            donation_amount: None,
            extras: None,
            raw: serde_json::json!({}),
        };
        writer.push(msg).await.unwrap();
    }

    // Should NOT have flushed yet (count = 2 < 3)
    let exists = tokio::fs::try_exists(&chat_file).await.unwrap_or(false);
    if exists {
        let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
        assert!(
            content.is_empty(),
            "Expected empty file before capacity trigger"
        );
    }

    // Push 3rd message -> triggers capacity flush
    let msg3 = RecordedChatMessage {
        time_ms: 3000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User3".to_string(),
        user_id_hash: None,
        content: "Message 3".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg3).await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3);

    let total = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 3);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_flushes_on_interval_and_termination() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_int_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat.jsonl");

    // Interval threshold of 50ms, large capacity
    let mut writer = ChatWriter::new(chat_file.clone(), Duration::from_millis(50), 1000);

    let msg = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User1".to_string(),
        user_id_hash: None,
        content: "Single message".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg).await.unwrap();

    // Wait for timer threshold to elapse
    tokio::time::sleep(Duration::from_millis(100)).await;
    let flushed = writer.maybe_flush_timer().await.unwrap();
    assert!(flushed);

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert_eq!(content.lines().count(), 1);

    let total = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 1);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_flushes_on_byte_threshold() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_bytes_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();
    let chat_file = temp_dir.join("chat.jsonl");

    // Capacity threshold high (1000), but large payload to exceed max_bytes_threshold (64KB)
    let mut writer = ChatWriter::new(chat_file.clone(), Duration::from_secs(3600), 1000);

    let large_content = "x".repeat(35 * 1024); // 35 KB each
    let msg1 = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "UserLarge".to_string(),
        user_id_hash: None,
        content: large_content.clone(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg1).await.unwrap();

    // After 1 msg (~35KB), should not have flushed
    let exists = tokio::fs::try_exists(&chat_file).await.unwrap_or(false);
    if exists {
        let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
        assert!(content.is_empty());
    }

    let msg2 = RecordedChatMessage {
        time_ms: 2000,
        datetime: "2026-09-25 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "UserLarge2".to_string(),
        user_id_hash: None,
        content: large_content,
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    // 2nd msg puts buffered bytes at ~70KB >= 64KB -> triggers flush
    writer.push(msg2).await.unwrap();

    let content = tokio::fs::read_to_string(&chat_file).await.unwrap();
    assert_eq!(content.lines().count(), 2);
    assert_eq!(writer.total_written(), 2);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
