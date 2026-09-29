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

    let (total, _) = writer.flush_and_close().await.unwrap();
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

    let (total, _) = writer.flush_and_close().await.unwrap();
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
    let (total, _) = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 2);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_rotates_on_interval() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_rot_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    // 50ms chunk interval, large capacity
    let mut writer = ChatWriter::new_rotating(
        temp_dir.clone(),
        Duration::from_millis(50),
        Duration::from_secs(3600),
        1000,
    );

    assert_eq!(writer.current_chunk_index(), 0);
    assert_eq!(writer.current_chunk_path(), temp_dir.join("chat_0000.jsonl"));

    let msg1 = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-29 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User1".to_string(),
        user_id_hash: None,
        content: "Message 1".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg1).await.unwrap();

    // Sleep past interval
    tokio::time::sleep(Duration::from_millis(70)).await;

    // Rotate should seal chunk 0
    let sealed0 = writer.maybe_rotate().await.unwrap();
    assert_eq!(sealed0, Some(temp_dir.join("chat_0000.jsonl")));
    assert_eq!(writer.current_chunk_index(), 1);
    assert_eq!(writer.current_chunk_path(), temp_dir.join("chat_0001.jsonl"));

    let content0 = tokio::fs::read_to_string(temp_dir.join("chat_0000.jsonl")).await.unwrap();
    assert_eq!(content0.lines().count(), 1);

    // Write message to chunk 1
    let msg2 = RecordedChatMessage {
        time_ms: 2000,
        datetime: "2026-09-29 00:00:01".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User2".to_string(),
        user_id_hash: None,
        content: "Message 2".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg2).await.unwrap();

    tokio::time::sleep(Duration::from_millis(70)).await;
    let sealed1 = writer.maybe_rotate().await.unwrap();
    assert_eq!(sealed1, Some(temp_dir.join("chat_0001.jsonl")));
    assert_eq!(writer.current_chunk_index(), 2);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_skips_empty_interval() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_empty_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let mut writer = ChatWriter::new_rotating(
        temp_dir.clone(),
        Duration::from_millis(50),
        Duration::from_secs(3600),
        1000,
    );

    // Chunk 0 has 1 message
    let msg = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-29 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User1".to_string(),
        user_id_hash: None,
        content: "First chunk".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(70)).await;
    let sealed0 = writer.maybe_rotate().await.unwrap();
    assert_eq!(sealed0, Some(temp_dir.join("chat_0000.jsonl")));
    assert_eq!(writer.current_chunk_index(), 1);

    // Interval 1: 0 messages pushed. Wait and rotate.
    tokio::time::sleep(Duration::from_millis(70)).await;
    let sealed1 = writer.maybe_rotate().await.unwrap();
    assert_eq!(sealed1, None); // Skipped!
    assert!(!tokio::fs::try_exists(temp_dir.join("chat_0001.jsonl")).await.unwrap_or(false));
    assert_eq!(writer.current_chunk_index(), 2);

    // Interval 2: 1 message pushed.
    let msg2 = RecordedChatMessage {
        time_ms: 2000,
        datetime: "2026-09-29 00:00:02".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User2".to_string(),
        user_id_hash: None,
        content: "Chunk 2 after skip".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg2).await.unwrap();

    let (total, final_sealed) = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 2);
    assert_eq!(final_sealed, Some(temp_dir.join("chat_0002.jsonl")));
    assert!(tokio::fs::try_exists(temp_dir.join("chat_0002.jsonl")).await.unwrap_or(false));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_chat_writer_flush_and_close_with_lingering_messages() {
    let temp_dir = std::env::temp_dir().join(format!("test_cw_close_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let mut writer = ChatWriter::new_rotating(
        temp_dir.clone(),
        Duration::from_secs(3600), // Long interval, won't auto-rotate
        Duration::from_secs(3600),
        1000,
    );

    // With 0 messages, flush_and_close returns None
    let (total, final_sealed) = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 0);
    assert_eq!(final_sealed, None);
    assert!(!tokio::fs::try_exists(temp_dir.join("chat_0000.jsonl")).await.unwrap_or(false));

    // Now push 1 message without rotating
    let msg = RecordedChatMessage {
        time_ms: 1000,
        datetime: "2026-09-29 00:00:00".to_string(),
        msg_type: "TEXT".to_string(),
        nickname: "User1".to_string(),
        user_id_hash: None,
        content: "Closing message".to_string(),
        donation_amount: None,
        extras: None,
        raw: serde_json::json!({}),
    };
    writer.push(msg).await.unwrap();

    let (total, final_sealed) = writer.flush_and_close().await.unwrap();
    assert_eq!(total, 1);
    assert_eq!(final_sealed, Some(temp_dir.join("chat_0000.jsonl")));
    assert!(tokio::fs::try_exists(temp_dir.join("chat_0000.jsonl")).await.unwrap_or(false));

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}
