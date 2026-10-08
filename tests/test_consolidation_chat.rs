use serde_json::json;
use std::fs;

mod common;
use common::mock_rclone::get_mock_rclone_bin;

use chzzk_load::chzzk::models_chat::RecordedChatMessage;
use chzzk_load::consolidation::chat::{
    ChatDeduplicator, DEFAULT_CHAT_DEDUP_WINDOW_MS, cleanup_staged_chat, consolidate_chat,
    finalize_staged_chat,
};
use chzzk_load::consolidation::manifest::{ConsolidationChunk, TargetLocation};
use tokio_util::sync::CancellationToken;

fn make_msg(time_ms: u64, user_id_hash: Option<&str>, content: &str) -> RecordedChatMessage {
    RecordedChatMessage {
        time_ms,
        datetime: "2026-10-08 20:00:00".to_string(),
        msg_type: "COMMERCE".to_string(),
        nickname: "tester".to_string(),
        user_id_hash: user_id_hash.map(|s| s.to_string()),
        content: content.to_string(),
        donation_amount: None,
        extras: None,
        raw: json!({}),
    }
}

fn make_json_line(time_ms: u64, user_id_hash: Option<&str>, content: &str) -> String {
    let msg = make_msg(time_ms, user_id_hash, content);
    serde_json::to_string(&msg).unwrap()
}

#[test]
fn test_chat_deduplicator_duplicate_rejection_within_window() {
    let mut dedup = ChatDeduplicator::new(DEFAULT_CHAT_DEDUP_WINDOW_MS, false);

    let msg1 = make_msg(10_000, Some("user_a"), "Hello world");
    let msg1_dup = make_msg(10_000, Some("user_a"), "Hello world");
    let msg2 = make_msg(10_000, Some("user_b"), "Hello world");
    let msg3 = make_msg(10_000, Some("user_a"), "Different message");

    assert!(
        dedup.process_message(&msg1),
        "First message should be accepted"
    );
    assert!(
        !dedup.process_message(&msg1_dup),
        "Duplicate message should be rejected"
    );
    assert!(
        dedup.process_message(&msg2),
        "Different user should be accepted"
    );
    assert!(
        dedup.process_message(&msg3),
        "Different content should be accepted"
    );
}

#[test]
fn test_chat_deduplicator_sliding_window_eviction() {
    let mut dedup = ChatDeduplicator::new(10_000, false);

    let msg_old = make_msg(1_000, Some("user_a"), "Old message");
    assert!(dedup.process_message(&msg_old));
    assert!(
        !dedup.process_message(&msg_old),
        "Duplicate at 1,000ms should be rejected"
    );

    // Advance time to 12,000ms. Window is [2,000, 12,000]. 1,000ms is evicted.
    let msg_new = make_msg(12_000, Some("user_b"), "New message");
    assert!(dedup.process_message(&msg_new));

    // After eviction, msg_old (1,000ms) has fallen outside the 10s window.
    // If it arrives again, it is no longer in the sliding window and thus accepted.
    assert!(
        dedup.process_message(&msg_old),
        "Evicted message should be accepted since it's outside the window"
    );
}

#[test]
fn test_chat_deduplicator_out_of_order_tolerance_within_window() {
    let mut dedup = ChatDeduplicator::new(10_000, false);

    // Message arrives at 10,000ms (window cutoff = 0)
    let msg_anchor = make_msg(10_000, Some("user_a"), "Anchor message");
    assert!(dedup.process_message(&msg_anchor));

    // Message arrives out of order at 6,000ms (within window: 6,000 >= 0)
    let msg_ooo = make_msg(6_000, Some("user_b"), "Out of order message");
    assert!(dedup.process_message(&msg_ooo));

    // Duplicate of out-of-order message arrives
    let msg_ooo_dup = make_msg(6_000, Some("user_b"), "Out of order message");
    assert!(
        !dedup.process_message(&msg_ooo_dup),
        "Out of order duplicate must be rejected within window"
    );

    // Another unique message at 6,000ms
    let msg_ooo_diff = make_msg(6_000, Some("user_c"), "Another out of order message");
    assert!(dedup.process_message(&msg_ooo_diff));
}

#[test]
fn test_chat_deduplicator_malformed_json_recovery_default_mode() {
    let mut dedup = ChatDeduplicator::new(10_000, false);

    let valid_line = make_json_line(5_000, Some("user_1"), "Good message");
    let malformed_line = "{\"time_ms\": \"not_a_number\", malformed json";
    let blank_line = "   ";

    let res1 = dedup
        .process_line(&valid_line)
        .expect("Valid line succeeds");
    assert_eq!(res1, Some(valid_line.as_str()));

    let res_blank = dedup
        .process_line(blank_line)
        .expect("Blank line is ignored");
    assert_eq!(res_blank, None);

    let res_malformed = dedup
        .process_line(malformed_line)
        .expect("Non-strict mode recovers from malformed JSON");
    assert_eq!(res_malformed, None);

    let stats = dedup.stats();
    assert_eq!(stats.total_messages, 1);
    assert_eq!(stats.emitted_messages, 1);
    assert_eq!(stats.malformed_messages, 1);
    assert_eq!(stats.deduplicated_messages, 0);
}

#[test]
fn test_chat_deduplicator_malformed_json_strict_mode_errors() {
    let mut dedup = ChatDeduplicator::new(10_000, true);

    let malformed_line = "{not valid json}";
    let err = dedup
        .process_line(malformed_line)
        .expect_err("Strict mode must error on malformed JSON");

    assert!(
        err.to_string().contains("Malformed chat message JSON"),
        "Error message should mention malformed chat JSON: {err}"
    );
}

#[tokio::test]
async fn test_consolidate_chat_local_pipeline_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_chat_local_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0_path = temp_dir.join("chat_0000.jsonl");
    let chunk1_path = temp_dir.join("chat_0001.jsonl");

    // Chunk 0 has messages at 1,000, 2,000, and 3,000
    let line1 = make_json_line(1_000, Some("u1"), "hello");
    let line2 = make_json_line(2_000, Some("u2"), "world");
    let line3 = make_json_line(3_000, Some("u3"), "overlap");
    let chunk0_content = format!("{line1}\n{line2}\n{line3}\n");
    fs::write(&chunk0_path, &chunk0_content).unwrap();

    // Chunk 1 repeats line3 (boundary overlap at 3,000) and adds line4 at 4,000
    let line4 = make_json_line(4_000, Some("u4"), "new content");
    let chunk1_content = format!("{line3}\n{line4}\n");
    fs::write(&chunk1_path, &chunk1_content).unwrap();

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chat_0000.jsonl".to_string(),
            size: chunk0_content.len() as u64,
        },
        ConsolidationChunk {
            index: 1,
            name: "chat_0001.jsonl".to_string(),
            size: chunk1_content.len() as u64,
        },
    ];

    let target = TargetLocation::Local(temp_dir.clone());
    let cancel_token = CancellationToken::new();
    let stats = consolidate_chat(&target, &chunks, false, None, cancel_token)
        .await
        .expect("consolidate_chat should succeed");

    assert_eq!(stats.total_messages, 5);
    assert_eq!(stats.deduplicated_messages, 1);
    assert_eq!(stats.emitted_messages, 4);
    assert_eq!(stats.malformed_messages, 0);

    // Verify .part exists and final does NOT exist prior to finalization
    let part_path = temp_dir.join("consolidated.jsonl.part");
    let final_path = temp_dir.join("consolidated.jsonl");
    assert!(
        part_path.exists(),
        "consolidated.jsonl.part must exist before finalize_staged_chat"
    );
    assert!(
        !final_path.exists(),
        "consolidated.jsonl must not exist before finalize_staged_chat"
    );

    // Atomically finalize staged chat
    finalize_staged_chat(&target, None)
        .await
        .expect("finalize_staged_chat should succeed");

    // Verify consolidated.jsonl exists and has 4 lines
    assert!(final_path.exists(), "consolidated.jsonl must exist");
    let final_content = fs::read_to_string(&final_path).unwrap();
    let lines: Vec<&str> = final_content.lines().collect();
    assert_eq!(
        lines.len(),
        4,
        "Final file should have 4 deduplicated lines"
    );
    assert_eq!(lines[0], line1);
    assert_eq!(lines[1], line2);
    assert_eq!(lines[2], line3);
    assert_eq!(lines[3], line4);

    // Verify .part file does NOT remain
    assert!(
        !part_path.exists(),
        "consolidated.jsonl.part must not remain"
    );

    // Verify original chunks are completely untouched
    assert!(chunk0_path.exists(), "Original chunk_0000 must remain");
    assert!(chunk1_path.exists(), "Original chunk_0001 must remain");
    assert_eq!(fs::read_to_string(&chunk0_path).unwrap(), chunk0_content);
    assert_eq!(fs::read_to_string(&chunk1_path).unwrap(), chunk1_content);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_consolidate_chat_local_pipeline_abort_cleanup_on_error() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_chat_abort_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0_path = temp_dir.join("chat_0000.jsonl");
    let line1 = make_json_line(1_000, Some("u1"), "hello");
    let malformed = "{not a json object}";
    let chunk0_content = format!("{line1}\n{malformed}\n");
    fs::write(&chunk0_path, &chunk0_content).unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chat_0000.jsonl".to_string(),
        size: chunk0_content.len() as u64,
    }];

    let target = TargetLocation::Local(temp_dir.clone());
    let cancel_token = CancellationToken::new();
    // Strict mode = true causes error on malformed line
    let result = consolidate_chat(&target, &chunks, true, None, cancel_token).await;
    assert!(
        result.is_err(),
        "Should fail on malformed JSON in strict mode"
    );

    // Verify .part was deleted
    let part_path = temp_dir.join("consolidated.jsonl.part");
    assert!(!part_path.exists(), "Part file must be removed upon abort");

    // Verify final file was NOT created
    let final_path = temp_dir.join("consolidated.jsonl");
    assert!(
        !final_path.exists(),
        "Final file must not be created upon abort"
    );

    // Verify original chunk remains untouched
    assert!(chunk0_path.exists());
    assert_eq!(fs::read_to_string(&chunk0_path).unwrap(), chunk0_content);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_consolidate_chat_remote_pipeline_success() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_chat_rem_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0_path = temp_dir.join("chat_0000.jsonl");
    let chunk1_path = temp_dir.join("chat_0001.jsonl");

    let line1 = make_json_line(10_000, Some("u1"), "first remote message");
    let line2 = make_json_line(15_000, Some("u2"), "second remote message");
    let chunk0_content = format!("{line1}\n{line2}\n");
    fs::write(&chunk0_path, &chunk0_content).unwrap();

    // Chunk 1 duplicates line2 and adds line3
    let line3 = make_json_line(20_000, Some("u3"), "third remote message");
    let chunk1_content = format!("{line2}\n{line3}\n");
    fs::write(&chunk1_path, &chunk1_content).unwrap();

    let chunks = vec![
        ConsolidationChunk {
            index: 0,
            name: "chat_0000.jsonl".to_string(),
            size: chunk0_content.len() as u64,
        },
        ConsolidationChunk {
            index: 1,
            name: "chat_0001.jsonl".to_string(),
            size: chunk1_content.len() as u64,
        },
    ];

    let mock_bin = get_mock_rclone_bin().to_string_lossy().to_string();
    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_target = TargetLocation::Remote(format!("mock_remote:{remote_dir_str}"));
    let cancel_token = CancellationToken::new();

    let stats = consolidate_chat(
        &remote_target,
        &chunks,
        false,
        Some(&mock_bin),
        cancel_token,
    )
    .await
    .expect("Remote consolidate_chat should succeed");

    assert_eq!(stats.total_messages, 4);
    assert_eq!(stats.deduplicated_messages, 1);
    assert_eq!(stats.emitted_messages, 3);

    // Verify .part was created and final file does not exist yet
    let part_path = temp_dir.join("consolidated.jsonl.part");
    let final_path = temp_dir.join("consolidated.jsonl");
    assert!(
        part_path.exists(),
        "Remote .part file must exist before finalization"
    );
    assert!(
        !final_path.exists(),
        "Remote final file must not exist before finalization"
    );

    // Atomically finalize staged chat
    finalize_staged_chat(&remote_target, Some(&mock_bin))
        .await
        .expect("Remote finalize_staged_chat should succeed");

    // Verify consolidated.jsonl was created and finalized in the remote location
    assert!(final_path.exists(), "Remote consolidated.jsonl must exist");
    let final_content = fs::read_to_string(&final_path).unwrap();
    let lines: Vec<&str> = final_content.lines().collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], line1);
    assert_eq!(lines[1], line2);
    assert_eq!(lines[2], line3);

    // Verify .part was cleaned up
    assert!(!part_path.exists(), "Part file must be moved/removed");

    // Verify original chunks are intact
    assert!(chunk0_path.exists());
    assert!(chunk1_path.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_consolidate_chat_remote_pipeline_abort_cleanup_on_error() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_cons_chat_rem_abort_{}",
        rand::random::<u32>()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0_path = temp_dir.join("chat_0000.jsonl");
    let line1 = make_json_line(10_000, Some("u1"), "good message");
    let malformed = "malformed_json_content";
    let chunk0_content = format!("{line1}\n{malformed}\n");
    fs::write(&chunk0_path, &chunk0_content).unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chat_0000.jsonl".to_string(),
        size: chunk0_content.len() as u64,
    }];

    let mock_bin = get_mock_rclone_bin().to_string_lossy().to_string();
    let remote_dir_str = temp_dir.to_string_lossy().replace('\\', "/");
    let remote_target = TargetLocation::Remote(format!("mock_remote:{remote_dir_str}"));
    let cancel_token = CancellationToken::new();

    // Strict mode = true causes error on malformed line
    let result =
        consolidate_chat(&remote_target, &chunks, true, Some(&mock_bin), cancel_token).await;
    assert!(result.is_err(), "Strict mode must fail on malformed JSON");

    // Verify .part file was deleted
    let part_path = temp_dir.join("consolidated.jsonl.part");
    assert!(
        !part_path.exists(),
        "Part file must be deleted upon error abort"
    );

    // Verify final file was not created
    let final_path = temp_dir.join("consolidated.jsonl");
    assert!(
        !final_path.exists(),
        "Final file must not be created upon error abort"
    );

    // Verify original chunk remains untouched
    assert!(chunk0_path.exists());
    assert_eq!(fs::read_to_string(&chunk0_path).unwrap(), chunk0_content);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_consolidate_chat_cooperative_cancellation() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_chat_cancel_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let chunk0_path = temp_dir.join("chat_0000.jsonl");
    let line1 = make_json_line(1_000, Some("u1"), "cancellation test message");
    let chunk0_content = format!("{line1}\n");
    fs::write(&chunk0_path, &chunk0_content).unwrap();

    let chunks = vec![ConsolidationChunk {
        index: 0,
        name: "chat_0000.jsonl".to_string(),
        size: chunk0_content.len() as u64,
    }];

    let target = TargetLocation::Local(temp_dir.clone());
    let cancel_token = CancellationToken::new();
    cancel_token.cancel(); // Pre-cancelled to trigger cancellation branch immediately

    let result = consolidate_chat(&target, &chunks, false, None, cancel_token).await;
    assert!(
        result.is_err(),
        "Cancelled chat consolidation must fail with error"
    );

    let part_path = temp_dir.join("consolidated.jsonl.part");
    assert!(
        !part_path.exists(),
        "Part file must be deleted upon cancellation"
    );

    let final_path = temp_dir.join("consolidated.jsonl");
    assert!(
        !final_path.exists(),
        "Final file must not be created upon cancellation"
    );

    assert!(chunk0_path.exists(), "Original chunk must remain untouched");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_staged_chat_cleanup_local() {
    let temp_dir =
        std::env::temp_dir().join(format!("test_cons_chat_cleanup_{}", rand::random::<u32>()));
    fs::create_dir_all(&temp_dir).unwrap();

    let staged_file = temp_dir.join("consolidated.jsonl.part");
    let chunk0 = temp_dir.join("chat_0000.jsonl");

    tokio::fs::write(&staged_file, b"partial-chat-content")
        .await
        .unwrap();
    tokio::fs::write(&chunk0, b"original-chat-0").await.unwrap();

    let target = TargetLocation::Local(temp_dir.clone());
    cleanup_staged_chat(&target, None)
        .await
        .expect("Cleanup of staged chat file should succeed");

    assert!(
        !staged_file.exists(),
        ".part file must be removed on cleanup"
    );
    assert!(chunk0.exists(), "Original chunk must be strictly preserved");

    let _ = fs::remove_dir_all(&temp_dir);
}
