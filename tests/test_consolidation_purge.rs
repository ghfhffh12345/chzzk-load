mod common;

use std::fs;
use std::path::PathBuf;

use chzzk_load::consolidation::chat::delete_remote_file_checked;
use chzzk_load::consolidation::manifest::{ConsolidationChunk, TargetLocation};
use chzzk_load::consolidation::{
    delete_original_chunks, delete_original_chunks_with_bin,
    delete_original_chunks_with_concurrency,
};
use common::mock_rclone::get_mock_rclone_bin;

fn create_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}_{}", rand::random::<u32>()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn test_delete_remote_file_checked_success_and_code_4() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_test_dir("test_del_chk");

    // 1. Existing file: exit 0 -> Ok(())
    let existing = temp_dir.join("existing.jsonl");
    fs::write(&existing, b"data\n").unwrap();
    let existing_remote = format!("remote:{}", existing.to_string_lossy().replace('\\', "/"));
    let res = delete_remote_file_checked(mock_bin.to_str().unwrap(), &existing_remote).await;
    assert!(
        res.is_ok(),
        "delete_remote_file_checked should succeed for existing file: {:?}",
        res
    );
    assert!(!existing.exists(), "File should have been removed");

    // 2. Nonexistent file: exit 4 -> Ok(())
    let nonexistent = temp_dir.join("nonexistent.jsonl");
    let nonexistent_remote = format!(
        "remote:{}",
        nonexistent.to_string_lossy().replace('\\', "/")
    );
    let res = delete_remote_file_checked(mock_bin.to_str().unwrap(), &nonexistent_remote).await;
    assert!(
        res.is_ok(),
        "delete_remote_file_checked should treat exit 4 as success: {:?}",
        res
    );

    // 3. Error case: mock returns 1 on fail_delete -> Err
    let failing = temp_dir.join("chunk_fail_delete.ts");
    fs::write(&failing, b"data\n").unwrap();
    let failing_remote = format!("remote:{}", failing.to_string_lossy().replace('\\', "/"));
    let res = delete_remote_file_checked(mock_bin.to_str().unwrap(), &failing_remote).await;
    assert!(
        res.is_err(),
        "delete_remote_file_checked must return Err on non-zero exit != 4"
    );
    let err_str = res.err().unwrap().to_string();
    assert!(err_str.contains("rclone deletefile"));

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_local_success_and_preserves_metadata() {
    let temp_dir = create_test_dir("test_purge_local");

    let mut v_chunks = Vec::new();
    let mut c_chunks = Vec::new();

    // Create 30 video chunks and 30 chat chunks
    for i in 0..30 {
        let v_name = format!("chunk_{i:04}.ts");
        let c_name = format!("chat_{i:04}.jsonl");
        let v_path = temp_dir.join(&v_name);
        let c_path = temp_dir.join(&c_name);
        fs::write(&v_path, b"video data").unwrap();
        fs::write(&c_path, b"chat data").unwrap();

        v_chunks.push(ConsolidationChunk {
            name: v_name,
            index: i,
            size: 10,
        });
        c_chunks.push(ConsolidationChunk {
            name: c_name,
            index: i,
            size: 9,
        });
    }

    // Create metadata.jsonl
    let meta_path = temp_dir.join("metadata.jsonl");
    fs::write(&meta_path, b"{\"event\":\"start\"}\n").unwrap();

    let target = TargetLocation::Local(temp_dir.clone());
    let res =
        delete_original_chunks_with_concurrency(&target, &v_chunks, &c_chunks, 16, None).await;
    assert!(res.is_ok(), "Concurrent purge should succeed: {:?}", res);

    // All chunks should be deleted
    for chunk in &v_chunks {
        assert!(
            !temp_dir.join(&chunk.name).exists(),
            "Video chunk {} must be deleted",
            chunk.name
        );
    }
    for chunk in &c_chunks {
        assert!(
            !temp_dir.join(&chunk.name).exists(),
            "Chat chunk {} must be deleted",
            chunk.name
        );
    }

    // metadata.jsonl must strictly remain
    assert!(
        meta_path.exists(),
        "metadata.jsonl must strictly be preserved"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_remote_success_and_preserves_metadata() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_test_dir("test_purge_remote");

    let mut v_chunks = Vec::new();
    let mut c_chunks = Vec::new();

    for i in 0..20 {
        let v_name = format!("chunk_{i:04}.ts");
        let c_name = format!("chat_{i:04}.jsonl");
        let v_path = temp_dir.join(&v_name);
        let c_path = temp_dir.join(&c_name);
        fs::write(&v_path, b"video data").unwrap();
        fs::write(&c_path, b"chat data").unwrap();

        v_chunks.push(ConsolidationChunk {
            name: v_name,
            index: i,
            size: 10,
        });
        c_chunks.push(ConsolidationChunk {
            name: c_name,
            index: i,
            size: 9,
        });
    }

    let meta_path = temp_dir.join("metadata.jsonl");
    fs::write(&meta_path, b"{\"event\":\"start\"}\n").unwrap();

    let remote_path = format!("remote:{}", temp_dir.to_string_lossy().replace('\\', "/"));
    let target = TargetLocation::Remote(remote_path);

    let res = delete_original_chunks_with_concurrency(
        &target,
        &v_chunks,
        &c_chunks,
        8,
        Some(mock_bin.to_str().unwrap()),
    )
    .await;
    assert!(
        res.is_ok(),
        "Remote concurrent purge should succeed: {:?}",
        res
    );

    // All chunks deleted
    for chunk in &v_chunks {
        assert!(
            !temp_dir.join(&chunk.name).exists(),
            "Video chunk {} must be deleted",
            chunk.name
        );
    }
    for chunk in &c_chunks {
        assert!(
            !temp_dir.join(&chunk.name).exists(),
            "Chat chunk {} must be deleted",
            chunk.name
        );
    }

    // metadata.jsonl must remain
    assert!(
        meta_path.exists(),
        "metadata.jsonl must strictly be preserved on remote"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_local_error_aggregation_best_effort() {
    let temp_dir = create_test_dir("test_purge_local_agg");

    let mut v_chunks = Vec::new();
    let mut c_chunks = Vec::new();

    for i in 0..10 {
        let v_name = format!("chunk_{i:04}.ts");
        let c_name = format!("chat_{i:04}.jsonl");
        let v_path = temp_dir.join(&v_name);
        let c_path = temp_dir.join(&c_name);

        if i == 3 {
            // Create chunk_0003.ts as a directory so unlink_local_file_with_retry fails
            fs::create_dir_all(&v_path).unwrap();
        } else {
            fs::write(&v_path, b"video data").unwrap();
        }
        fs::write(&c_path, b"chat data").unwrap();

        v_chunks.push(ConsolidationChunk {
            name: v_name,
            index: i,
            size: 10,
        });
        c_chunks.push(ConsolidationChunk {
            name: c_name,
            index: i,
            size: 9,
        });
    }

    let target = TargetLocation::Local(temp_dir.clone());
    let res = delete_original_chunks_with_concurrency(&target, &v_chunks, &c_chunks, 4, None).await;
    assert!(res.is_err(), "Purge must fail when an error occurs");
    let err_msg = format!("{:#}", res.err().unwrap());
    assert!(
        err_msg.contains("Failed to delete 1 chunk(s) during concurrent purge"),
        "Error summary should report 1 failure, got: {err_msg}"
    );

    // Best-effort check: all other 9 video chunks and all 10 chat chunks MUST have been deleted!
    for (i, chunk) in v_chunks.iter().enumerate() {
        if i == 3 {
            assert!(
                temp_dir.join(&chunk.name).exists(),
                "Failed chunk must still exist"
            );
        } else {
            assert!(
                !temp_dir.join(&chunk.name).exists(),
                "Non-failing chunk {} must be deleted",
                chunk.name
            );
        }
    }
    for chunk in &c_chunks {
        assert!(
            !temp_dir.join(&chunk.name).exists(),
            "Chat chunk {} must be deleted",
            chunk.name
        );
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_remote_error_aggregation_best_effort() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_test_dir("test_purge_remote_agg");

    let mut v_chunks = Vec::new();
    let mut c_chunks = Vec::new();

    for i in 0..10 {
        let v_name = if i == 2 {
            format!("chunk_{i:04}_fail_delete.ts")
        } else {
            format!("chunk_{i:04}.ts")
        };
        let c_name = if i == 7 {
            format!("chat_{i:04}_fail_delete.jsonl")
        } else {
            format!("chat_{i:04}.jsonl")
        };

        let v_path = temp_dir.join(&v_name);
        let c_path = temp_dir.join(&c_name);
        fs::write(&v_path, b"video data").unwrap();
        fs::write(&c_path, b"chat data").unwrap();

        v_chunks.push(ConsolidationChunk {
            name: v_name,
            index: i,
            size: 10,
        });
        c_chunks.push(ConsolidationChunk {
            name: c_name,
            index: i,
            size: 9,
        });
    }

    let remote_path = format!("remote:{}", temp_dir.to_string_lossy().replace('\\', "/"));
    let target = TargetLocation::Remote(remote_path);

    let res = delete_original_chunks_with_concurrency(
        &target,
        &v_chunks,
        &c_chunks,
        4,
        Some(mock_bin.to_str().unwrap()),
    )
    .await;

    assert!(
        res.is_err(),
        "Remote purge with injected failures must return Err"
    );
    let err_msg = format!("{:#}", res.err().unwrap());
    assert!(
        err_msg.contains("Failed to delete 2 chunk(s) during concurrent purge"),
        "Error summary should report 2 failures, got: {err_msg}"
    );

    // Verify all 8 other non-failing chunks were still deleted (best-effort completion)!
    for (i, chunk) in v_chunks.iter().enumerate() {
        if i == 2 {
            assert!(
                temp_dir.join(&chunk.name).exists(),
                "Failing video chunk should remain"
            );
        } else {
            assert!(
                !temp_dir.join(&chunk.name).exists(),
                "Video chunk {} should be deleted",
                chunk.name
            );
        }
    }
    for (i, chunk) in c_chunks.iter().enumerate() {
        if i == 7 {
            assert!(
                temp_dir.join(&chunk.name).exists(),
                "Failing chat chunk should remain"
            );
        } else {
            assert!(
                !temp_dir.join(&chunk.name).exists(),
                "Chat chunk {} should be deleted",
                chunk.name
            );
        }
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_metadata_exclusion() {
    let temp_dir = create_test_dir("test_purge_meta_excl");

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, b"video data").unwrap();
    let meta_file = temp_dir.join("metadata.jsonl");
    fs::write(&meta_file, b"{\"event\":\"start\"}\n").unwrap();

    // Inadvertently include metadata.jsonl in chunk lists
    let v_chunks = vec![
        ConsolidationChunk {
            name: "chunk_0000.ts".to_string(),
            index: 0,
            size: 10,
        },
        ConsolidationChunk {
            name: "metadata.jsonl".to_string(),
            index: 999,
            size: 20,
        },
    ];
    let c_chunks = vec![ConsolidationChunk {
        name: "metadata.jsonl".to_string(),
        index: 999,
        size: 20,
    }];

    let target = TargetLocation::Local(temp_dir.clone());
    let res = delete_original_chunks(&target, &v_chunks, &c_chunks).await;
    assert!(res.is_ok());

    assert!(!chunk0.exists(), "chunk_0000.ts must be deleted");
    assert!(
        meta_file.exists(),
        "metadata.jsonl must strictly NOT be deleted even if passed in chunk list"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_delete_original_chunks_with_bin_delegates_correctly() {
    let mock_bin = get_mock_rclone_bin();
    let temp_dir = create_test_dir("test_del_bin");

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, b"video data").unwrap();

    let v_chunks = vec![ConsolidationChunk {
        name: "chunk_0000.ts".to_string(),
        index: 0,
        size: 10,
    }];
    let c_chunks = Vec::new();

    let remote_path = format!("remote:{}", temp_dir.to_string_lossy().replace('\\', "/"));
    let target = TargetLocation::Remote(remote_path);

    let res = delete_original_chunks_with_bin(
        &target,
        &v_chunks,
        &c_chunks,
        Some(mock_bin.to_str().unwrap()),
    )
    .await;
    assert!(res.is_ok());
    assert!(!chunk0.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_concurrent_chunk_purge_concurrency_zero_clamped_to_one() {
    let temp_dir = create_test_dir("test_purge_zero_clamp");

    let chunk0 = temp_dir.join("chunk_0000.ts");
    fs::write(&chunk0, b"video data").unwrap();

    let v_chunks = vec![ConsolidationChunk {
        name: "chunk_0000.ts".to_string(),
        index: 0,
        size: 10,
    }];
    let c_chunks = Vec::new();

    let target = TargetLocation::Local(temp_dir.clone());
    // Passing concurrency 0 should clamp to 1 without panic or semaphore deadlock
    let res = delete_original_chunks_with_concurrency(&target, &v_chunks, &c_chunks, 0, None).await;
    assert!(res.is_ok());
    assert!(!chunk0.exists());

    let _ = fs::remove_dir_all(&temp_dir);
}
