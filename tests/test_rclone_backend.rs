use chzzk_load::config::RcloneConfig;
use chzzk_load::uploader::backend::{MockUploadBackend, UploadBackend};
use chzzk_load::uploader::rclone::{RcloneBackend, format_destination, parse_rclone_log_line};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

mod common;
use common::mock_rclone::get_mock_rclone_bin;

#[test]
fn test_format_destination_standard() {
    let dest = format_destination("gdrive:Chzzk", "session_1", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/session_1/chunk_0000.ts");
}

#[test]
fn test_format_destination_trailing_slashes() {
    let dest = format_destination("gdrive:Chzzk/", "session_1/", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/session_1/chunk_0000.ts");
    assert!(!dest.contains("//"));

    let dest2 = format_destination("gdrive:Chzzk///", "session_1///", "/chunk_0000.ts");
    assert_eq!(dest2, "gdrive:Chzzk/session_1/chunk_0000.ts");
    assert!(!dest2.contains("//"));
}

#[test]
fn test_format_destination_empty_remote_dir() {
    let dest = format_destination("gdrive:Chzzk", "", "chunk_0000.ts");
    assert_eq!(dest, "gdrive:Chzzk/chunk_0000.ts");

    let dest2 = format_destination("gdrive:Chzzk/", "/", "chunk_0000.ts");
    assert_eq!(dest2, "gdrive:Chzzk/chunk_0000.ts");
}

#[test]
fn test_parse_rclone_log_progress() {
    let json_line = r#"{"level":"info","msg":"Transferred","stats":{"bytes":5242880,"totalBytes":10485760,"speed":1048576.0}}"#;
    let stats = parse_rclone_log_line(json_line).expect("should parse stats from rclone json log");
    assert_eq!(stats.bytes, 5242880);
    assert_eq!(stats.total_bytes, 10485760);
    assert!((stats.speed - 1048576.0).abs() < f64::EPSILON);

    // Also verify bare stats object in json
    let json_bare = r#"{"stats":{"bytes":5242880,"totalBytes":10485760,"speed":1048576.0}}"#;
    let stats_bare = parse_rclone_log_line(json_bare).expect("should parse bare stats");
    assert_eq!(stats_bare.bytes, 5242880);
    assert_eq!(stats_bare.total_bytes, 10485760);
}

#[test]
fn test_parse_rclone_log_omits_total_bytes() {
    let json_line =
        r#"{"level":"info","msg":"Transferred","stats":{"bytes":5242880,"speed":1048576.0}}"#;
    let stats =
        parse_rclone_log_line(json_line).expect("should parse stats when totalBytes is omitted");
    assert_eq!(stats.bytes, 5242880);
    assert_eq!(stats.total_bytes, 0);
    assert!((stats.speed - 1048576.0).abs() < f64::EPSILON);
}

#[test]
fn test_parse_rclone_log_non_json_or_notice() {
    // Plain text log line
    assert!(parse_rclone_log_line("2026/09/29 14:00:00 NOTICE: Config file not found").is_none());

    // JSON without stats
    assert!(parse_rclone_log_line(r#"{"level":"info","msg":"Starting transfer"}"#).is_none());

    // Malformed JSON
    assert!(parse_rclone_log_line("{not json").is_none());
    assert!(parse_rclone_log_line("").is_none());
    assert!(parse_rclone_log_line("   ").is_none());
}

#[test]
fn test_rclone_backend_command_builder() {
    let config = RcloneConfig {
        remote_path: "remote:chzzk".to_string(),
        upload_concurrency: 3,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--fast-list".to_string(), "--transfers=4".to_string()],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_copy_command(
        Path::new("recordings/chunk_0000.ts"),
        "remote:chzzk/session/chunk_0000.ts",
    );
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "copyto");
    assert!(args[1].ends_with("chunk_0000.ts"));
    assert_eq!(args[2], "remote:chzzk/session/chunk_0000.ts");
    assert!(args.contains(&"--use-json-log".to_string()));
    assert!(args.contains(&"--stats".to_string()));
    assert!(args.contains(&"250ms".to_string()));
    assert!(args.contains(&"--stats-log-level".to_string()));
    assert!(args.contains(&"NOTICE".to_string()));
    assert!(args.contains(&"--fast-list".to_string()));
    assert!(args.contains(&"--transfers=4".to_string()));
}

#[test]
fn test_rclone_backend_build_copy_command_includes_high_throughput_flags() {
    let local_file = Path::new("local.txt");
    let remote_dest = "mock_remote:remote.txt";
    let extra_args = vec![
        "--low-level-retries".to_string(),
        "10".to_string(),
        "--fast-list".to_string(),
    ];

    let config = RcloneConfig {
        consolidate_extra_args: extra_args,
        ..Default::default()
    };
    let backend = RcloneBackend::for_consolidation(config).with_bin("mock_rclone");

    let cmd = backend.build_copy_command(local_file, remote_dest);
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "copyto");
    assert_eq!(args[1], local_file.to_string_lossy().to_string());
    assert_eq!(args[2], remote_dest);
    assert!(args.contains(&"--use-json-log".to_string()));
    assert!(args.contains(&"--stats".to_string()));
    assert!(args.contains(&"250ms".to_string()));
    assert!(args.contains(&"--stats-log-level".to_string()));
    assert!(args.contains(&"NOTICE".to_string()));
    assert!(args.contains(&"--drive-chunk-size".to_string()));
    assert!(args.contains(&"128M".to_string()));
    assert!(args.contains(&"--s3-chunk-size".to_string()));
    assert!(args.contains(&"64M".to_string()));
    assert!(args.contains(&"--s3-upload-concurrency".to_string()));
    assert!(args.contains(&"4".to_string()));
    assert!(args.contains(&"--buffer-size".to_string()));
    assert!(args.contains(&"64M".to_string()));
    assert!(args.contains(&"--multi-thread-streams".to_string()));
    assert!(args.contains(&"4".to_string()));
    assert!(args.contains(&"--multi-thread-cutoff".to_string()));
    assert!(args.contains(&"250M".to_string()));
    assert!(args.contains(&"--low-level-retries".to_string()));
    assert!(args.contains(&"10".to_string()));
    assert!(args.contains(&"--fast-list".to_string()));
}

#[test]
fn test_rclone_backend_bin_resolution() {
    let config = RcloneConfig {
        remote_path: "gdrive:Test".to_string(),
        upload_concurrency: 2,
        rclone_bin: "custom_rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);
    let backend_with_bin = backend.with_bin("explicit_rclone");
    assert_eq!(backend_with_bin.resolve_bin(), "explicit_rclone");
}

#[test]
fn test_rclone_backend_check_command_builder() {
    let config = RcloneConfig {
        remote_path: "gdrive:Chzzk".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=1".to_string()],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_check_command();
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "lsf");
    assert!(args.contains(&"--max-depth".to_string()));
    assert!(args.contains(&"1".to_string()));
    assert!(args.contains(&"gdrive:Chzzk".to_string()));
    assert!(args.contains(&"--retries=1".to_string()));
}

#[test]
fn test_rclone_backend_object_safety() {
    let config = RcloneConfig::default();
    let backend = RcloneBackend::new(config);
    let dyn_backend: Arc<dyn UploadBackend> = Arc::new(backend);
    assert_eq!(
        dyn_backend.as_ref() as *const _,
        dyn_backend.as_ref() as *const _
    );
}

#[test]
fn test_rclone_backend_move_command_builder() {
    let config = RcloneConfig {
        remote_path: "gdrive:Chzzk".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=2".to_string()],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_move_command("remote:dir/vod.mp4.part", "remote:dir/vod.mp4");
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "moveto");
    assert_eq!(args[1], "remote:dir/vod.mp4.part");
    assert_eq!(args[2], "remote:dir/vod.mp4");
    assert!(args.contains(&"--retries=2".to_string()));
}

#[test]
fn test_rclone_backend_delete_command_builder() {
    let config = RcloneConfig {
        remote_path: "gdrive:Chzzk".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec!["--retries=2".to_string()],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);
    let cmd = backend.build_delete_command("remote:dir/vod.mp4.part");
    let std_cmd = cmd.as_std();
    let args: Vec<String> = std_cmd
        .get_args()
        .map(|s| s.to_string_lossy().to_string())
        .collect();

    assert_eq!(args[0], "deletefile");
    assert_eq!(args[1], "remote:dir/vod.mp4.part");
    assert!(args.contains(&"--retries=2".to_string()));
}

#[tokio::test]
async fn test_rclone_backend_check_connection_with_mock() {
    let _mock_bin = get_mock_rclone_bin();

    // 1. Success check
    let valid_config = RcloneConfig {
        remote_path: "remote:valid_bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(valid_config);
    let res = backend.check_connection().await;
    assert!(res.is_ok(), "Valid connection check should succeed");

    // 2. Failure check with fail_conn
    let fail_config = RcloneConfig {
        remote_path: "remote:fail_conn_bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let fail_backend = RcloneBackend::new(fail_config);
    let err = fail_backend
        .check_connection()
        .await
        .expect_err("fail_conn remote path must fail check");
    assert!(
        format!("{err:#}").contains("rclone connection check failed"),
        "Error message must clearly report check failure: {err:#}"
    );
}

#[tokio::test]
async fn test_rclone_backend_move_and_delete_execution() {
    let _mock_bin = get_mock_rclone_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_rclone_mov_del_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let staged_file = temp_dir.join("vod.mp4.part");
    let final_file = temp_dir.join("vod.mp4");
    std::fs::write(&staged_file, b"test content").unwrap();

    let config = RcloneConfig {
        remote_path: "remote:bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);

    // Test move_file
    let staged_str = staged_file.to_string_lossy().to_string();
    let final_str = final_file.to_string_lossy().to_string();
    backend
        .move_file(&staged_str, &final_str)
        .await
        .expect("move_file should succeed");
    assert!(!staged_file.exists());
    assert!(final_file.exists());

    // Test delete_file
    backend
        .delete_file(&final_str)
        .await
        .expect("delete_file should succeed");
    assert!(!final_file.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_rclone_backend_upload_file_to_dest() {
    let _mock_bin = get_mock_rclone_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_rclone_up_dest_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let local_file = temp_dir.join("vod.mp4");
    std::fs::write(&local_file, b"sample mp4 video data").unwrap();

    let remote_dest = temp_dir.join("remote").join("vod.mp4.part");
    let remote_dest_str = remote_dest.to_string_lossy().to_string();

    let config = RcloneConfig {
        remote_path: "remote:bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);

    let progress_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let progress_clone = progress_called.clone();

    let bytes = backend
        .upload_file_to_dest(
            &local_file,
            &remote_dest_str,
            Some(Box::new(move |_uploaded, _total, _speed| {
                progress_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            })),
        )
        .await
        .expect("upload_file_to_dest should succeed");

    assert!(bytes > 0);
    assert!(progress_called.load(std::sync::atomic::Ordering::SeqCst));
    assert!(local_file.exists(), "local file must remain after upload");
    assert!(
        remote_dest.exists(),
        "remote file must be created at remote_dest"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_mock_upload_backend_virtual_remote_store() {
    let temp_dir = std::env::temp_dir().join(format!("test_mock_vstore_{}", rand::random::<u32>()));
    tokio::fs::create_dir_all(&temp_dir).await.unwrap();

    let local_file = temp_dir.join("vod.mp4.part");
    tokio::fs::write(&local_file, b"test virtual store payload 12345")
        .await
        .unwrap();
    let file_len = tokio::fs::metadata(&local_file).await.unwrap().len();

    let mock = MockUploadBackend::new();
    let progress_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let progress_clone = progress_called.clone();

    // 1. Upload to explicit dest
    let uploaded_bytes = mock
        .upload_file_to_dest(
            &local_file,
            "remote:archive/vod.mp4.part",
            Some(Box::new(move |bytes, total, speed| {
                assert_eq!(bytes, file_len);
                assert_eq!(total, file_len);
                assert!(speed > 0.0);
                progress_clone.store(true, Ordering::SeqCst);
            })),
        )
        .await
        .expect("upload_file_to_dest should succeed");

    assert_eq!(uploaded_bytes, file_len);
    assert!(progress_called.load(Ordering::SeqCst));

    // Verify virtual store state
    {
        let files = mock.files.lock().await;
        assert_eq!(files.get("remote:archive/vod.mp4.part"), Some(&file_len));
    }
    {
        let uploads = mock.uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(
            uploads[0],
            (
                local_file.clone(),
                "remote:archive/vod.mp4.part".to_string()
            )
        );
    }

    // 2. Move file (atomic promotion)
    mock.move_file("remote:archive/vod.mp4.part", "remote:archive/vod.mp4")
        .await
        .expect("move_file should succeed");

    {
        let files = mock.files.lock().await;
        assert!(!files.contains_key("remote:archive/vod.mp4.part"));
        assert_eq!(files.get("remote:archive/vod.mp4"), Some(&file_len));
    }

    // Move non-existent source must fail
    let err = mock
        .move_file("remote:archive/nonexistent.mp4", "remote:archive/dst.mp4")
        .await
        .expect_err("moving absent file must fail");
    assert!(format!("{err:#}").contains("not found"));

    // 3. Delete file
    mock.delete_file("remote:archive/vod.mp4")
        .await
        .expect("delete_file should succeed");
    {
        let files = mock.files.lock().await;
        assert!(!files.contains_key("remote:archive/vod.mp4"));
    }

    // 4. Idempotent deletion (deleting absent file succeeds)
    mock.delete_file("remote:archive/vod.mp4")
        .await
        .expect("repeated delete_file must be idempotent");

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

#[tokio::test]
async fn test_mock_upload_backend_delete_files_concurrent() {
    let mock = MockUploadBackend::new();

    // Populate virtual store directly
    {
        let mut files = mock.files.lock().await;
        for i in 0..20 {
            files.insert(format!("remote:bucket/chunks/chunk_{i:04}.ts"), 1024);
        }
        assert_eq!(files.len(), 20);
    }

    // Delete a subset
    let to_delete: Vec<String> = (0..10)
        .map(|i| format!("remote:bucket/chunks/chunk_{i:04}.ts"))
        .collect();

    mock.delete_files(&to_delete)
        .await
        .expect("delete_files should succeed");

    {
        let files = mock.files.lock().await;
        assert_eq!(files.len(), 10);
        for i in 0..10 {
            assert!(!files.contains_key(&format!("remote:bucket/chunks/chunk_{i:04}.ts")));
        }
        for i in 10..20 {
            assert!(files.contains_key(&format!("remote:bucket/chunks/chunk_{i:04}.ts")));
        }
    }

    // Deleting empty slice is a no-op Ok(())
    mock.delete_files(&[])
        .await
        .expect("delete_files on empty slice must succeed");

    // Deleting already deleted files is strictly idempotent
    mock.delete_files(&to_delete)
        .await
        .expect("delete_files on absent files must succeed idempotently");
}

#[tokio::test]
async fn test_mock_upload_backend_failure_injection() {
    let mock = MockUploadBackend::new();
    mock.should_fail.store(true, Ordering::SeqCst);

    let dummy_path = Path::new("test.ts");

    assert!(
        mock.upload_file_to_dest(dummy_path, "remote:dst.ts", None)
            .await
            .is_err()
    );
    assert!(
        mock.move_file("remote:src.ts", "remote:dst.ts")
            .await
            .is_err()
    );
    assert!(mock.delete_file("remote:dst.ts").await.is_err());
    assert!(
        mock.delete_files(&["remote:dst.ts".to_string()])
            .await
            .is_err()
    );
    assert!(mock.check_connection().await.is_err());
    assert!(
        mock.upload_file(dummy_path, "remote_dir", Box::new(|_, _, _| {}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_rclone_backend_optional_progress_none() {
    let _mock_bin = get_mock_rclone_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_rclone_no_cb_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let local_file = temp_dir.join("vod_no_cb.mp4");
    std::fs::write(&local_file, b"sample mp4 video data without callback").unwrap();
    let remote_dest = temp_dir.join("remote").join("vod_no_cb.mp4");
    let remote_dest_str = remote_dest.to_string_lossy().to_string();

    let config = RcloneConfig {
        remote_path: "remote:bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend = RcloneBackend::new(config);

    // Call with None progress callback
    let bytes = backend
        .upload_file_to_dest(&local_file, &remote_dest_str, None)
        .await
        .expect("upload_file_to_dest with None callback should succeed");

    assert!(bytes > 0);
    assert!(remote_dest.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_rclone_backend_check_connection_skip_and_empty() {
    // 1. skip_connection_check: true bypasses subprocess check
    let skip_config = RcloneConfig {
        remote_path: "remote:fail_conn_bucket".to_string(),
        upload_concurrency: 1,
        rclone_bin: "nonexistent_rclone_binary".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: true,
    };
    let backend = RcloneBackend::new(skip_config);
    assert!(
        backend.check_connection().await.is_ok(),
        "check_connection must succeed when skip_connection_check is true"
    );

    // 2. empty remote_path bypasses subprocess check
    let empty_config = RcloneConfig {
        remote_path: "   ".to_string(),
        upload_concurrency: 1,
        rclone_bin: "nonexistent_rclone_binary".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let empty_backend = RcloneBackend::new(empty_config);
    assert!(
        empty_backend.check_connection().await.is_ok(),
        "check_connection must succeed when remote_path is empty"
    );
}

#[tokio::test]
async fn test_rclone_backend_trait_dispatch_and_idempotent_delete() {
    let _mock_bin = get_mock_rclone_bin();
    let temp_dir =
        std::env::temp_dir().join(format!("test_rclone_trait_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    let local_file = temp_dir.join("chunk.ts");
    std::fs::write(&local_file, b"test chunk content").unwrap();

    let staged_file = temp_dir.join("remote").join("chunk.ts.part");
    let final_file = temp_dir.join("remote").join("chunk.ts");

    let staged_str = staged_file.to_string_lossy().to_string();
    let final_str = final_file.to_string_lossy().to_string();

    let config = RcloneConfig {
        remote_path: "remote:bucket".to_string(),
        upload_concurrency: 2,
        rclone_bin: "rclone".to_string(),
        extra_args: vec![],
        consolidate_extra_args: vec![],
        skip_connection_check: false,
    };
    let backend: Arc<dyn UploadBackend> = Arc::new(RcloneBackend::new(config));

    // 1. upload_file_to_dest via trait
    backend
        .upload_file_to_dest(&local_file, &staged_str, None)
        .await
        .expect("trait upload_file_to_dest should succeed");
    assert!(staged_file.exists());

    // 2. move_file via trait
    backend
        .move_file(&staged_str, &final_str)
        .await
        .expect("trait move_file should succeed");
    assert!(!staged_file.exists());
    assert!(final_file.exists());

    // 3. delete_file via trait
    backend
        .delete_file(&final_str)
        .await
        .expect("trait delete_file should succeed");
    assert!(!final_file.exists());

    // 4. delete_file idempotency via trait (target absent, mock returns code 4)
    backend
        .delete_file(&final_str)
        .await
        .expect("trait delete_file on absent target must succeed idempotently");

    // 5. delete_files via trait
    let file_a = temp_dir.join("remote").join("a.ts");
    let file_b = temp_dir.join("remote").join("b.ts");
    std::fs::write(&file_a, b"a").unwrap();
    std::fs::write(&file_b, b"b").unwrap();
    let paths = vec![
        file_a.to_string_lossy().to_string(),
        file_b.to_string_lossy().to_string(),
        final_str, // already absent, must also succeed
    ];
    backend
        .delete_files(&paths)
        .await
        .expect("trait delete_files should succeed");
    assert!(!file_a.exists());
    assert!(!file_b.exists());

    let _ = std::fs::remove_dir_all(&temp_dir);
}
