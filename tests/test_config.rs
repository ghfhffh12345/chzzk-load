use chzzk_load::config::{GeneralConfig, RcloneConfig, Settings};

#[test]
fn test_general_config_chat_settings_default() {
    let json_data = r#"{}"#;
    let cfg: GeneralConfig =
        serde_json::from_str(json_data).expect("Failed to parse empty general config");
    assert!(cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 30);
}

#[test]
fn test_general_config_chat_settings_custom() {
    let json_data = r#"{
        "record_chat": false,
        "chat_flush_interval_seconds": 60
    }"#;
    let cfg: GeneralConfig =
        serde_json::from_str(json_data).expect("Failed to parse custom chat config");
    assert!(!cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 60);
}

#[test]
fn test_default_rclone_config() {
    let cfg = RcloneConfig::default();
    assert_eq!(cfg.remote_path, "gdrive:Chzzk_Recordings");
    assert_eq!(cfg.upload_concurrency, 3);
    assert_eq!(cfg.rclone_bin, "rclone");
    assert!(cfg.extra_args.is_empty());

    let settings = Settings::default();
    assert_eq!(settings.rclone, cfg);
}

#[test]
fn test_rclone_config_custom_deserialization() {
    let json_data = r#"{
        "remote_path": "onedrive:Recordings",
        "upload_concurrency": 5,
        "rclone_bin": "/usr/local/bin/rclone",
        "extra_args": ["--fast-list", "--transfers=4"]
    }"#;
    let cfg: RcloneConfig =
        serde_json::from_str(json_data).expect("Failed to parse custom rclone config");
    assert_eq!(cfg.remote_path, "onedrive:Recordings");
    assert_eq!(cfg.upload_concurrency, 5);
    assert_eq!(cfg.rclone_bin, "/usr/local/bin/rclone");
    assert_eq!(cfg.extra_args, vec!["--fast-list", "--transfers=4"]);
}

#[test]
fn test_rclone_local_only_mode() {
    let json_data = r#"{
        "remote_path": ""
    }"#;
    let cfg: RcloneConfig =
        serde_json::from_str(json_data).expect("Failed to parse local-only rclone config");
    assert_eq!(cfg.remote_path, "");
    assert_eq!(cfg.upload_concurrency, 3);
    assert_eq!(cfg.rclone_bin, "rclone");
    assert!(cfg.extra_args.is_empty());
}

#[test]
fn test_default_settings_and_serialization() {
    let settings = Settings::default();
    assert_eq!(settings.general.chunk_duration_seconds, 600);
    assert_eq!(settings.general.poll_interval_seconds, 20);
    assert_eq!(settings.rclone.remote_path, "gdrive:Chzzk_Recordings");
    assert_eq!(settings.rclone.upload_concurrency, 3);
    assert_eq!(settings.channels.len(), 1);

    let json_str = serde_json::to_string_pretty(&settings).expect("Serialize to json");
    let deserialized: Settings = serde_json::from_str(&json_str).expect("Deserialize from json");
    assert_eq!(deserialized.general.chunk_duration_seconds, 600);
    assert_eq!(deserialized.rclone.upload_concurrency, 3);
}

#[test]
fn test_load_or_create_creates_file_if_missing() {
    let temp_dir = std::env::temp_dir().join(format!("chzzk_test_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("settings.json");

    assert!(!config_path.exists());
    let settings = Settings::load_or_create_default(&config_path).expect("Create default");
    assert!(config_path.exists());
    assert_eq!(settings.general.chunk_duration_seconds, 600);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
