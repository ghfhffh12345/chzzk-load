use chzzk_load::config::{ChannelConfig, GeneralConfig, RcloneConfig, Settings};

#[test]
fn test_general_config_chat_settings_default() {
    let toml_data = r#""#;
    let cfg: GeneralConfig =
        toml::from_str(toml_data).expect("Failed to parse empty general config");
    assert!(cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 30);
}

#[test]
fn test_general_config_chat_settings_custom() {
    let toml_data = r#"
        record_chat = false
        chat_flush_interval_seconds = 60
    "#;
    let cfg: GeneralConfig = toml::from_str(toml_data).expect("Failed to parse custom chat config");
    assert!(!cfg.record_chat);
    assert_eq!(cfg.chat_flush_interval_seconds, 60);
}

#[test]
fn test_default_rclone_config() {
    let cfg = RcloneConfig::default();
    assert_eq!(cfg.remote_path, "remote:chzzk");
    assert_eq!(cfg.upload_concurrency, 3);
    assert_eq!(cfg.rclone_bin, "rclone");
    assert!(cfg.extra_args.is_empty());

    let settings = Settings::default();
    assert_eq!(settings.rclone, cfg);
}

#[test]
fn test_rclone_config_custom_deserialization() {
    let toml_data = r#"
        remote_path = "onedrive:Recordings"
        upload_concurrency = 5
        rclone_bin = "/usr/local/bin/rclone"
        extra_args = ["--fast-list", "--transfers=4"]
    "#;
    let cfg: RcloneConfig =
        toml::from_str(toml_data).expect("Failed to parse custom rclone config");
    assert_eq!(cfg.remote_path, "onedrive:Recordings");
    assert_eq!(cfg.upload_concurrency, 5);
    assert_eq!(cfg.rclone_bin, "/usr/local/bin/rclone");
    assert_eq!(cfg.extra_args, vec!["--fast-list", "--transfers=4"]);
}

#[test]
fn test_rclone_local_only_mode() {
    let toml_data = r#"
        remote_path = ""
    "#;
    let cfg: RcloneConfig =
        toml::from_str(toml_data).expect("Failed to parse local-only rclone config");
    assert_eq!(cfg.remote_path, "");
    assert_eq!(cfg.upload_concurrency, 3);
    assert_eq!(cfg.rclone_bin, "rclone");
    assert!(cfg.extra_args.is_empty());
}

#[test]
fn test_channel_config_shorthand_string_deserialization() {
    let toml_data = r#"
        channels = [
            "dc7fb0d085cfbbe90e11836e3b85b784",
            "c8adce2ff4a3618931e07c327e1fa070",
        ]
    "#;
    #[derive(serde::Deserialize)]
    struct Wrapper {
        channels: Vec<ChannelConfig>,
    }
    let parsed: Wrapper =
        toml::from_str(toml_data).expect("Failed to parse shorthand string channels");
    assert_eq!(parsed.channels.len(), 2);
    assert_eq!(parsed.channels[0].id, "dc7fb0d085cfbbe90e11836e3b85b784");
    assert_eq!(parsed.channels[0].alias, None);
    assert_eq!(parsed.channels[1].id, "c8adce2ff4a3618931e07c327e1fa070");
    assert_eq!(parsed.channels[1].alias, None);
}

#[test]
fn test_channel_config_table_with_and_without_alias() {
    let toml_data = r#"
        [[channels]]
        id = "dc7fb0d085cfbbe90e11836e3b85b784"
        alias = "Soyeon"

        [[channels]]
        id = "c8adce2ff4a3618931e07c327e1fa070"
    "#;
    #[derive(serde::Deserialize)]
    struct Wrapper {
        channels: Vec<ChannelConfig>,
    }
    let parsed: Wrapper = toml::from_str(toml_data).expect("Failed to parse table channels");
    assert_eq!(parsed.channels.len(), 2);
    assert_eq!(parsed.channels[0].id, "dc7fb0d085cfbbe90e11836e3b85b784");
    assert_eq!(parsed.channels[0].alias.as_deref(), Some("Soyeon"));
    assert_eq!(parsed.channels[1].id, "c8adce2ff4a3618931e07c327e1fa070");
    assert_eq!(parsed.channels[1].alias, None);
}

#[test]
fn test_settings_toml_roundtrip() {
    let settings = Settings::default();
    let toml_str = toml::to_string_pretty(&settings).expect("Serialize to toml");
    let deserialized: Settings = toml::from_str(&toml_str).expect("Deserialize from toml");
    assert_eq!(deserialized.general.chunk_duration_seconds, 600);
    assert_eq!(deserialized.channels.len(), 1);
    assert_eq!(
        deserialized.channels[0].id,
        "4c3b44869c9b1399723ec28ec236f736"
    );
    assert_eq!(
        deserialized.channels[0].alias.as_deref(),
        Some("SampleStreamer")
    );
}

#[test]
fn test_load_or_create_creates_settings_toml() {
    let temp_dir = std::env::temp_dir().join(format!("chzzk_toml_test_{}", rand::random::<u32>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config_path = temp_dir.join("settings.toml");

    assert!(!config_path.exists());
    let settings =
        Settings::load_or_create_default(&config_path).expect("Create default settings.toml");
    assert!(config_path.exists());
    assert_eq!(settings.general.chunk_duration_seconds, 600);

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("[general]"));
    assert!(content.contains("[rclone]"));
    assert!(content.contains("[[channels]]"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
