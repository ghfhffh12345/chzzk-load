use chzzk_load::config::Settings;

#[test]
fn test_default_settings_and_serialization() {
    let settings = Settings::default();
    assert_eq!(settings.general.chunk_duration_seconds, 600);
    assert_eq!(settings.general.poll_interval_seconds, 20);
    assert_eq!(settings.google_drive.root_folder_name, "Chzzk_Recordings");
    assert_eq!(settings.channels.len(), 1);

    let json_str = serde_json::to_string_pretty(&settings).expect("Serialize to json");
    let deserialized: Settings = serde_json::from_str(&json_str).expect("Deserialize from json");
    assert_eq!(deserialized.general.chunk_duration_seconds, 600);
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
