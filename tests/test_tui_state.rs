use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::backend::TestBackend;
use ratatui::Terminal;

use chzzk_load::tui::app::App;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::tui::ui::draw_ui;

#[test]
fn test_app_state_mutation_on_events() {
    let mut app = App::new();
    assert_eq!(app.reclaimed_mb, 0.0);

    app.handle_event(AppEvent::Log("Hello".to_string()));
    assert_eq!(app.logs.len(), 1);
    assert_eq!(app.logs[0], "Hello");

    app.handle_event(AppEvent::UploadCompleted {
        chunk_name: "chunk_0000.ts".to_string(),
        reclaimed_bytes: 524_288_000,
    });
    assert!((app.reclaimed_mb - 500.0).abs() < 1.0);
}

#[test]
fn test_app_channel_updates() {
    let mut app = App::new();
    assert!(app.channels.is_empty());

    // Insert new channel
    app.handle_event(AppEvent::ChannelUpdate {
        channel_id: "ch_123".to_string(),
        channel_name: "Streamer A".to_string(),
        is_live: false,
        title: "Offline Title".to_string(),
    });
    assert_eq!(app.channels.len(), 1);
    assert_eq!(app.channels[0].id, "ch_123");
    assert_eq!(app.channels[0].name, "Streamer A");
    assert!(!app.channels[0].is_live);
    assert_eq!(app.channels[0].title, "Offline Title");

    // Update existing channel
    app.handle_event(AppEvent::ChannelUpdate {
        channel_id: "ch_123".to_string(),
        channel_name: "Streamer A".to_string(),
        is_live: true,
        title: "Going Live!".to_string(),
    });
    assert_eq!(app.channels.len(), 1);
    assert!(app.channels[0].is_live);
    assert_eq!(app.channels[0].title, "Going Live!");

    // Add another channel
    app.handle_event(AppEvent::ChannelUpdate {
        channel_id: "ch_456".to_string(),
        channel_name: "Streamer B".to_string(),
        is_live: true,
        title: "Streamer B Live".to_string(),
    });
    assert_eq!(app.channels.len(), 2);
}

#[test]
fn test_app_upload_progress_and_completion() {
    let mut app = App::new();

    app.handle_event(AppEvent::UploadProgress {
        chunk_name: "chunk_0001.ts".to_string(),
        uploaded_bytes: 50_000_000,
        total_bytes: 100_000_000,
        speed_mb_s: 12.5,
    });

    assert_eq!(app.active_upload_name.as_deref(), Some("chunk_0001.ts"));
    assert_eq!(app.upload_progress_pct, 50);
    assert!((app.upload_speed - 12.5).abs() < f64::EPSILON);

    // Test gauge clamping to 100 max
    app.handle_event(AppEvent::UploadProgress {
        chunk_name: "chunk_0002.ts".to_string(),
        uploaded_bytes: 150_000_000,
        total_bytes: 100_000_000,
        speed_mb_s: 20.0,
    });
    assert_eq!(app.upload_progress_pct, 100);

    // Complete chunk
    app.handle_event(AppEvent::UploadCompleted {
        chunk_name: "chunk_0001.ts".to_string(),
        reclaimed_bytes: 100_000_000,
    });
    assert_eq!(app.active_upload_name, Some("chunk_0002.ts".to_string()));
    assert_eq!(app.uploaded_count, 1);
}

#[test]
fn test_app_refresh_keybinding() {
    let mut app = App::new();
    assert!(!app.refresh_requested);

    let r_key = KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(r_key));
    assert!(app.refresh_requested);
}

#[test]
fn test_app_log_fifo_cap() {
    let mut app = App::new();

    for i in 0..250 {
        app.handle_event(AppEvent::Log(format!("Log message {}", i)));
    }

    assert_eq!(app.logs.len(), 200);
    // Oldest 50 should be discarded; logs[0] should be "Log message 50"
    assert_eq!(app.logs[0], "Log message 50");
    assert_eq!(app.logs[199], "Log message 249");
}

#[test]
fn test_app_keyboard_navigation_and_quit() {
    let mut app = App::new();
    assert!(!app.should_quit);

    // Quit key 'q'
    let q_key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.should_quit);

    // Populate channels
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "Channel 1".to_string(),
        is_live: false,
        title: "Title 1".to_string(),
    });
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c2".to_string(),
        name: "Channel 2".to_string(),
        is_live: true,
        title: "Title 2".to_string(),
    });
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c3".to_string(),
        name: "Channel 3".to_string(),
        is_live: false,
        title: "Title 3".to_string(),
    });

    assert_eq!(app.selected_channel_idx, 0);

    // Down key
    let down_key = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.selected_channel_idx, 1);

    // Down key again
    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.selected_channel_idx, 2);

    // Down key clamped at end
    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.selected_channel_idx, 2);

    // Up key
    let up_key = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(up_key));
    assert_eq!(app.selected_channel_idx, 1);

    // Up key again
    app.handle_event(AppEvent::Key(up_key));
    assert_eq!(app.selected_channel_idx, 0);

    // Up key clamped at 0
    app.handle_event(AppEvent::Key(up_key));
    assert_eq!(app.selected_channel_idx, 0);
}

#[test]
fn test_recording_started_updates_active_stream() {
    let mut app = App::new();
    assert!(app.active_stream.is_none());

    app.handle_event(AppEvent::RecordingStarted {
        channel_id: "ch_abc".to_string(),
        session_title: "Overwatch 2 Stream".to_string(),
    });

    assert!(app.active_stream.is_some());
    let stream_text = app.active_stream.unwrap();
    assert!(stream_text.contains("ch_abc"));
    assert!(stream_text.contains("Overwatch 2 Stream"));
}

#[test]
fn test_draw_ui_rendering_smoke() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "Test Streamer".to_string(),
        is_live: true,
        title: "Playing Minecraft".to_string(),
    });
    app.active_upload_name = Some("chunk_0001.ts".to_string());
    app.upload_progress_pct = 42;
    app.upload_speed = 5.2;
    app.uploaded_count = 3;
    app.reclaimed_mb = 150.0;
    app.logs.push("[INFO] System initialized".to_string());

    // Verify drawing does not panic
    terminal.draw(|f| draw_ui(f, &app)).unwrap();

    // Check buffer content
    let buffer = terminal.backend().buffer();
    let content: String = buffer
        .content()
        .iter()
        .map(|cell| cell.symbol())
        .collect();

    assert!(content.contains("chzzk-load"));
    assert!(content.contains("Monitored Channels"));
    assert!(content.contains("Cloud Upload"));
    assert!(content.contains("Live Activity Logs"));
}

#[test]
fn test_logs_strictly_bounded_and_clipping() {
    // Test on multiple terminal dimensions (small 80x20, standard 80x24, large 120x40)
    for (width, height) in [(80, 20), (80, 24), (120, 40)] {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();

        let mut app = App::new();
        // Add single-line logs, multi-line logs, and very long lines exceeding width
        for i in 0..100 {
            if i % 10 == 0 {
                app.logs.push(format!("[ERROR] Line {} with\nnewline 1\nnewline 2\nnewline 3", i));
            } else if i % 5 == 0 {
                app.logs.push(format!("[WARN] Very long log line {} {}", i, "x".repeat(200)));
            } else {
                app.logs.push(format!("[INFO] Regular log {}", i));
            }
        }

        terminal.draw(|f| draw_ui(f, &app)).unwrap();
        let buffer = terminal.backend().buffer();

        // Verify the entire buffer area matches width and height
        assert_eq!(buffer.area.width, width);
        assert_eq!(buffer.area.height, height);

        // Verify that the footer is rendered at the very last line
        let last_line: String = (0..width)
            .map(|x| buffer.cell((x, height - 1)).unwrap().symbol())
            .collect();
        assert!(last_line.contains("[q] Quit"), "Footer missing on {}x{}", width, height);

        // Verify that the logs panel title is present
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(content.contains("Live Activity Logs"), "Logs title missing on {}x{}", width, height);

        // Verify that latest log (e.g. 99) is visible in tail mode
        assert!(content.contains("Regular log 99"), "Latest log not visible on {}x{}", width, height);
    }
}

#[test]
fn test_logs_autoscroll_and_pageup_down() {
    let mut app = App::new();
    for i in 0..50 {
        app.logs.push(format!("[INFO] Entry {}", i));
    }
    assert_eq!(app.log_scroll, 0);

    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();

    // Default: tail mode (scroll = 0), latest log "Entry 49" is rendered
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content: String = terminal.backend().buffer().content().iter().map(|c| c.symbol()).collect();
    assert!(content.contains("Entry 49"));
    assert!(!content.contains("[Scrolled:"));

    // PageUp scrolls into history
    let pgup = crossterm::event::KeyEvent::new(crossterm::event::KeyCode::PageUp, crossterm::event::KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(pgup));
    assert_eq!(app.log_scroll, 5);

    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_scrolled: String = terminal.backend().buffer().content().iter().map(|c| c.symbol()).collect();
    assert!(content_scrolled.contains("[Scrolled: -5]"));

    // PageDown scrolls back down
    let pgdn = crossterm::event::KeyEvent::new(crossterm::event::KeyCode::PageDown, crossterm::event::KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(pgdn));
    assert_eq!(app.log_scroll, 0);

    // End resets scroll to 0
    app.log_scroll = 20;
    let end_key = crossterm::event::KeyEvent::new(crossterm::event::KeyCode::End, crossterm::event::KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(end_key));
    assert_eq!(app.log_scroll, 0);
}
