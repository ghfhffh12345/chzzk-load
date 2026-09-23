use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;

use chzzk_load::tui::app::App;
use chzzk_load::tui::event::{AppEvent, LogEntry};
use chzzk_load::tui::ui::draw_ui;

#[test]
fn test_app_state_mutation_on_events() {
    let mut app = App::new();
    assert_eq!(app.reclaimed_mb, 0.0);

    app.handle_event(AppEvent::Log(LogEntry::info("Hello")));
    assert_eq!(app.logs.len(), 1);
    assert_eq!(app.logs[0], "[INFO] Hello");

    app.handle_event(AppEvent::UploadCompleted {
        channel_id: "c1".to_string(),
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
        channel_id: "c1".to_string(),
        chunk_name: "chunk_0001.ts".to_string(),
        streamer_name: "Streamer 1".to_string(),
        uploaded_bytes: 50_000_000,
        total_bytes: 100_000_000,
        speed_mb_s: 12.5,
    });

    assert_eq!(app.active_upload_name.as_deref(), Some("chunk_0001.ts"));
    assert_eq!(app.upload_progress_pct, 50);
    assert!((app.upload_speed - 12.5).abs() < f64::EPSILON);

    // Test gauge clamping to 100 max
    app.handle_event(AppEvent::UploadProgress {
        channel_id: "c2".to_string(),
        chunk_name: "chunk_0002.ts".to_string(),
        streamer_name: "Streamer 2".to_string(),
        uploaded_bytes: 150_000_000,
        total_bytes: 100_000_000,
        speed_mb_s: 20.0,
    });
    assert_eq!(app.upload_progress_pct, 100);

    // Complete chunk
    app.handle_event(AppEvent::UploadCompleted {
        channel_id: "c1".to_string(),
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
    use chzzk_load::tui::event::{AppEvent, LogEntry, LogKind};

    let mut app = App::new();

    for i in 0..250 {
        app.handle_event(AppEvent::Log(LogEntry::info(format!("Log message {}", i))));
    }

    assert_eq!(app.logs.len(), 200);
    assert_eq!(app.logs.front().unwrap().message, "Log message 50");
    assert_eq!(app.logs.front().unwrap().kind, LogKind::Info);
    assert_eq!(app.logs.back().unwrap().message, "Log message 249");
}

#[test]
fn test_app_log_incoming_does_not_reset_scroll() {
    let mut app = App::new();
    app.log_scroll = 10;
    app.handle_event(AppEvent::Log(LogEntry::info(
        "New message while viewing history",
    )));
    assert_eq!(
        app.log_scroll, 10,
        "Incoming log must not reset user scroll offset"
    );
}

#[test]
fn test_app_upload_failed_transitions_to_next_active_upload() {
    let mut app = App::new();

    app.handle_event(AppEvent::UploadProgress {
        channel_id: "c2".to_string(),
        chunk_name: "chunk_0002.ts".to_string(),
        streamer_name: "Streamer 2".to_string(),
        uploaded_bytes: 20_000_000,
        total_bytes: 80_000_000,
        speed_mb_s: 5.0,
    });

    app.handle_event(AppEvent::UploadProgress {
        channel_id: "c1".to_string(),
        chunk_name: "chunk_0001.ts".to_string(),
        streamer_name: "Streamer 1".to_string(),
        uploaded_bytes: 50_000_000,
        total_bytes: 100_000_000,
        speed_mb_s: 10.0,
    });

    assert_eq!(app.active_upload_name.as_deref(), Some("chunk_0001.ts"));
    assert_eq!(app.upload_progress_pct, 50);
    assert!((app.upload_speed - 10.0).abs() < f64::EPSILON);

    // Failing active upload c1 chunk_0001.ts should transition to c2 chunk_0002.ts
    app.handle_event(AppEvent::UploadFailed {
        channel_id: "c1".to_string(),
        chunk_name: "chunk_0001.ts".to_string(),
    });

    assert_eq!(app.active_upload_name.as_deref(), Some("chunk_0002.ts"));
    assert_eq!(app.upload_progress_pct, 25);
    assert!((app.upload_speed - 5.0).abs() < f64::EPSILON);
}

#[test]
fn test_app_quit_two_stage_shutdown() {
    let mut app = App::new();
    assert!(!app.is_shutting_down);
    assert!(!app.should_quit);

    // First 'q' press triggers shutdown mode, not immediate quit
    let q_key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.is_shutting_down);
    assert!(!app.should_quit);

    // Second 'q' press while in shutdown mode forces immediate quit
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.is_shutting_down);
    assert!(app.should_quit);
}

#[test]
fn test_app_keyboard_navigation_and_quit() {
    let mut app = App::new();
    assert!(!app.should_quit);

    // First 'q' sets is_shutting_down
    let q_key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.is_shutting_down);
    assert!(!app.should_quit);

    // Second 'q' sets should_quit
    app.handle_event(AppEvent::Key(q_key));
    assert!(app.should_quit);

    // Populate channels
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "Channel 1".to_string(),
        is_live: false,
        is_active: false,
        title: "Title 1".to_string(),
    });
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c2".to_string(),
        name: "Channel 2".to_string(),
        is_live: true,
        is_active: false,
        title: "Title 2".to_string(),
    });
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c3".to_string(),
        name: "Channel 3".to_string(),
        is_live: false,
        is_active: false,
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
fn test_draw_ui_rendering_smoke() {
    let backend = TestBackend::new(100, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "Test Streamer".to_string(),
        is_live: true,
        is_active: false,
        title: "Playing Minecraft".to_string(),
    });
    app.active_upload_name = Some("chunk_0001.ts".to_string());
    app.upload_progress_pct = 42;
    app.upload_speed = 5.2;
    app.uploaded_count = 3;
    app.reclaimed_mb = 150.0;
    app.logs
        .push_back(LogEntry::from("[INFO] System initialized"));

    // Verify drawing does not panic
    terminal.draw(|f| draw_ui(f, &app)).unwrap();

    // Check buffer content
    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|cell| cell.symbol()).collect();

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
                app.logs.push_back(LogEntry::from(format!(
                    "[ERROR] Line {} with\nnewline 1\nnewline 2\nnewline 3",
                    i
                )));
            } else if i % 5 == 0 {
                app.logs.push_back(LogEntry::from(format!(
                    "[WARN] Very long log line {} {}",
                    i,
                    "x".repeat(200)
                )));
            } else {
                app.logs
                    .push_back(LogEntry::from(format!("[INFO] Regular log {}", i)));
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
        assert!(
            last_line.contains("q Quit"),
            "Footer missing on {}x{}",
            width,
            height
        );
        assert!(!last_line.contains("[q]"));

        // Verify that the logs panel title is present
        let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(
            content.contains("Live Activity Logs"),
            "Logs title missing on {}x{}",
            width,
            height
        );

        // Verify that latest log (e.g. 99) is visible in tail mode
        assert!(
            content.contains("Regular log 99"),
            "Latest log not visible on {}x{}",
            width,
            height
        );
    }
}

#[test]
fn test_logs_autoscroll_and_pageup_down() {
    let mut app = App::new();
    for i in 0..50 {
        app.logs
            .push_back(LogEntry::from(format!("[INFO] Entry {}", i)));
    }
    assert_eq!(app.log_scroll, 0);

    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();

    // Default: tail mode (scroll = 0), latest log "Entry 49" is rendered
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content.contains("Entry 49"));
    assert!(!content.contains("Scrolled"));

    // PageUp scrolls into history
    let pgup = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::PageUp,
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(pgup));
    assert_eq!(app.log_scroll, 5);

    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_scrolled: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_scrolled.contains("-5"));
    assert!(!content_scrolled.contains("[Scrolled: -5]"));

    // PageDown scrolls back down
    let pgdn = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::PageDown,
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(pgdn));
    assert_eq!(app.log_scroll, 0);

    // End resets scroll to 0
    app.log_scroll = 20;
    let end_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::End,
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(end_key));
    assert_eq!(app.log_scroll, 0);
}

#[test]
fn test_channel_active_status_and_recording_ended() {
    let mut app = App::new();
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "ch_123".to_string(),
        name: "Streamer A".to_string(),
        is_live: true,
        is_active: false,
        title: "Test Stream".to_string(),
    });

    assert!(!app.channels[0].is_active);

    // Recording started sets is_active to true
    app.handle_event(AppEvent::RecordingStarted {
        channel_id: "ch_123".to_string(),
        session_title: "Test Stream".to_string(),
    });
    assert!(app.channels[0].is_active);

    // Recording ended resets is_active to false
    app.handle_event(AppEvent::RecordingEnded {
        channel_id: "ch_123".to_string(),
    });
    assert!(!app.channels[0].is_active);

    // Starting again, then going offline resets is_active to false
    app.handle_event(AppEvent::RecordingStarted {
        channel_id: "ch_123".to_string(),
        session_title: "Test Stream 2".to_string(),
    });
    assert!(app.channels[0].is_active);

    app.handle_event(AppEvent::ChannelUpdate {
        channel_id: "ch_123".to_string(),
        channel_name: "Streamer A".to_string(),
        is_live: false,
        title: "Offline".to_string(),
    });
    assert!(!app.channels[0].is_active);
}

#[test]
fn test_channel_synchronized_scrolling() {
    let mut app = App::new();
    for i in 0..12 {
        app.channels.push(chzzk_load::tui::app::ChannelItem {
            id: format!("ch_{}", i),
            name: format!("Streamer {}", i),
            is_live: true,
            is_active: false,
            title: format!("Title {}", i),
        });
    }

    assert_eq!(app.selected_channel_idx, 0);
    assert_eq!(app.channel_scroll, 0);

    let down_key = KeyEvent::new(KeyCode::Down, KeyModifiers::NONE);
    // Direct scrolling: Down key directly scrolls the views
    for _ in 0..7 {
        app.handle_event(AppEvent::Key(down_key));
    }
    assert_eq!(app.channel_scroll, 7);

    // Scroll down once more to offset 8
    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.channel_scroll, 8);

    // Scroll back up 7 times to offset 1
    let up_key = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
    for _ in 0..7 {
        app.handle_event(AppEvent::Key(up_key));
    }
    assert_eq!(app.channel_scroll, 1);
}

#[test]
fn test_draw_ui_row_alignment_and_active_status() {
    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "StreamerActive".to_string(),
        is_live: true,
        is_active: true,
        title: "Active Game".to_string(),
    });
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c2".to_string(),
        name: "StreamerOffline".to_string(),
        is_live: false,
        is_active: false,
        title: "Offline Title".to_string(),
    });

    app.handle_event(AppEvent::UploadProgress {
        channel_id: "c1".to_string(),
        chunk_name: "chunk_0001.ts".to_string(),
        streamer_name: "StreamerActive".to_string(),
        uploaded_bytes: 10_485_760,
        total_bytes: 20_971_520,
        speed_mb_s: 7.5,
    });

    terminal.draw(|f| draw_ui(f, &app)).unwrap();

    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();

    // Verify ACTIVE tag appears in Monitored Channels
    assert!(content.contains("ACTIVE"));
    assert!(!content.contains("[ ACTIVE ]"));
    assert!(!content.contains("["));
    assert!(content.contains("StreamerActive"));

    // Verify chunk and progress appear in Cloud Upload aligned with c1
    assert!(content.contains("UP"));
    assert!(!content.contains("▲"));
    assert!(!content.contains("chunk_0001.ts"));
    assert!(content.contains("50%"));
    assert!(content.contains("7.5MB/s"));
    assert!(content.contains("━"));
    assert!(content.contains("─"));

    // Verify Stream Recorder panel was removed
    assert!(!content.contains("Stream Recorder"));
}

#[test]
fn test_draw_ui_cloud_upload_visual_badges_and_unstyled_metrics() {
    use ratatui::style::{Color, Modifier};

    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    // c1: Actively uploading
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c1".to_string(),
        name: "Streamer1".to_string(),
        is_live: true,
        is_active: true,
        title: "Live 1".to_string(),
    });
    // c2: Recording (is_active: true, but no active upload)
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c2".to_string(),
        name: "Streamer2".to_string(),
        is_live: true,
        is_active: true,
        title: "Live 2".to_string(),
    });
    // c3: Live standby (is_live: true, is_active: false)
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c3".to_string(),
        name: "Streamer3".to_string(),
        is_live: true,
        is_active: false,
        title: "Live 3".to_string(),
    });
    // c4: Offline (is_live: false, is_active: false)
    app.channels.push(chzzk_load::tui::app::ChannelItem {
        id: "c4".to_string(),
        name: "Streamer4".to_string(),
        is_live: false,
        is_active: false,
        title: "Offline".to_string(),
    });

    app.handle_event(AppEvent::UploadProgress {
        channel_id: "c1".to_string(),
        chunk_name: "chunk_0001.ts".to_string(),
        streamer_name: "Streamer1".to_string(),
        uploaded_bytes: 10_485_760,
        total_bytes: 20_971_520,
        speed_mb_s: 7.5,
    });

    terminal.draw(|f| draw_ui(f, &app)).unwrap();

    let buffer = terminal.backend().buffer();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();

    // 1. Uploading state:
    // Has "UP" badge, no "▲"
    assert!(content.contains("UP"));
    assert!(!content.contains("▲"));
    assert!(!content.contains("●"));
    assert!(!content.contains("○"));
    // Does NOT contain chunk filename in UI
    assert!(!content.contains("chunk_0001.ts"));
    // Uses box drawing characters ━ and ─
    assert!(content.contains("━"));
    assert!(content.contains("─"));
    assert!(!content.contains("█") && !content.contains("░"));
    assert!(!content.contains("▰") && !content.contains("▱"));
    // Contains progress bar symbols, percentage, and compact speed
    assert!(content.contains("50%"));
    assert!(content.contains("7.5MB/s"));

    // 2. Recording state:
    // Has "REC" badge and compact "Staging..." instead of verbose text
    assert!(content.contains("REC"));
    assert!(content.contains("Staging..."));
    assert!(!content.contains("[Recording] Waiting for sealed chunk..."));

    // 3. Live standby state:
    // Has "IDLE" badge and "Standby" instead of verbose text
    assert!(content.contains("IDLE"));
    assert!(content.contains("Standby"));
    assert!(!content.contains("[Idle] Waiting for stream..."));

    // 4. Offline state:
    assert!(content.contains("—"));

    // 5. Colors and Column Alignment:
    // Badges must be padded so that following content starts at the exact same column (offset 5 in Cloud Upload panel).
    let mut found_up_green = false;
    let mut found_rec_red = false;
    let mut found_pct_dim = false;
    let mut checked_up_align = false;
    let mut checked_rec_align = false;
    let mut checked_idle_align = false;

    for y in 0..buffer.area.height {
        let mut row_symbols = String::new();
        for x in 0..buffer.area.width {
            row_symbols.push_str(buffer.cell((x, y)).unwrap().symbol());
        }

        if row_symbols.contains("UP") && row_symbols.contains("50%") {
            // Find x where 'U' starts in this row
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "U" && buffer.cell((x + 1, y)).unwrap().symbol() == "P" {
                    assert_eq!(cell.fg, Color::Green, "UP badge must be green");
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "UP badge must be dimmed"
                    );
                    found_up_green = true;

                    // Whatever follows (progress bar ━) must start at x + 5
                    let next_cell = buffer.cell((x + 5, y)).unwrap();
                    assert_eq!(
                        next_cell.symbol(),
                        "━",
                        "Progress bar must start at offset 5"
                    );
                    checked_up_align = true;
                }
                // Check that '50%' is dimmed (Modifier::DIM)
                if cell.symbol() == "%" {
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "Progress percentage must be dimmed"
                    );
                    found_pct_dim = true;
                }
            }
        }

        if row_symbols.contains("REC") && row_symbols.contains("Staging...") {
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "R" && buffer.cell((x + 1, y)).unwrap().symbol() == "E" {
                    assert_eq!(cell.fg, Color::Red, "REC badge must be red");
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "REC badge must be dimmed"
                    );
                    found_rec_red = true;

                    // Staging... must start at x + 5
                    let next_cell = buffer.cell((x + 5, y)).unwrap();
                    assert_eq!(next_cell.symbol(), "S", "Staging... must start at offset 5");
                    checked_rec_align = true;
                }
            }
        }

        if row_symbols.contains("IDLE") && row_symbols.contains("Standby") {
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "I" && buffer.cell((x + 1, y)).unwrap().symbol() == "D" {
                    // Standby must start at x + 5
                    let next_cell = buffer.cell((x + 5, y)).unwrap();
                    assert_eq!(next_cell.symbol(), "S", "Standby must start at offset 5");
                    checked_idle_align = true;
                }
            }
        }
    }

    assert!(found_up_green, "Did not find UP cell");
    assert!(found_rec_red, "Did not find REC cell");
    assert!(found_pct_dim, "Did not find dimmed % cell");
    assert!(checked_up_align, "Did not check UP column alignment");
    assert!(checked_rec_align, "Did not check REC column alignment");
    assert!(checked_idle_align, "Did not check IDLE column alignment");

    // 6. Monitored Channels: No brackets, padded status text, unstyled streamer name/title
    assert!(!content.contains("[ ACTIVE ]"));
    assert!(!content.contains("[ LIVE ]"));
    assert!(!content.contains("[ OFFLINE ]"));
    assert!(content.contains("ACTIVE"));
    assert!(content.contains("LIVE"));
    assert!(content.contains("OFFLINE"));

    // Verify streamer names are unstyled (Color::Reset) and aligned
    let mut checked_ch_active_align = false;
    let mut checked_ch_live_align = false;
    let mut checked_ch_offline_align = false;

    for y in 0..buffer.area.height {
        let mut row_symbols = String::new();
        for x in 0..buffer.area.width {
            row_symbols.push_str(buffer.cell((x, y)).unwrap().symbol());
        }

        if row_symbols.contains("ACTIVE") && row_symbols.contains("Streamer1") {
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "A" && buffer.cell((x + 1, y)).unwrap().symbol() == "C" {
                    assert_eq!(cell.fg, Color::Cyan, "ACTIVE badge must be cyan");
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "ACTIVE badge must be dimmed"
                    );

                    // Streamer1 must start at x + 8 (ACTIVE padded to 7 + 1 space)
                    let name_cell = buffer.cell((x + 8, y)).unwrap();
                    assert_eq!(
                        name_cell.symbol(),
                        "S",
                        "Streamer name must start at offset 8"
                    );
                    assert_eq!(name_cell.fg, Color::Reset, "Streamer name must be unstyled");
                    checked_ch_active_align = true;
                }
            }
        }

        if row_symbols.contains("LIVE") && row_symbols.contains("Streamer3") {
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "L" && buffer.cell((x + 1, y)).unwrap().symbol() == "I" {
                    assert_eq!(cell.fg, Color::Green, "LIVE badge must be green");
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "LIVE badge must be dimmed"
                    );

                    // Streamer3 must start at x + 8
                    let name_cell = buffer.cell((x + 8, y)).unwrap();
                    assert_eq!(
                        name_cell.symbol(),
                        "S",
                        "Streamer name must start at offset 8"
                    );
                    assert_eq!(name_cell.fg, Color::Reset, "Streamer name must be unstyled");
                    checked_ch_live_align = true;
                }
            }
        }

        if row_symbols.contains("OFFLINE") && row_symbols.contains("Streamer4") {
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if cell.symbol() == "O" && buffer.cell((x + 1, y)).unwrap().symbol() == "F" {
                    assert_eq!(cell.fg, Color::DarkGray, "OFFLINE badge must be dark gray");
                    assert!(
                        cell.modifier.contains(Modifier::DIM),
                        "OFFLINE badge must be dimmed"
                    );

                    // Streamer4 must start at x + 8
                    let name_cell = buffer.cell((x + 8, y)).unwrap();
                    assert_eq!(
                        name_cell.symbol(),
                        "S",
                        "Streamer name must start at offset 8"
                    );
                    assert_eq!(name_cell.fg, Color::Reset, "Streamer name must be unstyled");
                    checked_ch_offline_align = true;
                }
            }
        }
    }

    assert!(
        checked_ch_active_align,
        "Did not check ACTIVE channel alignment"
    );
    assert!(
        checked_ch_live_align,
        "Did not check LIVE channel alignment"
    );
    assert!(
        checked_ch_offline_align,
        "Did not check OFFLINE channel alignment"
    );

    // 7. Verify zero square brackets across the entire screen
    assert!(!content.contains("["), "UI must not contain '[' anywhere");
    assert!(!content.contains("]"), "UI must not contain ']' anywhere");
}

#[test]
fn test_draw_ui_shutdown_banner_rendering() {
    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.reclaimed_mb = 42.5;
    app.uploaded_count = 5;

    // 1. Normal state rendering
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_normal: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    let expected_title = format!("chzzk-load v{}", env!("CARGO_PKG_VERSION"));
    assert!(content_normal.contains(&expected_title));
    assert!(content_normal.contains("q Quit"));
    assert!(!content_normal.contains("SHUTTING DOWN"));

    // 2. Shutdown state rendering
    app.is_shutting_down = true;
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_shutdown: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_shutdown.contains("SHUTTING DOWN"));
    assert!(!content_shutdown.contains("[ SHUTTING DOWN ]"));
    assert!(content_shutdown.contains("Stopping recordings & finishing uploads..."));
    assert!(content_shutdown.contains("Archived: 42.5 MB"));
    assert!(content_shutdown.contains("q / Ctrl+C Force Exit Immediately"));

    // Verify shutdown header retains Yellow and BOLD
    let buffer_shutdown = terminal.backend().buffer();
    let mut found_yellow_shutdown_char = false;
    for x in 0..buffer_shutdown.area.width {
        let cell = buffer_shutdown.cell((x, 0)).unwrap();
        if !cell.symbol().trim().is_empty() {
            assert_eq!(
                cell.fg,
                ratatui::style::Color::Yellow,
                "Shutdown header character '{}' at ({}, 0) must be Yellow",
                cell.symbol(),
                x
            );
            assert!(
                cell.modifier.contains(ratatui::style::Modifier::BOLD),
                "Shutdown header character '{}' at ({}, 0) must be BOLD",
                cell.symbol(),
                x
            );
            found_yellow_shutdown_char = true;
        }
    }
    assert!(found_yellow_shutdown_char);
}

#[test]
fn test_l_key_toggles_logs_and_expands_body() {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    for i in 0..15 {
        app.channels.push(chzzk_load::tui::app::ChannelItem {
            id: format!("c{}", i),
            name: format!("Streamer{:02}", i),
            is_live: true,
            is_active: false,
            title: format!("Title {:02}", i),
        });
    }
    app.logs.push_back(LogEntry::from("[INFO] Log 1"));

    // By default, show_logs is true
    assert!(app.show_logs);
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_with_logs: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_with_logs.contains("Live Activity Logs"));
    assert!(content_with_logs.contains("l Logs"));
    // With logs shown, body height is 9 visible items (00..08)
    assert!(content_with_logs.contains("Streamer00"));
    assert!(content_with_logs.contains("Streamer08"));
    assert!(!content_with_logs.contains("Streamer10"));

    // Press 'l' to toggle logs off
    let l_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Char('l'),
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(l_key));
    assert!(!app.show_logs);

    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_no_logs: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    // Logs panel is hidden
    assert!(!content_no_logs.contains("Live Activity Logs"));
    assert!(!content_no_logs.contains("Log 1"));
    // Channels section expanded dynamically to fill vertical space!
    // Streamer00 through Streamer14 should now all be visible
    assert!(content_no_logs.contains("Streamer00"));
    assert!(content_no_logs.contains("Streamer06"));
    assert!(content_no_logs.contains("Streamer08"));
    assert!(content_no_logs.contains("Streamer14"));

    // Press 'l' again to toggle logs back on
    app.handle_event(AppEvent::Key(l_key));
    assert!(app.show_logs);
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_logs_restored: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(content_logs_restored.contains("Live Activity Logs"));
    assert!(content_logs_restored.contains("l Logs"));
}

#[test]
fn test_no_underline_on_channels_and_direct_view_scrolling() {
    use ratatui::style::Modifier;

    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    for i in 0..10 {
        app.channels.push(chzzk_load::tui::app::ChannelItem {
            id: format!("c{}", i),
            name: format!("Streamer{:02}", i),
            is_live: true,
            is_active: false,
            title: format!("Title {:02}", i),
        });
    }

    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let buffer = terminal.backend().buffer();

    // Verify NO cell has UNDERLINED modifier across the entire buffer
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            let cell = buffer.cell((x, y)).unwrap();
            assert!(
                !cell.modifier.contains(Modifier::UNDERLINED),
                "No cell should be UNDERLINED (found at ({}, {}))",
                x,
                y
            );
        }
    }

    // Direct scrolling interaction:
    assert_eq!(app.channel_scroll, 0);
    let down_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Down,
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.channel_scroll, 1);

    app.handle_event(AppEvent::Key(down_key));
    assert_eq!(app.channel_scroll, 2);

    let up_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Up,
        crossterm::event::KeyModifiers::NONE,
    );
    app.handle_event(AppEvent::Key(up_key));
    assert_eq!(app.channel_scroll, 1);
}

#[test]
fn test_l_key_single_press_toggle_ignores_release_event() {
    use crossterm::event::{KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    let mut app = App::new();
    assert!(app.show_logs);

    let l_press = KeyEvent {
        code: KeyCode::Char('l'),
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    };
    let l_release = KeyEvent {
        code: KeyCode::Char('l'),
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Release,
        state: KeyEventState::NONE,
    };

    // 1. Initial key press toggles logs to false (hidden)
    app.handle_event(AppEvent::Key(l_press));
    assert!(!app.show_logs, "Logs must be hidden after first press");

    // 2. Key release MUST NOT toggle logs back (must remain hidden!)
    app.handle_event(AppEvent::Key(l_release));
    assert!(!app.show_logs, "Logs must remain hidden after key release!");

    // 3. Second key press toggles logs back to true (visible)
    app.handle_event(AppEvent::Key(l_press));
    assert!(app.show_logs, "Logs must be visible after second press");

    // 4. Second key release MUST NOT toggle logs
    app.handle_event(AppEvent::Key(l_release));
    assert!(app.show_logs, "Logs must remain visible after key release!");
}

#[test]
fn test_log_kind_badge_and_style_mappings() {
    use chzzk_load::tui::event::LogKind;
    use chzzk_load::tui::ui::log_kind_badge_and_style;
    use ratatui::style::{Color, Modifier, Style};

    let cases = [
        (
            LogKind::Error,
            " ERROR  ",
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Warn,
            " WARN   ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Clean,
            " CLEAN  ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Rec,
            " REC    ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Ffmpeg,
            " FFMPEG ",
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Drive,
            " DRIVE  ",
            Style::default().fg(Color::Blue).add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Poll,
            " POLL   ",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ),
        (
            LogKind::Info,
            " INFO   ",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ),
    ];

    for (kind, expected_badge, expected_style) in cases {
        let (badge, style) = log_kind_badge_and_style(kind);
        assert_eq!(badge, expected_badge, "Badge mismatch for {:?}", kind);
        assert_eq!(style, expected_style, "Style mismatch for {:?}", kind);
    }
}

#[test]
fn test_draw_ui_renders_all_log_kinds_without_brackets() {
    use chzzk_load::tui::event::{LogEntry, LogKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    let backend = TestBackend::new(120, 24);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.logs
        .push_back(LogEntry::new(LogKind::Error, "Database error"));
    app.logs
        .push_back(LogEntry::new(LogKind::Warn, "High memory"));
    app.logs
        .push_back(LogEntry::new(LogKind::Clean, "Cleaned chunk"));
    app.logs
        .push_back(LogEntry::new(LogKind::Rec, "Started rec"));
    app.logs
        .push_back(LogEntry::new(LogKind::Ffmpeg, "Encoding details"));
    app.logs
        .push_back(LogEntry::new(LogKind::Drive, "Uploading segment"));
    app.logs
        .push_back(LogEntry::new(LogKind::Poll, "Channel poll"));
    app.logs
        .push_back(LogEntry::new(LogKind::Info, "Normal info"));

    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();

    assert!(content.contains("ERROR"));
    assert!(content.contains("Database error"));
    assert!(content.contains("WARN"));
    assert!(content.contains("High memory"));
    assert!(content.contains("CLEAN"));
    assert!(content.contains("Cleaned chunk"));
    assert!(content.contains("REC"));
    assert!(content.contains("Started rec"));
    assert!(content.contains("FFMPEG"));
    assert!(content.contains("Encoding details"));
    assert!(content.contains("DRIVE"));
    assert!(content.contains("Uploading segment"));
    assert!(content.contains("POLL"));
    assert!(content.contains("Channel poll"));
    assert!(content.contains("INFO"));
    assert!(content.contains("Normal info"));

    // Ensure zero square brackets anywhere
    assert!(!content.contains("["));
    assert!(!content.contains("]"));
}

#[test]
fn test_draw_ui_normal_header_style_has_no_color() {
    use ratatui::style::{Color, Modifier};

    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let app = App::new();

    terminal.draw(|f| draw_ui(f, &app)).unwrap();

    let buffer = terminal.backend().buffer();

    let mut header_text = String::new();
    for x in 0..buffer.area.width {
        let cell = buffer.cell((x, 0)).unwrap();
        header_text.push_str(cell.symbol());
        if !cell.symbol().trim().is_empty() {
            assert_eq!(
                cell.fg,
                Color::Reset,
                "Header character '{}' at ({}, 0) must have default/Reset color (no color), found {:?}",
                cell.symbol(),
                x,
                cell.fg
            );
            assert!(
                cell.modifier.contains(Modifier::BOLD),
                "Header character '{}' at ({}, 0) must be BOLD",
                cell.symbol(),
                x
            );
        }
    }
    assert!(header_text.contains("chzzk-load"));
}

#[test]
fn test_app_recording_duration_tracking() {
    let mut app = App::new();
    assert_eq!(app.total_recorded_duration(), std::time::Duration::ZERO);
    assert_eq!(app.format_total_recorded(), "0s");

    // Start recording channel 1
    app.handle_event(AppEvent::RecordingStarted {
        channel_id: "ch_1".to_string(),
        session_title: "Stream 1".to_string(),
    });

    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(app.total_recorded_duration() >= std::time::Duration::from_millis(40));

    // End recording channel 1
    app.handle_event(AppEvent::RecordingEnded {
        channel_id: "ch_1".to_string(),
    });
    let dur_after_end = app.total_recorded_duration();
    assert!(dur_after_end >= std::time::Duration::from_millis(40));

    // Wait a bit and verify total duration does not tick up when no recordings are active
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(app.total_recorded_duration(), dur_after_end);
}

#[test]
fn test_format_duration_helper() {
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(0)),
        "0s"
    );
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(45)),
        "45s"
    );
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(65)),
        "1m 05s"
    );
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(845)),
        "14m 05s"
    );
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(3600)),
        "1h 00m"
    );
    assert_eq!(
        App::format_duration(std::time::Duration::from_secs(11700)),
        "3h 15m"
    );
}

#[test]
fn test_app_format_archived_size() {
    let mut app = App::new();
    app.reclaimed_mb = 0.0;
    assert_eq!(app.format_archived_size(), "0.0 MB");

    app.reclaimed_mb = 142.5;
    assert_eq!(app.format_archived_size(), "142.5 MB");

    app.reclaimed_mb = 1024.0;
    assert_eq!(app.format_archived_size(), "1.00 GB");

    app.reclaimed_mb = 4935.68;
    assert_eq!(app.format_archived_size(), "4.82 GB");
}

#[test]
fn test_draw_ui_header_preset_a_metrics() {
    let backend = TestBackend::new(120, 30);
    let mut terminal = Terminal::new(backend).unwrap();

    let mut app = App::new();
    app.channels = vec![
        chzzk_load::tui::app::ChannelItem {
            id: "ch_1".to_string(),
            name: "Streamer A".to_string(),
            is_live: true,
            is_active: true,
            title: "Live Game".to_string(),
        },
        chzzk_load::tui::app::ChannelItem {
            id: "ch_2".to_string(),
            name: "Streamer B".to_string(),
            is_live: true,
            is_active: false,
            title: "Chatting".to_string(),
        },
        chzzk_load::tui::app::ChannelItem {
            id: "ch_3".to_string(),
            name: "Streamer C".to_string(),
            is_live: false,
            is_active: false,
            title: "Offline".to_string(),
        },
    ];
    app.reclaimed_mb = 1450.0;
    app.total_recorded_duration = std::time::Duration::from_secs(5040); // 1h 24m

    // 1. Normal state rendering
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_normal: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();

    assert!(content_normal.contains("Recording: 1/3"));
    assert!(content_normal.contains("Total Recorded: 1h 24m"));
    assert!(content_normal.contains("Archived: 1.42 GB"));
    assert!(!content_normal.contains("Reclaimed Space"));
    assert!(!content_normal.contains("Chunks Uploaded"));

    // 2. Shutdown state rendering
    app.is_shutting_down = true;
    terminal.draw(|f| draw_ui(f, &app)).unwrap();
    let content_shutdown: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();

    assert!(content_shutdown.contains("SHUTTING DOWN"));
    assert!(content_shutdown.contains("Recording: 1/3"));
    assert!(content_shutdown.contains("Archived: 1.42 GB"));
    assert!(!content_shutdown.contains("Reclaimed:"));
    assert!(!content_shutdown.contains("Uploads:"));
}

#[test]
fn test_draw_ui_dividers_render_correctly_on_various_widths() {
    use ratatui::style::{Color, Modifier};

    let widths = [10, 25, 40, 80, 120];
    for &width in &widths {
        let backend = TestBackend::new(width, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let app = App::new();

        terminal.draw(|f| draw_ui(f, &app)).unwrap();
        let buffer = terminal.backend().buffer();

        // Row 1 is header divider: verify all cells are horizontal rule '─' with Color::DarkGray and NO Modifier::DIM
        for x in 0..width {
            let cell = buffer.cell((x, 1)).unwrap();
            assert_eq!(
                cell.symbol(),
                "─",
                "Header divider at x={} on width={} should be '─'",
                x,
                width
            );
            assert_eq!(
                cell.fg,
                Color::DarkGray,
                "Header divider must have Color::DarkGray"
            );
            assert!(
                !cell.modifier.contains(Modifier::DIM),
                "Header divider must not have Modifier::DIM (which causes invisible lines on dark themes)"
            );
        }
    }
}
