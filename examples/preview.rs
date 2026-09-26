use std::collections::{HashMap, VecDeque};
use std::io::stdout;
use std::time::{Duration, Instant};

use chzzk_load::tui::ConsoleCodePageGuard;
use chzzk_load::tui::app::{ActiveUpload, App, ChannelItem};
use chzzk_load::tui::event::{AppEvent, LogEntry};
use chzzk_load::tui::ui::draw_ui;
use crossterm::cursor::{Hide, Show};
use crossterm::event::{Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize Windows UTF-8 console codepage guard
    let _console_guard = ConsoleCodePageGuard::init();

    // 2. Set terminal panic recovery hook
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen, Show);
        ConsoleCodePageGuard::restore_original();
        default_panic(panic_info);
    }));

    let mut app = App::new();

    let channel_hane = "a9a343510e132ea3026ff3cf682820b5".to_string();
    let channel_lilpa = "b1a234567890abcdef1234567890abcd".to_string();
    let channel_kangqui = "1a1dd9ce56fb61a37ffb6f69f6d5b978".to_string();
    let channel_wakgood = "c3d4e5f67890abcdef1234567890abcd".to_string();
    let channel_gosegu = "d5e6f7a89012bcde34567890abcdef12".to_string();
    let channel_looksam = "8803cee946a9e610a76fbdee98d98c61".to_string();
    let channel_poong = "7ce8032370ac5121dcabce7bad375ced".to_string();
    let channel_calmdown = "e1f2a3b4c5d6e7f809123456789abcdef".to_string();
    let channel_nokduro = "f2a3b4c5d6e7f809123456789abcdef01".to_string();
    let channel_ddahyoni = "09123456789abcdef0123456789abcdef".to_string();
    let channel_okja = "123456789abcdef0123456789abcdef01".to_string();
    let channel_cheol = "23456789abcdef0123456789abcdef012".to_string();

    app.channels = vec![
        ChannelItem {
            id: channel_hane.clone(),
            name: "하네 | Hane".to_string(),
            is_live: true,
            is_active: true,
            title: "쌀먹쥐 ~~~쌀쌀의 생활(봉누도2)".to_string(),
            chat_count: 1420,
        },
        ChannelItem {
            id: channel_lilpa.clone(),
            name: "너불".to_string(),
            is_live: true,
            is_active: true,
            title: "교정야호 교통정비공사 사장 황인정".to_string(),
            chat_count: 852,
        },
        ChannelItem {
            id: channel_kangqui.clone(),
            name: "강퀴".to_string(),
            is_live: true,
            is_active: false,
            title: "엄마한턴만더하고끌게 / 1루트 클래식 하드 3부".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_wakgood.clone(),
            name: "계춘회".to_string(),
            is_live: true,
            is_active: false,
            title: "고춘애오늘며칠차임 아무튼출근".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_gosegu.clone(),
            name: "텐코 시부키".to_string(),
            is_live: true,
            is_active: false,
            title: "봉누도 정지해입니다".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_looksam.clone(),
            name: "룩삼".to_string(),
            is_live: false,
            is_active: false,
            title: "룩삼 닌텐도 지휘자 게임".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_poong.clone(),
            name: "풍월량".to_string(),
            is_live: false,
            is_active: false,
            title: "wow 포에버".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_calmdown.clone(),
            name: "침착맨".to_string(),
            is_live: false,
            is_active: false,
            title: "침착맨의 일상 토크".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_nokduro.clone(),
            name: "녹두로".to_string(),
            is_live: false,
            is_active: false,
            title: "슈퍼 마리오 메이커 2 익스트림".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_ddahyoni.clone(),
            name: "따효니".to_string(),
            is_live: false,
            is_active: false,
            title: "하스스톤 신규 확장팩 덱 메이킹".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_okja.clone(),
            name: "카가야키 노바".to_string(),
            is_live: false,
            is_active: false,
            title: "봉누도2 경찰 김억척 8일차 ".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_cheol.clone(),
            name: "철면수심".to_string(),
            is_live: false,
            is_active: false,
            title: "전장 1등 도전기".to_string(),
            chat_count: 0,
        },
    ];

    // Top status metrics & active recording starts
    let now = Instant::now();
    app.active_recording_starts.insert(
        channel_hane.clone(),
        now - Duration::from_secs(2 * 3600 + 15 * 60 + 42),
    );
    app.active_recording_starts.insert(
        channel_lilpa.clone(),
        now - Duration::from_secs(48 * 60 + 19),
    );
    app.reclaimed_mb = 4120.8;
    app.uploaded_count = 141;

    // Concurrent multi-stream upload simulation (v0.5.0 upload_concurrency)
    let mut uploads = HashMap::new();
    uploads.insert(
        channel_hane.clone(),
        ActiveUpload {
            channel_id: channel_hane.clone(),
            chunk_name: "chunk_0142.ts".to_string(),
            streamer_name: "하네 | Hane".to_string(),
            uploaded_bytes: (18.4 * 1024.0 * 1024.0) as u64,
            total_bytes: (28.4 * 1024.0 * 1024.0) as u64,
            speed_mb_s: 8.5,
        },
    );
    uploads.insert(
        channel_lilpa.clone(),
        ActiveUpload {
            channel_id: channel_lilpa.clone(),
            chunk_name: "chunk_0048.ts".to_string(),
            streamer_name: "너불".to_string(),
            uploaded_bytes: (8.9 * 1024.0 * 1024.0) as u64,
            total_bytes: (27.8 * 1024.0 * 1024.0) as u64,
            speed_mb_s: 7.2,
        },
    );
    app.active_uploads = uploads;

    app.logs = VecDeque::from([
        LogEntry::info("Google Drive authenticated successfully (root: 'Chzzk_Recordings')"),
        LogEntry::rec("Spawned FFmpeg segmenter (600s TS chunks) -> recordings/a9a34351_20260926"),
        LogEntry::rec("Direct CDN stream extracted: 1080p single-variant (p2p bypass)"),
        LogEntry::drive("Session folder ready: 'Chzzk_Recordings/2026-09-26 하네 - 쌀먹쥐'"),
        LogEntry::chat("Connected to live chat WebSocket (kr-ss1.chat.naver.com)"),
        LogEntry::chat("Buffered 500 messages (64 KB). Flushed to chat.jsonl"),
        LogEntry::rec("chunk_0140.ts sealed. Pushed to Drive upload queue."),
        LogEntry::clean("Uploaded & deleted chunk_0140.ts (reclaimed 27.9 MB)"),
        LogEntry::rec("chunk_0141.ts sealed. Pushed to Drive upload queue."),
        LogEntry::clean("Uploaded & deleted chunk_0141.ts (reclaimed 28.1 MB)"),
        LogEntry::rec("Spawned FFmpeg segmenter (600s TS chunks) -> recordings/b1a23456_20260926"),
        LogEntry::drive("Initialized 'title_history.txt' in Drive folder for 너불"),
        LogEntry::rec("chunk_0142.ts sealed. Pushed to Drive upload queue."),
        LogEntry::rec("chunk_0048.ts sealed. Pushed to Drive upload queue."),
        LogEntry::info("Monitored channels synced: 12 total, 5 live, 2 recording"),
    ]);

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        Hide,
        crossterm::terminal::SetTitle("chzzk-load preview (Ratatui TUI)")
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut event_reader = crossterm::event::EventStream::new();
    let mut anim_interval = tokio::time::interval(Duration::from_millis(150));
    let mut chat_interval = tokio::time::interval(Duration::from_secs(3));
    let mut needs_redraw = true;

    let mut chunk_counter_hane = 143;
    let mut chunk_counter_lilpa = 49;
    let mut chat_flush_cycle = 0;

    loop {
        if needs_redraw {
            terminal.draw(|f| draw_ui(f, &app))?;
            needs_redraw = false;
        }

        tokio::select! {
            biased;

            // 1. Process crossterm input events asynchronously
            Some(item) = event_reader.next() => {
                if let Ok(Event::Key(key)) = item
                    && key.kind != crossterm::event::KeyEventKind::Release
                {
                    if key.code == KeyCode::Esc
                        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        break;
                    }
                    if key.code == KeyCode::Char('q') {
                        if app.is_shutting_down {
                            break;
                        } else {
                            app.is_shutting_down = true;
                            app.logs.push_back(LogEntry::warn(
                                "Shutdown initiated. Completing active uploads... (press 'q' again to exit)",
                            ));
                        }
                    } else if key.code == KeyCode::Char('r') {
                        app.logs.push_back(LogEntry::info("Manual refresh triggered: polled 12 monitored channels."));
                    } else {
                        app.handle_event(AppEvent::Key(key));
                    }
                    needs_redraw = true;
                }
            }

            // 2. Animate concurrent multi-stream uploads smoothly
            _ = anim_interval.tick() => {
                // Animate channel_hane upload
                if let Some(upload) = app.active_uploads.get_mut(&channel_hane) {
                    let increment = (0.45 * 1024.0 * 1024.0) as u64;
                    upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                    if upload.uploaded_bytes >= upload.total_bytes {
                        app.uploaded_count += 1;
                        app.reclaimed_mb += 28.4;
                        app.logs.push_back(LogEntry::clean(format!(
                            "Uploaded & deleted chunk_{:04}.ts (reclaimed 28.4 MB)",
                            chunk_counter_hane - 1
                        )));
                        upload.chunk_name = format!("chunk_{:04}.ts", chunk_counter_hane);
                        upload.uploaded_bytes = (1.2 * 1024.0 * 1024.0) as u64;
                        upload.total_bytes = (28.4 * 1024.0 * 1024.0) as u64;
                        chunk_counter_hane += 1;
                    }
                }

                // Animate channel_lilpa upload
                if let Some(upload) = app.active_uploads.get_mut(&channel_lilpa) {
                    let increment = (0.35 * 1024.0 * 1024.0) as u64;
                    upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                    if upload.uploaded_bytes >= upload.total_bytes {
                        app.uploaded_count += 1;
                        app.reclaimed_mb += 27.8;
                        app.logs.push_back(LogEntry::clean(format!(
                            "Uploaded & deleted chunk_{:04}.ts (reclaimed 27.8 MB)",
                            chunk_counter_lilpa - 1
                        )));
                        upload.chunk_name = format!("chunk_{:04}.ts", chunk_counter_lilpa);
                        upload.uploaded_bytes = (0.8 * 1024.0 * 1024.0) as u64;
                        upload.total_bytes = (27.8 * 1024.0 * 1024.0) as u64;
                        chunk_counter_lilpa += 1;
                    }
                }

                needs_redraw = true;
            }

            // 3. Simulate periodic live chat telemetry updates
            _ = chat_interval.tick() => {
                if let Some(ch) = app.channels.iter_mut().find(|c| c.id == channel_hane) {
                    ch.chat_count += 3;
                }
                if let Some(ch) = app.channels.iter_mut().find(|c| c.id == channel_lilpa) {
                    ch.chat_count += 2;
                }
                chat_flush_cycle += 1;
                if chat_flush_cycle % 5 == 0 {
                    app.logs.push_back(LogEntry::chat("Buffered 500 messages (64 KB). Flushed to chat.jsonl"));
                }
                needs_redraw = true;
            }
        }

        if app.should_quit {
            break;
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, Show)?;
    terminal.show_cursor()?;

    Ok(())
}
