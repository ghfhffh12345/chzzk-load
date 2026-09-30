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
    let channel_pokijjang = "c8adce2ff4a3618931e07c327e1fa070".to_string();
    let channel_gangsoyeon = "dc7fb0d085cfbbe90e11836e3b85b784".to_string();
    let channel_wakgood = "c3d4e5f67890abcdef1234567890abcd".to_string();
    let channel_gosegu = "d5e6f7a89012bcde34567890abcdef12".to_string();
    let channel_looksam = "8803cee946a9e610a76fbdee98d98c61".to_string();
    let channel_poong = "7ce8032370ac5121dcabce7bad375ced".to_string();
    let channel_calmdown = "e1f2a3b4c5d6e7f809123456789abcdef".to_string();
    let channel_nokduro = "f2a3b4c5d6e7f809123456789abcdef01".to_string();
    let channel_ddahyoni = "09123456789abcdef0123456789abcdef".to_string();

    // Showcases all channel states: ACTIVE (with live chat telemetry), LIVE (standby), and OFFLINE
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
            is_active: true,
            title: "엄마한턴만더하고끌게 / 1루트 클래식 하드 3부".to_string(),
            chat_count: 318,
        },
        ChannelItem {
            id: channel_pokijjang.clone(),
            name: "포키쨩".to_string(),
            is_live: true,
            is_active: false,
            title: "[치지직] 노래하고 노는 방송 (신곡 녹음 후기)".to_string(),
            chat_count: 0,
        },
        ChannelItem {
            id: channel_gangsoyeon.clone(),
            name: "강소연".to_string(),
            is_live: true,
            is_active: false,
            title: "롤 솔랭 다이아 승급전 도전기".to_string(),
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
            is_live: false,
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
    ];

    // Top status metrics & active recording session starts
    let now = Instant::now();
    app.active_recording_starts.insert(
        channel_hane.clone(),
        now - Duration::from_secs(2 * 3600 + 15 * 60 + 42),
    );
    app.active_recording_starts.insert(
        channel_lilpa.clone(),
        now - Duration::from_secs(48 * 60 + 19),
    );
    app.active_recording_starts.insert(
        channel_kangqui.clone(),
        now - Duration::from_secs(14 * 60 + 35),
    );
    app.reclaimed_mb = 4120.8;
    app.uploaded_count = 141;

    // Showcases diverse Cloud Upload states simultaneously:
    // 1. "UP" (Green) with progress bar & metrics: channel_hane and channel_lilpa
    // 2. "REC" (Red) with "Staging...": channel_kangqui (active recording, waiting for chunk N+1 seal)
    // 3. "IDLE" (Muted Gray) with "Standby": channel_pokijjang, channel_gangsoyeon, channel_wakgood (live but unrecorded)
    // 4. " —" (Muted Gray): offline channels
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
    // Note: channel_kangqui is actively recording (is_active: true) but not in active_uploads,
    // showcasing the "REC" (Staging...) badge in the Cloud Upload panel!
    app.active_uploads = uploads;

    // Showcases all 9 distinct log kinds with their unique badge colors & messages:
    // [INFO] (Muted Gray), [CLOUD] (Blue), [POLL] (Muted Gray), [REC] (Cyan),
    // [FFMPEG] (Magenta), [CHAT] (Cyan), [CLEAN] (Green), [WARN] (Yellow), [ERROR] (Red)
    // Streamer names are prepended to chunk and chat event logs.
    app.logs = VecDeque::from([
        LogEntry::info(
            "Loaded configuration from 'settings.toml' (upload_concurrency: 3, record_chat: true)",
        ),
        LogEntry::cloud("Rclone remote 'gdrive:chzzk' verified successfully (v1.68.0)"),
        LogEntry::poll("Polling 12 monitored channels (cycle #142): 6 online, 3 recording"),
        LogEntry::rec(
            "[하네 | Hane] Spawned FFmpeg segmenter (60s TS chunks) -> recordings/[2026-09-30_1158] [Hane] 하네 - 쌀먹쥐 ~~~쌀쌀의 생활(봉누도2)",
        ),
        LogEntry::rec(
            "[하네 | Hane] Direct CDN stream extracted: 1080p single-variant (p2p bypass)",
        ),
        LogEntry::cloud(
            "Remote folder ready: 'gdrive:chzzk/[2026-09-30_1158] [Hane] 하네 - 쌀먹쥐 ~~~쌀쌀의 생활(봉누도2)'",
        ),
        LogEntry::chat("[하네 | Hane] Connected to live chat WebSocket (kr-ss1.chat.naver.com)"),
        LogEntry::ffmpeg(
            "frame= 1800 fps= 60 q=-1.0 size= 28416kB time=00:01:00.00 bitrate=3878.4kbits/s speed=0.999x",
        ),
        LogEntry::chat("[하네 | Hane] Buffered 500 messages (64 KB). Flushed to chat_0000.jsonl"),
        LogEntry::rec("[하네 | Hane] chunk_0140.ts sealed. Pushed to cloud upload queue."),
        LogEntry::clean("[하네 | Hane] Uploaded & deleted chunk_0140.ts (reclaimed 27.9 MB)"),
        LogEntry::rec(
            "[너불] Spawned FFmpeg segmenter (60s TS chunks) -> recordings/[2026-09-30_1158] 너불 - 교정야호 교통정비공사 사장 황인정",
        ),
        LogEntry::cloud("Initialized 'title_history.txt' on remote storage for 너불"),
        LogEntry::chat("[너불] Connected to live chat WebSocket (kr-ss2.chat.naver.com)"),
        LogEntry::rec("[너불] chunk_0047.ts sealed. Pushed to cloud upload queue."),
        LogEntry::clean("[너불] Uploaded & deleted chunk_0047.ts (reclaimed 27.8 MB)"),
        LogEntry::chat("[너불] Buffered 500 messages (64 KB). Flushed to chat_0000.jsonl"),
        LogEntry::warn(
            "Stream cooldown active for channel a9a34351 (live_id '3829140' deduplicated, 14s remaining)",
        ),
        LogEntry::error(
            "Recording unavailable for channel dc7fb0d085cfbbe90e11836e3b85b784 (강소연): 19+ age-restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
        ),
        LogEntry::rec("[하네 | Hane] chunk_0142.ts sealed. Pushed to cloud upload queue."),
        LogEntry::rec("[너불] chunk_0048.ts sealed. Pushed to cloud upload queue."),
        LogEntry::rec("[강퀴] Initializing segmenter for chunk_0001.ts (Staging...)"),
        LogEntry::poll(
            "Channel status update: '포키쨩' OPEN -> CDN 1080p stream detected (standby)",
        ),
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
    let mut sim_interval = tokio::time::interval(Duration::from_secs(7));
    // Consume initial ticks so initial startup logs remain cleanly visible
    chat_interval.tick().await;
    sim_interval.tick().await;

    let mut needs_redraw = true;

    let mut chunk_counter_hane = 143;
    let mut chunk_counter_lilpa = 49;
    let mut chunk_counter_kangqui = 1;

    // Staging counters: Some(ticks) indicates the channel is currently staging chunk N+1 (REC badge)
    let mut staging_hane: Option<u32> = None;
    let mut staging_lilpa: Option<u32> = None;
    let mut staging_kangqui: Option<u32> = Some(30); // starts in staging (REC) for ~4.5 seconds

    let mut chat_flush_cycle = 0;
    let mut sim_cycle = 0;

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
                            app.handle_event(AppEvent::Log(LogEntry::warn(
                                "Shutdown initiated. Completing active uploads... (press 'q' again to exit)",
                            )));
                        }
                    } else if key.code == KeyCode::Char('r') {
                        app.handle_event(AppEvent::Log(LogEntry::poll(
                            "Manual refresh triggered: polling 12 monitored channels...",
                        )));
                        app.handle_event(AppEvent::Log(LogEntry::info(
                            "Poll complete: 12 configured, 6 live, 3 recording",
                        )));
                    } else if key.code == KeyCode::Char('e') {
                        app.handle_event(AppEvent::Log(LogEntry::error(
                            "Recording unavailable for channel dc7fb0d085cf (강소연): 19+ age-restricted stream requires valid Naver credentials (nid_aut, nid_ses)",
                        )));
                    } else if key.code == KeyCode::Char('w') {
                        app.handle_event(AppEvent::Log(LogEntry::warn(
                            "Stream cooldown active for channel a9a34351 (live_id '3829140' deduplicated, 18s remaining)",
                        )));
                    } else if key.code == KeyCode::Char('f') {
                        app.handle_event(AppEvent::Log(LogEntry::ffmpeg(
                            "frame= 7200 fps= 60 q=-1.0 size= 113664kB time=00:04:00.00 bitrate=3879.5kbits/s speed=1.000x",
                        )));
                    } else if key.code == KeyCode::Char('c') {
                        app.handle_event(AppEvent::Log(LogEntry::chat(
                            "[하네 | Hane] Live chat donation captured: 5,000 Cheese from '치즈도둑'",
                        )));
                    } else if key.code == KeyCode::Char('p') {
                        app.handle_event(AppEvent::Log(LogEntry::poll(
                            "Polled channel '포키쨩': title changed to '신곡 녹음 후기 & 잡담'",
                        )));
                    } else {
                        app.handle_event(AppEvent::Key(key));
                    }
                    needs_redraw = true;
                }
            }

            // 2. Animate concurrent multi-stream uploads and smooth transitions between UP and REC (Staging...)
            _ = anim_interval.tick() => {
                // Channel Hane: upload or stage
                if let Some(ticks) = staging_hane {
                    if ticks > 0 {
                        staging_hane = Some(ticks - 1);
                    } else {
                        staging_hane = None;
                        app.handle_event(AppEvent::Log(LogEntry::rec(format!(
                            "[하네 | Hane] chunk_{chunk_counter_hane:04}.ts sealed. Pushed to cloud upload queue."
                        ))));
                        app.active_uploads.insert(
                            channel_hane.clone(),
                            ActiveUpload {
                                channel_id: channel_hane.clone(),
                                chunk_name: format!("chunk_{chunk_counter_hane:04}.ts"),
                                streamer_name: "하네 | Hane".to_string(),
                                uploaded_bytes: 0,
                                total_bytes: (28.4 * 1024.0 * 1024.0) as u64,
                                speed_mb_s: 8.5,
                            },
                        );
                        chunk_counter_hane += 1;
                    }
                } else if let Some(upload) = app.active_uploads.get_mut(&channel_hane) {
                    let increment = (0.45 * 1024.0 * 1024.0) as u64;
                    upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                    if upload.uploaded_bytes >= upload.total_bytes {
                        app.uploaded_count += 1;
                        app.reclaimed_mb += 28.4;
                        let finished_chunk = upload.chunk_name.clone();
                        app.handle_event(AppEvent::Log(LogEntry::clean(format!(
                            "[하네 | Hane] Uploaded & deleted {finished_chunk} (reclaimed 28.4 MB)"
                        ))));
                        app.active_uploads.remove(&channel_hane);
                        staging_hane = Some(35); // Transitions to REC (Staging...) for ~5.2s
                    }
                }

                // Channel Lilpa: upload or stage
                if let Some(ticks) = staging_lilpa {
                    if ticks > 0 {
                        staging_lilpa = Some(ticks - 1);
                    } else {
                        staging_lilpa = None;
                        app.handle_event(AppEvent::Log(LogEntry::rec(format!(
                            "[너불] chunk_{chunk_counter_lilpa:04}.ts sealed. Pushed to cloud upload queue."
                        ))));
                        app.active_uploads.insert(
                            channel_lilpa.clone(),
                            ActiveUpload {
                                channel_id: channel_lilpa.clone(),
                                chunk_name: format!("chunk_{chunk_counter_lilpa:04}.ts"),
                                streamer_name: "너불".to_string(),
                                uploaded_bytes: 0,
                                total_bytes: (27.8 * 1024.0 * 1024.0) as u64,
                                speed_mb_s: 7.2,
                            },
                        );
                        chunk_counter_lilpa += 1;
                    }
                } else if let Some(upload) = app.active_uploads.get_mut(&channel_lilpa) {
                    let increment = (0.35 * 1024.0 * 1024.0) as u64;
                    upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                    if upload.uploaded_bytes >= upload.total_bytes {
                        app.uploaded_count += 1;
                        app.reclaimed_mb += 27.8;
                        let finished_chunk = upload.chunk_name.clone();
                        app.handle_event(AppEvent::Log(LogEntry::clean(format!(
                            "[너불] Uploaded & deleted {finished_chunk} (reclaimed 27.8 MB)"
                        ))));
                        app.active_uploads.remove(&channel_lilpa);
                        staging_lilpa = Some(40); // Transitions to REC (Staging...) for ~6s
                    }
                }

                // Channel Kangqui: upload or stage (starts in staging REC)
                if let Some(ticks) = staging_kangqui {
                    if ticks > 0 {
                        staging_kangqui = Some(ticks - 1);
                    } else {
                        staging_kangqui = None;
                        app.handle_event(AppEvent::Log(LogEntry::rec(format!(
                            "[강퀴] chunk_{chunk_counter_kangqui:04}.ts sealed. Pushed to cloud upload queue."
                        ))));
                        app.active_uploads.insert(
                            channel_kangqui.clone(),
                            ActiveUpload {
                                channel_id: channel_kangqui.clone(),
                                chunk_name: format!("chunk_{chunk_counter_kangqui:04}.ts"),
                                streamer_name: "강퀴".to_string(),
                                uploaded_bytes: 0,
                                total_bytes: (26.5 * 1024.0 * 1024.0) as u64,
                                speed_mb_s: 6.8,
                            },
                        );
                        chunk_counter_kangqui += 1;
                    }
                } else if let Some(upload) = app.active_uploads.get_mut(&channel_kangqui) {
                    let increment = (0.40 * 1024.0 * 1024.0) as u64;
                    upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                    if upload.uploaded_bytes >= upload.total_bytes {
                        app.uploaded_count += 1;
                        app.reclaimed_mb += 26.5;
                        let finished_chunk = upload.chunk_name.clone();
                        app.handle_event(AppEvent::Log(LogEntry::clean(format!(
                            "[강퀴] Uploaded & deleted {finished_chunk} (reclaimed 26.5 MB)"
                        ))));
                        app.active_uploads.remove(&channel_kangqui);
                        staging_kangqui = Some(30); // Transitions back to REC (Staging...) for ~4.5s
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
                if let Some(ch) = app.channels.iter_mut().find(|c| c.id == channel_kangqui) {
                    ch.chat_count += 1;
                }
                chat_flush_cycle += 1;
                if chat_flush_cycle % 5 == 0 {
                    let chunk_id = chat_flush_cycle / 5;
                    app.handle_event(AppEvent::Log(LogEntry::chat(format!(
                        "[하네 | Hane] Buffered 500 messages (64 KB). Flushed to chat_{chunk_id:04}.jsonl"
                    ))));
                }
                needs_redraw = true;
            }

            // 4. Simulate periodic realistic background events (FFmpeg, Poll, Cloud, Warn, Error restriction)
            _ = sim_interval.tick() => {
                match sim_cycle % 8 {
                    0 => {
                        let frame = 3600 + sim_cycle * 600;
                        let size = 56832 + sim_cycle * 9472;
                        let secs = 120 + sim_cycle * 20;
                        app.handle_event(AppEvent::Log(LogEntry::ffmpeg(format!(
                            "frame= {frame} fps= 60 q=-1.0 size= {size}kB time=00:{:02}:{:02}.00 bitrate=3878.6kbits/s speed=1.000x",
                            secs / 60, secs % 60
                        ))));
                    }
                    1 => {
                        app.handle_event(AppEvent::Log(LogEntry::poll(
                            "Polling 12 monitored channels (cycle #143): 6 online, 3 recording",
                        )));
                    }
                    2 => {
                        app.handle_event(AppEvent::Log(LogEntry::cloud(
                            "Synced stream title update to 'title_history.txt' on remote storage",
                        )));
                    }
                    3 => {
                        app.handle_event(AppEvent::Log(LogEntry::warn(
                            "Temporary upload bandwidth throttle: 3.2 MB/s (resuming target speed)",
                        )));
                    }
                    4 => {
                        app.handle_event(AppEvent::Log(LogEntry::error(
                            "Recording unavailable for channel c3d4e5f67890 (계춘회): subscriber-only stream requires valid Naver credentials (nid_aut, nid_ses)",
                        )));
                    }
                    5 => {
                        app.handle_event(AppEvent::Log(LogEntry::chat(
                            "[너불] Re-established WebSocket handshake with kr-ss3.chat.naver.com (cmd: 100)",
                        )));
                    }
                    6 => {
                        app.handle_event(AppEvent::Log(LogEntry::poll(
                            "Channel status update: '강소연' live session verified (standby)",
                        )));
                    }
                    _ => {
                        app.handle_event(AppEvent::Log(LogEntry::info(
                            "Monitored channels health check passed. Disk free: 142.6 GB",
                        )));
                    }
                }
                sim_cycle += 1;
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
