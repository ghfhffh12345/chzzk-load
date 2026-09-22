use std::collections::HashMap;
use std::io::stdout;
use std::time::{Duration, Instant};

use chzzk_load::tui::app::{ActiveUpload, App, ChannelItem};
use chzzk_load::tui::event::AppEvent;
use chzzk_load::tui::ui::draw_ui;
use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        },
        ChannelItem {
            id: channel_lilpa.clone(),
            name: "릴파 | LILPA".to_string(),
            is_live: true,
            is_active: true,
            title: "노래 연습 및 소통 뱅온!".to_string(),
        },
        ChannelItem {
            id: channel_kangqui.clone(),
            name: "강퀴".to_string(),
            is_live: true,
            is_active: false,
            title: "엄마한턴만더하고끌게 / 1루트 클래식 하드 3부".to_string(),
        },
        ChannelItem {
            id: channel_wakgood.clone(),
            name: "우왁굳".to_string(),
            is_live: true,
            is_active: false,
            title: "왁물원 구경하기 및 종합 게임".to_string(),
        },
        ChannelItem {
            id: channel_gosegu.clone(),
            name: "고세구".to_string(),
            is_live: true,
            is_active: false,
            title: "세구세구 온 세상이 고세구".to_string(),
        },
        ChannelItem {
            id: channel_looksam.clone(),
            name: "룩삼".to_string(),
            is_live: false,
            is_active: false,
            title: "룩삼 닌텐도 지휘자 게임".to_string(),
        },
        ChannelItem {
            id: channel_poong.clone(),
            name: "풍월량".to_string(),
            is_live: false,
            is_active: false,
            title: "wow 포에버".to_string(),
        },
        ChannelItem {
            id: channel_calmdown.clone(),
            name: "침착맨".to_string(),
            is_live: false,
            is_active: false,
            title: "침착맨의 일상 토크".to_string(),
        },
        ChannelItem {
            id: channel_nokduro.clone(),
            name: "녹두로".to_string(),
            is_live: false,
            is_active: false,
            title: "슈퍼 마리오 메이커 2 익스트림".to_string(),
        },
        ChannelItem {
            id: channel_ddahyoni.clone(),
            name: "따효니".to_string(),
            is_live: false,
            is_active: false,
            title: "하스스톤 신규 확장팩 덱 메이킹".to_string(),
        },
        ChannelItem {
            id: channel_okja.clone(),
            name: "옥자미".to_string(),
            is_live: false,
            is_active: false,
            title: "리그 오브 레전드 랭크 게임".to_string(),
        },
        ChannelItem {
            id: channel_cheol.clone(),
            name: "철면수심".to_string(),
            is_live: false,
            is_active: false,
            title: "전장 1등 도전기".to_string(),
        },
    ];

    app.reclaimed_mb = 4120.8;
    app.uploaded_count = 141;

    let mut uploads = HashMap::new();
    uploads.insert(
        channel_hane.clone(),
        ActiveUpload {
            channel_id: channel_hane,
            chunk_name: "chunk_0142.ts".to_string(),
            streamer_name: "하네 | Hane".to_string(),
            uploaded_bytes: (18.4 * 1024.0 * 1024.0) as u64,
            total_bytes: (28.4 * 1024.0 * 1024.0) as u64,
            speed_mb_s: 8.5,
        },
    );
    app.active_uploads = uploads;

    app.logs = vec![
        "[INFO] Google Drive authenticated successfully".to_string(),
        "[REC] Spawned FFmpeg segmenter (600s TS chunks) -> recordings/a9a34351_20260922"
            .to_string(),
        "[DRIVE] Session folder ready: 'Chzzk_Recordings/2026-09-22 하네 - 쌀먹쥐'".to_string(),
        "[FFMPEG] Packet corrupt (stream = 0, dts = 1875900).".to_string(),
        "[FFMPEG] Invalid NAL unit size (10632 > 6612).".to_string(),
        "[REC] chunk_0140.ts sealed. Pushed to Drive upload queue.".to_string(),
        "[CLEAN] Uploaded & deleted chunk_0140.ts (reclaimed 27.9 MB)".to_string(),
        "[REC] chunk_0141.ts sealed. Pushed to Drive upload queue.".to_string(),
        "[CLEAN] Uploaded & deleted chunk_0141.ts (reclaimed 28.1 MB)".to_string(),
        "[REC] Spawned FFmpeg segmenter (600s TS chunks) -> recordings/b1a23456_20260922"
            .to_string(),
        "[REC] chunk_0142.ts sealed. Pushed to Drive upload queue.".to_string(),
        "[INFO] All monitored channels status synced successfully".to_string(),
    ];

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        Hide,
        crossterm::terminal::SetTitle("chzzk-load preview")
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        // Animate upload progress smoothly
        if last_tick.elapsed() >= Duration::from_millis(150) {
            last_tick = Instant::now();
            if let Some(upload) = app
                .active_uploads
                .get_mut("a9a343510e132ea3026ff3cf682820b5")
            {
                let increment = (0.45 * 1024.0 * 1024.0) as u64;
                upload.uploaded_bytes = upload.uploaded_bytes.saturating_add(increment);
                if upload.uploaded_bytes >= upload.total_bytes {
                    upload.uploaded_bytes = (1.5 * 1024.0 * 1024.0) as u64;
                    app.uploaded_count += 1;
                    app.reclaimed_mb += 28.4;
                }
            }
        }

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            if key.kind == crossterm::event::KeyEventKind::Release {
                continue;
            }
            if key.code == KeyCode::Esc
                || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                break;
            }
            app.handle_event(AppEvent::Key(key));
            if app.should_quit {
                break;
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, Show)?;
    terminal.show_cursor()?;

    Ok(())
}
