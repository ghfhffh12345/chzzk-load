use std::collections::HashMap;
use std::io::stdout;
use std::time::Duration;

use chzzk_load::tui::app::{ActiveUpload, App, ChannelItem};
use chzzk_load::tui::ui::draw_ui;
use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = App::new();

    let channel_hane = "ch_hane_001".to_string();
    let channel_kangqui = "1a1dd9ce56fb61a37ffb6f69f6d5b978".to_string();
    let channel_looksam = "8803cee946a9e610a76fbdee98d98c61".to_string();
    let channel_poong = "7ce8032370ac5121dcabce7bad375ced".to_string();

    app.channels = vec![
        ChannelItem {
            id: channel_hane.clone(),
            name: "하네 | Hane".to_string(),
            is_live: true,
            is_active: true,
            title: "쌀먹쥐 ~~~쌀쌀의 생활(봉누도2)".to_string(),
        },
        ChannelItem {
            id: channel_kangqui.clone(),
            name: "강퀴".to_string(),
            is_live: true,
            is_active: false,
            title: "엄마한턴만더하고끌게 / 1루트 클래식 하드 3부".to_string(),
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
    ];

    app.selected_channel_idx = 0;
    app.reclaimed_mb = 4120.8;
    app.uploaded_count = 141;

    let mut uploads = HashMap::new();
    uploads.insert(
        channel_hane.clone(),
        ActiveUpload {
            channel_id: channel_hane,
            chunk_name: "chunk_0142.ts".to_string(),
            streamer_name: "하네 | Hane".to_string(),
            uploaded_bytes: (19.3 * 1024.0 * 1024.0) as u64,
            total_bytes: (28.4 * 1024.0 * 1024.0) as u64,
            speed_mb_s: 8.5,
        },
    );
    app.active_uploads = uploads;

    app.logs = vec![
        "[REC] 치지직 라이브 감지: '하네 | Hane' (status: OPEN, 1080p60)".to_string(),
        "[REC] Initialized lossless FFmpeg stream-copy session (-c copy)".to_string(),
        "[FFMPEG] Segment chunk_0141.ts completed (duration: 600s)".to_string(),
        "[CLEAN] Upload confirmed for 'chunk_0140.ts' -> Deleted local file (+27.9 MB reclaimed)"
            .to_string(),
        "[REC] Sealed chunk 'chunk_0142.ts' (size: 28.4 MB)".to_string(),
        "[REC] 치지직 라이브 감지: '강퀴' (status: OPEN, 1080p60)".to_string(),
        "[CLEAN] Upload confirmed for 'chunk_0038.ts' -> Deleted local file (+26.5 MB reclaimed)"
            .to_string(),
        "[REC] Chzzk CDN polling active (channels: 4, interval: 20s)".to_string(),
        "[FFMPEG] Generating stream-copied MPEG-TS segment 'chunk_0143.ts'".to_string(),
        "[CLEAN] Resumable Google Drive upload session active for 'chunk_0142.ts'".to_string(),
        "[CLEAN] Upload confirmed for 'chunk_0141.ts' -> Deleted local file (+28.1 MB reclaimed)"
            .to_string(),
        "[CLEAN] Total space reclaimed: 4,120.8 MB (strictly bounded disk footprint)".to_string(),
    ];

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        Hide,
        crossterm::terminal::SetTitle("chzzk-load")
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Up | KeyCode::Char('k') => app.prev_channel(),
                KeyCode::Down | KeyCode::Char('j') => app.next_channel(),
                KeyCode::PageUp => {
                    app.log_scroll = app.log_scroll.saturating_add(5);
                }
                KeyCode::PageDown => {
                    app.log_scroll = app.log_scroll.saturating_sub(5);
                }
                KeyCode::Home => {
                    app.log_scroll = usize::MAX;
                }
                KeyCode::End => {
                    app.log_scroll = 0;
                }
                _ => {}
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, Show)?;
    terminal.show_cursor()?;

    Ok(())
}
