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

    let channel_hane = "a9a343510e132ea3026ff3cf682820b5".to_string();
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
        "[INFO] Google Drive authenticated successfully".to_string(),
        "[REC] Spawned FFmpeg segmenter (600s TS chunks) -> /home/ghfhffh12345/chzzk/recordings/a9a343510e132ea3026ff3cf682820b5_20260922_172226".to_string(),
        "[DRIVE] Session folder ready: 'Chzzk_Recordings/[2026-09-22_1722] 하네 _ Hane - 쌀먹쥐 ~~~쌀쌀의 생활(봉누도2)'".to_string(),
        "[FFMPEG] [mov,mp4,m4a,3gp,3g2,mj2 @ 0xaaaac40a46d0] Packet corrupt (stream = 0, dts = 1875900).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac40f3a20] Invalid NAL unit size (10632 > 6612).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac40f3a20] missing picture in access unit with size 6645".to_string(),
        "[FFMPEG] [mov,mp4,m4a,3gp,3g2,mj2 @ 0xaaaac4238680] Packet corrupt (stream = 0, dts = 1875900).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac4286670] Invalid NAL unit size (1136 > 459).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac4286670] missing picture in access unit with size 492".to_string(),
        "[REC] chunk_0140.ts sealed. Pushed to Drive upload queue.".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4abe0] Packet corrupt (stream = 2, dts = 1875900).".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4a930] corrupt input packet in stream 2".to_string(),
        "[CLEAN] Uploaded & deleted chunk_0140.ts (reclaimed 27.9 MB)".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4abe0] Packet corrupt (stream = 6, dts = 1875900).".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4a930] corrupt input packet in stream 6".to_string(),
        "[FFMPEG] [mov,mp4,m4a,3gp,3g2,mj2 @ 0xaaaac416f240] Packet corrupt (stream = 0, dts = 1876500).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac41bd550] Invalid NAL unit size (4856 > 1857).".to_string(),
        "[FFMPEG] [NULL @ 0xaaaac41bd550] missing picture in access unit with size 1890".to_string(),
        "[REC] chunk_0141.ts sealed. Pushed to Drive upload queue.".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4abe0] Packet corrupt (stream = 4, dts = 1876500).".to_string(),
        "[FFMPEG] [in#0/hls @ 0xaaaac3f4a930] corrupt input packet in stream 4".to_string(),
        "[CLEAN] Uploaded & deleted chunk_0141.ts (reclaimed 28.1 MB)".to_string(),
        "[FFMPEG] [mov,mp4,m4a,3gp,3g2,mj2 @ 0xaaaac400a180] Found duplicated MOOV Atom. Skipped it".to_string(),
        "[FFMPEG] [mov,mp4,m4a,3gp,3g2,mj2 @ 0xaaaac4301bb0] Found duplicated MOOV Atom. Skipped it".to_string(),
        "[FFMPEG] Last message repeated 1 times".to_string(),
        "[REC] chunk_0142.ts sealed. Pushed to Drive upload queue.".to_string(),
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
