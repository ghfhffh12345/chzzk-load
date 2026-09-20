use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui::app::App;

pub fn draw_ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Body (Channels + Active Progress)
            Constraint::Length(8), // Logs
            Constraint::Length(1), // Keybind footer
        ])
        .split(f.area());

    // Header
    let header = Paragraph::new(format!(
        " chzzk-load v0.1.0 │ Reclaimed Space: {:.1} MB │ Chunks Uploaded: {}",
        app.reclaimed_mb, app.uploaded_count
    ))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded),
    );
    f.render_widget(header, chunks[0]);

    // Body: Split horizontally
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(chunks[1]);

    // Channels List
    let items: Vec<ListItem> = app
        .channels
        .iter()
        .enumerate()
        .map(|(idx, c)| {
            let status = if c.is_live {
                "[ LIVE ]"
            } else {
                "[ OFFLINE ]"
            };
            let color = if c.is_live {
                Color::Green
            } else {
                Color::DarkGray
            };
            let mut style = Style::default().fg(color);
            if idx == app.selected_channel_idx && !app.channels.is_empty() {
                style = style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
            }
            ListItem::new(format!("{} {} - {}", status, c.name, c.title)).style(style)
        })
        .collect();

    let channels_list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Monitored Channels ")
            .border_type(BorderType::Rounded),
    );
    f.render_widget(channels_list, body_chunks[0]);

    // Pipeline Panel
    let active_panel = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Min(2),
        ])
        .split(body_chunks[1]);

    let upload_title = app
        .active_upload_name
        .as_deref()
        .unwrap_or("Idle (Waiting for completed chunk)");
    let upload_gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Cloud Upload: {} ", upload_title)),
        )
        .gauge_style(Style::default().fg(Color::LightGreen))
        .percent(app.upload_progress_pct)
        .label(format!(
            "{}% @ {:.1} MB/s",
            app.upload_progress_pct, app.upload_speed
        ));
    f.render_widget(upload_gauge, active_panel[0]);

    let stream_title = app
        .active_stream
        .as_deref()
        .unwrap_or("Idle (Waiting for stream)");
    let stream_info = Paragraph::new(format!(" Active Session: {}", stream_title))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Stream Recorder ")
                .border_type(BorderType::Rounded),
        )
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(stream_info, active_panel[1]);

    // Logs Panel
    let log_items: Vec<ListItem> = app
        .logs
        .iter()
        .rev()
        .take(15)
        .rev()
        .map(|l| ListItem::new(l.as_str()))
        .collect();
    let logs_widget = List::new(log_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Live Activity Logs ")
            .border_type(BorderType::Rounded),
    );
    f.render_widget(logs_widget, chunks[2]);

    // Footer
    let footer = Paragraph::new(" [q] Quit   [↑/↓] Select Channel   [r] Refresh ")
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(footer, chunks[3]);
}
