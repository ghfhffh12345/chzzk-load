use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui::app::App;

pub fn draw_ui(f: &mut Frame, app: &App) {
    let area = f.area();
    if area.width == 0 || area.height == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Length(9),  // Body (Channels + Active Progress)
            Constraint::Fill(1),    // Logs (flexible fill of remaining vertical space)
            Constraint::Length(1),  // Keybind footer
        ])
        .split(area);

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

    // Pipeline Panel: 4 rows for upload, 5 rows for recorder (sum = 9 rows of chunks[1])
    let active_panel = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(5),
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

    // Logs Panel: strictly bounded, horizontally clipped, and auto-scrolled to tail
    let log_area = chunks[2];
    if log_area.height >= 3 && log_area.width >= 2 {
        let inner_height = (log_area.height - 2) as usize;
        let inner_width = (log_area.width - 2) as usize;

        // Flatten all log entries into individual lines, splitting on newlines
        let mut flattened_lines: Vec<&str> = Vec::new();
        for log in &app.logs {
            for line in log.lines() {
                flattened_lines.push(line);
            }
        }

        let total_lines = flattened_lines.len();
        let max_scroll = total_lines.saturating_sub(inner_height);
        let effective_scroll = app.log_scroll.min(max_scroll);

        let end_idx = total_lines.saturating_sub(effective_scroll);
        let start_idx = end_idx.saturating_sub(inner_height);

        let slice = &flattened_lines[start_idx..end_idx];

        let visible_lines: Vec<Line> = slice
            .iter()
            .map(|raw_line| {
                // Truncate line horizontally if it exceeds inner_width to prevent wrap overflow
                let line_str = if raw_line.chars().count() > inner_width {
                    let mut s: String = raw_line.chars().take(inner_width.saturating_sub(1)).collect();
                    s.push('…');
                    s
                } else {
                    raw_line.to_string()
                };

                let style = if line_str.starts_with("[ERROR]") {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else if line_str.starts_with("[WARN]") {
                    Style::default().fg(Color::Yellow)
                } else if line_str.starts_with("[CLEAN]") {
                    Style::default().fg(Color::LightGreen)
                } else if line_str.starts_with("[REC]") {
                    Style::default().fg(Color::Cyan)
                } else if line_str.starts_with("[FFMPEG]") {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default().fg(Color::Gray)
                };
                Line::styled(line_str, style)
            })
            .collect();

        let title = if effective_scroll > 0 {
            format!(" Live Activity Logs [Scrolled: -{}] ", effective_scroll)
        } else {
            " Live Activity Logs ".to_string()
        };

        let logs_widget = Paragraph::new(visible_lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_type(BorderType::Rounded),
        );
        f.render_widget(logs_widget, log_area);
    } else if log_area.height > 0 {
        let fallback = Paragraph::new(" Logs: (expand window) ")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(fallback, log_area);
    }

    // Footer
    let footer = Paragraph::new(
        " [q] Quit   [↑/↓] Channel   [PgUp/PgDn] Scroll Logs   [Home/End] Top/Latest   [r] Refresh ",
    )
    .style(Style::default().fg(Color::Yellow));
    f.render_widget(footer, chunks[3]);
}
