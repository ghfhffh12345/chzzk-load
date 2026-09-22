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
            Constraint::Length(3), // Header
            Constraint::Length(9), // Body (Channels + Active Progress)
            Constraint::Fill(1),   // Logs (flexible fill of remaining vertical space)
            Constraint::Length(1), // Keybind footer
        ])
        .split(area);

    // Header
    let (header_text, header_style, border_style) = if app.is_shutting_down {
        (
            format!(
                " [ SHUTTING DOWN ] Stopping recordings & finishing uploads... │ Reclaimed: {:.1} MB │ Uploads: {} ",
                app.reclaimed_mb, app.uploaded_count
            ),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
            Style::default().fg(Color::Yellow),
        )
    } else {
        (
            format!(
                " chzzk-load v0.1.0 │ Reclaimed Space: {:.1} MB │ Chunks Uploaded: {}",
                app.reclaimed_mb, app.uploaded_count
            ),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            Style::default(),
        )
    };

    let header = Paragraph::new(header_text).style(header_style).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(border_style),
    );
    f.render_widget(header, chunks[0]);

    // Body: Split horizontally
    // Body: Split horizontally (40% Monitored Channels, 60% Cloud Upload)
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(chunks[1]);

    let inner_height = (body_chunks[0].height.saturating_sub(2)) as usize;
    let inner_width_left = (body_chunks[0].width.saturating_sub(2)) as usize;
    let inner_width_right = (body_chunks[1].width.saturating_sub(2)) as usize;

    // Synchronized scroll offset & slice of visible channels
    let total_channels = app.channels.len();
    let max_scroll = total_channels.saturating_sub(inner_height);
    let scroll = app.channel_scroll.min(max_scroll);
    let visible_channels = if total_channels > 0 {
        let end = (scroll + inner_height).min(total_channels);
        &app.channels[scroll..end]
    } else {
        &[]
    };

    // 1. Left Panel: Monitored Channels
    let channel_items: Vec<ListItem> = if visible_channels.is_empty() {
        vec![ListItem::new("No monitored channels").style(Style::default().fg(Color::DarkGray))]
    } else {
        visible_channels
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let channel_idx = scroll + i;
                let is_selected = channel_idx == app.selected_channel_idx;

                let (status, color) = if c.is_active {
                    ("[ ACTIVE ]", Color::Cyan)
                } else if c.is_live {
                    ("[ LIVE ]", Color::Green)
                } else {
                    ("[ OFFLINE ]", Color::DarkGray)
                };

                let text = format!("{} {} - {}", status, c.name, c.title);
                let line_str = if text.chars().count() > inner_width_left && inner_width_left > 1 {
                    let mut s: String = text
                        .chars()
                        .take(inner_width_left.saturating_sub(1))
                        .collect();
                    s.push('…');
                    s
                } else {
                    text
                };

                let mut style = Style::default().fg(color);
                if is_selected {
                    style = style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
                }
                ListItem::new(line_str).style(style)
            })
            .collect()
    };

    let channels_list = List::new(channel_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Monitored Channels ")
            .border_type(BorderType::Rounded),
    );
    f.render_widget(channels_list, body_chunks[0]);

    // 2. Right Panel: Cloud Upload (1:1 aligned with Monitored Channels)
    let total_speed: f64 = app.active_uploads.values().map(|u| u.speed_mb_s).sum();
    let active_count = app.active_uploads.len();
    let upload_title = if active_count > 0 {
        format!(
            " Cloud Upload (Total: {:.1} MB/s │ {} Active) ",
            total_speed, active_count
        )
    } else {
        " Cloud Upload (Idle) ".to_string()
    };

    let upload_items: Vec<ListItem> = if visible_channels.is_empty() {
        vec![
            ListItem::new("Idle (Waiting for monitored channels)")
                .style(Style::default().fg(Color::DarkGray)),
        ]
    } else {
        visible_channels
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let channel_idx = scroll + i;
                let is_selected = channel_idx == app.selected_channel_idx;

                let (raw_text, color) = if let Some(u) = app.active_uploads.get(&c.id) {
                    let pct = if u.total_bytes > 0 {
                        ((u.uploaded_bytes as f64 / u.total_bytes as f64) * 100.0)
                            .round()
                            .min(100.0) as u16
                    } else {
                        0
                    };
                    let up_mb = u.uploaded_bytes as f64 / 1_048_576.0;
                    let tot_mb = u.total_bytes as f64 / 1_048_576.0;

                    let bar_width = 12;
                    let filled = ((pct as usize * bar_width) / 100).min(bar_width);
                    let bar_str =
                        format!("[{}{}]", "█".repeat(filled), "░".repeat(bar_width - filled));

                    let text = if inner_width_right >= 55 {
                        format!(
                            "{}: {} {}% ({:.1}/{:.1} MB) @ {:.1} MB/s",
                            u.chunk_name, bar_str, pct, up_mb, tot_mb, u.speed_mb_s
                        )
                    } else if inner_width_right >= 38 {
                        format!(
                            "{}: {} {}% @ {:.1} MB/s",
                            u.chunk_name, bar_str, pct, u.speed_mb_s
                        )
                    } else {
                        format!("{}: {}% @ {:.1} MB/s", u.chunk_name, pct, u.speed_mb_s)
                    };
                    (text, Color::LightGreen)
                } else if c.is_active {
                    (
                        "[Recording] Waiting for sealed chunk...".to_string(),
                        Color::Cyan,
                    )
                } else if c.is_live {
                    ("[Idle] Waiting for stream...".to_string(), Color::Green)
                } else {
                    ("-".to_string(), Color::DarkGray)
                };

                let line_str =
                    if raw_text.chars().count() > inner_width_right && inner_width_right > 1 {
                        let mut s: String = raw_text
                            .chars()
                            .take(inner_width_right.saturating_sub(1))
                            .collect();
                        s.push('…');
                        s
                    } else {
                        raw_text
                    };

                let mut style = Style::default().fg(color);
                if is_selected {
                    style = style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
                }
                ListItem::new(line_str).style(style)
            })
            .collect()
    };

    let upload_list = List::new(upload_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(upload_title)
            .border_type(BorderType::Rounded),
    );
    f.render_widget(upload_list, body_chunks[1]);

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
                    let mut s: String = raw_line
                        .chars()
                        .take(inner_width.saturating_sub(1))
                        .collect();
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
        let fallback =
            Paragraph::new(" Logs: (expand window) ").style(Style::default().fg(Color::DarkGray));
        f.render_widget(fallback, log_area);
    }

    // Footer
    let (footer_text, footer_style) = if app.is_shutting_down {
        (
            " [q / Ctrl+C] Force Exit Immediately │ Cleaning up: stopping FFmpeg & flushing uploads... ",
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            " [q] Quit   [↑/↓] Channel   [PgUp/PgDn] Scroll Logs   [Home/End] Top/Latest   [r] Refresh ",
            Style::default().fg(Color::Yellow),
        )
    };
    let footer = Paragraph::new(footer_text).style(footer_style);
    f.render_widget(footer, chunks[3]);
}
