use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::tui::app::App;
use crate::tui::event::LogKind;

pub fn log_kind_badge_and_style(kind: LogKind) -> (&'static str, Style) {
    match kind {
        LogKind::Error => (
            " ERROR  ",
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM),
        ),
        LogKind::Warn => (
            " WARN   ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::DIM),
        ),
        LogKind::Clean => (
            " CLEAN  ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::DIM),
        ),
        LogKind::Rec => (
            " REC    ",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::DIM),
        ),
        LogKind::Ffmpeg => (
            " FFMPEG ",
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::DIM),
        ),
        LogKind::Drive => (
            " DRIVE  ",
            Style::default().fg(Color::Blue).add_modifier(Modifier::DIM),
        ),
        LogKind::Poll => (
            " POLL   ",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ),
        LogKind::Info => (
            " INFO   ",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        ),
    }
}

pub fn draw_ui(f: &mut Frame, app: &App) {
    let area = f.area();
    if area.width == 0 || area.height == 0 {
        return;
    }

    let (header_area, header_div_area, body_area, log_areas, footer_div_area, footer_area) =
        if app.show_logs {
            let chunks = Layout::vertical([
                Constraint::Length(1),  // 0: Top Status / Header
                Constraint::Length(1),  // 1: Horizontal rule divider
                Constraint::Length(10), // 2: Body (1 header row + 9 channel rows)
                Constraint::Length(1),  // 3: Live Activity Logs title divider
                Constraint::Fill(1),    // 4: Logs content
                Constraint::Length(1),  // 5: Footer divider
                Constraint::Length(1),  // 6: Keybind footer
            ])
            .split(area);
            (
                chunks[0],
                chunks[1],
                chunks[2],
                Some((chunks[3], chunks[4])),
                chunks[5],
                chunks[6],
            )
        } else {
            let chunks = Layout::vertical([
                Constraint::Length(1), // 0: Top Status / Header
                Constraint::Length(1), // 1: Horizontal rule divider
                Constraint::Fill(1),   // 2: Body (dynamically expanded)
                Constraint::Length(1), // 3: Footer divider
                Constraint::Length(1), // 4: Keybind footer
            ])
            .split(area);
            (chunks[0], chunks[1], chunks[2], None, chunks[3], chunks[4])
        };

    // 0. Header Status Line
    let (header_text, header_style) = if app.is_shutting_down {
        (
            format!(
                " SHUTTING DOWN │ Stopping recordings & finishing uploads... │ Reclaimed: {:.1} MB │ Uploads: {} ",
                app.reclaimed_mb, app.uploaded_count
            ),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            format!(
                " chzzk-load v{} │ Reclaimed Space: {:.1} MB │ Chunks Uploaded: {}",
                env!("CARGO_PKG_VERSION"),
                app.reclaimed_mb,
                app.uploaded_count
            ),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
    };
    let header = Paragraph::new(header_text).style(header_style);
    f.render_widget(header, header_area);

    // 1. Header Divider
    let header_divider = Paragraph::new("─".repeat(header_div_area.width as usize)).style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    );
    f.render_widget(header_divider, header_div_area);

    // 2. Body: Horizontal split (40% Monitored Channels, 60% Cloud Upload)
    let body_chunks = Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(body_area);

    let left_chunks =
        Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).split(body_chunks[0]);
    let right_chunks =
        Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).split(body_chunks[1]);

    let inner_height = left_chunks[1].height as usize;
    let inner_width_left = left_chunks[1].width as usize;
    let inner_width_right = right_chunks[1].width as usize;

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

    // 2.1. Left Panel: Monitored Channels
    let channels_header = Paragraph::new(" Monitored Channels").style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );
    f.render_widget(channels_header, left_chunks[0]);

    let channel_items: Vec<ListItem> = if visible_channels.is_empty() {
        vec![ListItem::new(" No monitored channels").style(Style::default().fg(Color::DarkGray))]
    } else {
        visible_channels
            .iter()
            .map(|c| {
                let (status, color) = if c.is_active {
                    ("ACTIVE", Color::Cyan)
                } else if c.is_live {
                    ("LIVE", Color::Green)
                } else {
                    ("OFFLINE", Color::DarkGray)
                };

                let badge_str = format!(" {:<7} ", status);
                let badge_len = 9;
                let badge_style = Style::default().fg(color).add_modifier(Modifier::DIM);

                let name_and_title = format!("{} - {}", c.name, c.title);
                let name_style = Style::default();

                let line = if inner_width_left >= badge_len + name_and_title.chars().count() {
                    Line::from(vec![
                        Span::styled(badge_str, badge_style),
                        Span::styled(name_and_title, name_style),
                    ])
                } else if inner_width_left > badge_len {
                    let available = inner_width_left.saturating_sub(badge_len + 1);
                    let mut truncated: String = name_and_title.chars().take(available).collect();
                    truncated.push('…');
                    Line::from(vec![
                        Span::styled(badge_str, badge_style),
                        Span::styled(truncated, name_style),
                    ])
                } else {
                    let mut truncated: String = badge_str
                        .chars()
                        .take(inner_width_left.saturating_sub(1))
                        .collect();
                    truncated.push('…');
                    Line::from(vec![Span::styled(truncated, badge_style)])
                };

                ListItem::new(line)
            })
            .collect()
    };
    let channels_list = List::new(channel_items);
    f.render_widget(channels_list, left_chunks[1]);

    // 2.2. Right Panel: Cloud Upload
    let total_speed: f64 = app.active_uploads.values().map(|u| u.speed_mb_s).sum();
    let active_count = app.active_uploads.len();
    let upload_title = if active_count > 0 {
        format!(
            " Cloud Upload (Total: {:.1} MB/s │ {} Active)",
            total_speed, active_count
        )
    } else {
        " Cloud Upload (Idle)".to_string()
    };
    let upload_header = Paragraph::new(upload_title).style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );
    f.render_widget(upload_header, right_chunks[0]);

    let upload_items: Vec<ListItem> = if visible_channels.is_empty() {
        vec![
            ListItem::new(" Idle (Waiting for monitored channels)")
                .style(Style::default().fg(Color::DarkGray)),
        ]
    } else {
        visible_channels
            .iter()
            .map(|c| {
                let line = if let Some(u) = app.active_uploads.get(&c.id) {
                    let pct = if u.total_bytes > 0 {
                        ((u.uploaded_bytes as f64 / u.total_bytes as f64) * 100.0)
                            .round()
                            .min(100.0) as u16
                    } else {
                        0
                    };
                    let up_mb = u.uploaded_bytes as f64 / 1_048_576.0;
                    let tot_mb = u.total_bytes as f64 / 1_048_576.0;

                    let badge = " UP   ";
                    let badge_len = 6;
                    let badge_style = Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::DIM);

                    let metrics_str = if inner_width_right >= 48 {
                        format!(
                            " {}% {:.1}/{:.1}MB {:.1}MB/s",
                            pct, up_mb, tot_mb, u.speed_mb_s
                        )
                    } else if inner_width_right >= 30 {
                        format!(" {}% {:.1}MB/s", pct, u.speed_mb_s)
                    } else {
                        format!(" {}%", pct)
                    };
                    let metrics_len = metrics_str.chars().count();

                    let bar_width = inner_width_right.saturating_sub(badge_len + metrics_len);
                    let (filled_count, unfilled_count) = if bar_width > 0 {
                        let filled = ((pct as usize * bar_width) / 100).min(bar_width);
                        let unfilled = bar_width.saturating_sub(filled);
                        (filled, unfilled)
                    } else {
                        (0, 0)
                    };

                    if inner_width_right >= badge_len + metrics_len {
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled("━".repeat(filled_count), Style::default()),
                            Span::styled(
                                "─".repeat(unfilled_count),
                                Style::default()
                                    .fg(Color::DarkGray)
                                    .add_modifier(Modifier::DIM),
                            ),
                            Span::styled(metrics_str, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else if inner_width_right > badge_len {
                        let available = inner_width_right.saturating_sub(badge_len + 1);
                        let mut truncated: String = metrics_str.chars().take(available).collect();
                        truncated.push('…');
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled(truncated, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else {
                        let mut truncated: String = badge
                            .chars()
                            .take(inner_width_right.saturating_sub(1))
                            .collect();
                        truncated.push('…');
                        Line::from(vec![Span::styled(truncated, badge_style)])
                    }
                } else if c.is_active {
                    let badge = " REC  ";
                    let badge_style = Style::default().fg(Color::Red).add_modifier(Modifier::DIM);
                    let rest = "Staging...";
                    if inner_width_right >= 6 + rest.chars().count() {
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled(rest, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else if inner_width_right > 6 {
                        let available = inner_width_right.saturating_sub(6 + 1);
                        let mut truncated: String = rest.chars().take(available).collect();
                        truncated.push('…');
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled(truncated, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else {
                        let mut truncated: String = badge
                            .chars()
                            .take(inner_width_right.saturating_sub(1))
                            .collect();
                        truncated.push('…');
                        Line::from(vec![Span::styled(truncated, badge_style)])
                    }
                } else if c.is_live {
                    let badge = " IDLE ";
                    let badge_style = Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::DIM);
                    let rest = "Standby";
                    if inner_width_right >= 6 + rest.chars().count() {
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled(rest, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else if inner_width_right > 6 {
                        let available = inner_width_right.saturating_sub(6 + 1);
                        let mut truncated: String = rest.chars().take(available).collect();
                        truncated.push('…');
                        Line::from(vec![
                            Span::styled(badge, badge_style),
                            Span::styled(truncated, Style::default().add_modifier(Modifier::DIM)),
                        ])
                    } else {
                        let mut truncated: String = badge
                            .chars()
                            .take(inner_width_right.saturating_sub(1))
                            .collect();
                        truncated.push('…');
                        Line::from(vec![Span::styled(truncated, badge_style)])
                    }
                } else {
                    Line::from(vec![Span::styled(
                        " —",
                        Style::default().fg(Color::DarkGray),
                    )])
                };

                ListItem::new(line)
            })
            .collect()
    };
    let upload_list = List::new(upload_items);
    f.render_widget(upload_list, right_chunks[1]);

    // 3 & 4. Activity Logs (if visible)
    if let Some((log_divider_area, log_area)) = log_areas {
        let effective_scroll = if app.log_scroll > 0 {
            let max_scroll = app.logs.len().saturating_sub(log_area.height as usize);
            app.log_scroll.min(max_scroll)
        } else {
            0
        };
        let log_title = if effective_scroll > 0 {
            format!("── Live Activity Logs (-{}) ", effective_scroll)
        } else {
            "── Live Activity Logs ".to_string()
        };
        let title_len = log_title.chars().count();
        let rule_len = (log_divider_area.width as usize).saturating_sub(title_len);
        let log_divider_text = format!("{}{}", log_title, "─".repeat(rule_len));
        let log_divider = Paragraph::new(log_divider_text).style(
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::DIM),
        );
        f.render_widget(log_divider, log_divider_area);

        if log_area.height > 0 && log_area.width > 0 {
            let inner_height = log_area.height as usize;
            let inner_width = log_area.width as usize;

            // Flatten log entries directly into (LogKind, &str) borrowed pairs
            let mut flattened_lines: Vec<(LogKind, &str)> = Vec::new();
            for entry in &app.logs {
                for line in entry.message.lines() {
                    flattened_lines.push((entry.kind, line));
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
                .map(|(kind, message_str)| {
                    let (badge_str, badge_style) = log_kind_badge_and_style(*kind);
                    let badge_len = 8;
                    if inner_width >= badge_len + message_str.chars().count() {
                        Line::from(vec![
                            Span::styled(badge_str, badge_style),
                            Span::styled(*message_str, Style::default()),
                        ])
                    } else if inner_width > badge_len {
                        let available = inner_width.saturating_sub(badge_len + 1);
                        let mut truncated: String = message_str.chars().take(available).collect();
                        truncated.push('…');
                        Line::from(vec![
                            Span::styled(badge_str, badge_style),
                            Span::styled(truncated, Style::default()),
                        ])
                    } else {
                        let mut truncated: String = badge_str
                            .chars()
                            .take(inner_width.saturating_sub(1))
                            .collect();
                        truncated.push('…');
                        Line::from(vec![Span::styled(truncated, badge_style)])
                    }
                })
                .collect();

            let logs_widget = Paragraph::new(visible_lines);
            f.render_widget(logs_widget, log_area);
        }
    }

    // 5. Footer Divider
    let footer_divider = Paragraph::new("─".repeat(footer_div_area.width as usize)).style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    );
    f.render_widget(footer_divider, footer_div_area);

    // 6. Keybind Footer
    let (footer_text, footer_style) = if app.is_shutting_down {
        (
            " q / Ctrl+C Force Exit Immediately │ Cleaning up: stopping FFmpeg & flushing uploads... ",
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        )
    } else if app.show_logs {
        (
            " q Quit   l Logs   ↑/↓ Scroll   PgUp/PgDn Scroll Logs   Home/End Top/Latest   r Refresh ",
            Style::default().fg(Color::DarkGray),
        )
    } else {
        (
            " q Quit   l Logs   ↑/↓ Scroll   r Refresh ",
            Style::default().fg(Color::DarkGray),
        )
    };
    let footer = Paragraph::new(footer_text).style(footer_style);
    f.render_widget(footer, footer_area);
}
