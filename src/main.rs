use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossterm::cursor::Show;
use crossterm::event::{Event, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::prelude::*;
use tokio_util::sync::CancellationToken;

use chzzk_load::app_path::resolve_path;
use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::Settings;
use chzzk_load::drive::auth::DriveAuth;
use chzzk_load::drive::client::DriveClient;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::app::App;
use chzzk_load::tui::ui::draw_ui;
use chzzk_load::tui::{AppEvent, LogEntry};

#[derive(Parser, Debug)]
#[command(
    name = "chzzk-load",
    version,
    about = "Real-time Chzzk stream recording and Google Drive syncing"
)]
pub struct Cli {
    #[arg(short, long, help = "Path to dedicated settings.json file")]
    pub config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _console_guard = chzzk_load::tui::ConsoleCodePageGuard::init();

    // Register panic hook to restore terminal on panic
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
        chzzk_load::tui::ConsoleCodePageGuard::restore_original();
        default_panic(panic_info);
    }));

    let args = Cli::parse();
    let config_path = args
        .config
        .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.json")));

    let settings = Settings::load_or_create_default(&config_path)?;
    let creds_path = resolve_path(&PathBuf::from(&settings.google_drive.credentials_path));
    let token_path = resolve_path(&PathBuf::from(&settings.google_drive.token_path));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(1000);

    // Initialize Google Drive Auth if credentials exist
    let drive_client = if creds_path.exists() {
        match DriveAuth::load_or_authorize(&creds_path, &token_path).await {
            Ok(auth) => {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::info(
                        "Google Drive authenticated successfully",
                    )))
                    .await;
                Some(DriveClient::new(Arc::new(auth)))
            }
            Err(e) => {
                let _ = event_tx
                    .send(AppEvent::Log(LogEntry::warn(format!(
                        "Drive auth failed: {}",
                        e
                    ))))
                    .await;
                None
            }
        }
    } else {
        let _ = event_tx
            .send(AppEvent::Log(LogEntry::info(format!(
                "'{}' not found; running in local-only recording mode",
                creds_path.display()
            ))))
            .await;
        None
    };

    let cancel_token = CancellationToken::new();
    let chzzk = ChzzkClient::new(&settings.chzzk);
    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings.clone(),
        chzzk,
        drive_client,
        event_tx.clone(),
        cancel_token.clone(),
    ));

    // Handle Ctrl+C: first triggers graceful shutdown, subsequent presses force immediate exit
    let cancel_token_ctrlc = cancel_token.clone();
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_ok() {
                if cancel_token_ctrlc.is_cancelled() {
                    let _ = disable_raw_mode();
                    let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
                    chzzk_load::tui::ConsoleCodePageGuard::restore_original();
                    std::process::exit(130);
                } else {
                    cancel_token_ctrlc.cancel();
                }
            }
        }
    });

    let mut orch_handle = tokio::spawn(orchestrator.clone().run());

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::from_settings(&settings);

    // Main TUI render loop
    let mut event_reader = crossterm::event::EventStream::new();
    let mut tick_interval = tokio::time::interval(Duration::from_millis(250));
    let mut needs_redraw = true;

    loop {
        // Check if external shutdown signal (e.g. Ctrl+C) was received
        if cancel_token.is_cancelled() && !app.is_shutting_down {
            app.is_shutting_down = true;
            needs_redraw = true;
        }

        // Check exit conditions
        if app.should_quit {
            cancel_token.cancel();
            break;
        }
        if app.is_shutting_down && orch_handle.is_finished() {
            break;
        }

        if needs_redraw {
            terminal.draw(|f| draw_ui(f, &app))?;
            needs_redraw = false;
        }

        tokio::select! {
            biased;

            // Immediate exit when orchestrator terminates during shutdown
            _ = &mut orch_handle, if app.is_shutting_down => {
                break;
            }

            // Drain engine and upload events with sub-millisecond latency
            Some(ev) = event_rx.recv() => {
                app.handle_event(ev);
                while let Ok(ev) = event_rx.try_recv() {
                    app.handle_event(ev);
                }
                if app.refresh_requested {
                    app.refresh_requested = false;
                    let _ = event_tx
                        .send(AppEvent::Log(LogEntry::info("Manual refresh triggered...")))
                        .await;
                    orchestrator.trigger_refresh();
                }
                needs_redraw = true;
            }

            // Asynchronously process crossterm events (non-blocking)
            Some(item) = event_reader.next() => {
                if let Ok(Event::Key(key)) = item
                    && key.kind != crossterm::event::KeyEventKind::Release
                {
                    if key.code == KeyCode::Char('q') && key.kind == crossterm::event::KeyEventKind::Press {
                        if app.is_shutting_down {
                            // Second 'q' press triggers immediate exit
                            app.should_quit = true;
                            cancel_token.cancel();
                            break;
                        } else {
                            app.is_shutting_down = true;
                            cancel_token.cancel();
                        }
                    } else {
                        app.handle_event(AppEvent::Key(key));
                    }
                    if app.refresh_requested {
                        app.refresh_requested = false;
                        let _ = event_tx
                            .send(AppEvent::Log(LogEntry::info("Manual refresh triggered...")))
                            .await;
                        orchestrator.trigger_refresh();
                    }
                    needs_redraw = true;
                }
            }

            // Periodic tick for elapsed time & progress animations
            _ = tick_interval.tick() => {
                if !app.active_recording_starts.is_empty() || !app.active_uploads.is_empty() || app.is_shutting_down {
                    needs_redraw = true;
                }
            }
        }
    }

    // Restore terminal
    let _ = disable_raw_mode();
    let _ = crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen, Show);

    // Drain event_rx in background so event_tx never blocks during shutdown
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    // If orch_handle is still running (e.g. forced exit or safety timeout), await with short grace period
    if !orch_handle.is_finished() {
        let _ = tokio::time::timeout(Duration::from_secs(2), orch_handle).await;
    }

    // Clean up any empty stream session folders inside local recordings directory on shutdown
    let recordings_base = resolve_path(std::path::Path::new(&settings.general.recordings_dir));
    let _ = EngineOrchestrator::cleanup_empty_session_dirs(&recordings_base).await;

    Ok(())
}
