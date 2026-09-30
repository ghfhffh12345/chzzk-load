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
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::app::App;
use chzzk_load::tui::ui::draw_ui;
use chzzk_load::tui::{AppEvent, LogEntry};
use chzzk_load::uploader::{RcloneBackend, UploadBackend};

#[derive(Parser, Debug)]
#[command(
    name = "chzzk-load",
    version,
    about = "Real-time Chzzk stream recording and cloud storage syncing"
)]
pub struct Cli {
    #[arg(short, long, help = "Path to dedicated settings.toml file")]
    pub config: Option<PathBuf>,
    #[arg(
        long,
        help = "Skip rclone remote connection verification check at startup"
    )]
    pub skip_rclone_check: bool,
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
        .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.toml")));

    let settings = Settings::load_or_create_default(&config_path)?;

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(1000);
    let cancel_token = CancellationToken::new();

    // Initialize cloud upload backend if remote_path is configured
    let upload_backend: Option<Arc<dyn UploadBackend>> = if !settings.rclone.remote_path.is_empty()
    {
        let backend = Arc::new(RcloneBackend::new(settings.rclone.clone()));
        let should_skip = args.skip_rclone_check || settings.rclone.skip_connection_check;
        if should_skip {
            let _ = event_tx
                .send(AppEvent::Log(LogEntry::cloud(
                    "Rclone remote connection check skipped",
                )))
                .await;
        } else {
            let _ = event_tx
                .send(AppEvent::Log(LogEntry::cloud(format!(
                    "Verifying rclone remote '{}' in background...",
                    settings.rclone.remote_path
                ))))
                .await;

            let check_backend = backend.clone();
            let check_event_tx = event_tx.clone();
            let check_cancel_token = cancel_token.clone();
            let check_remote_path = settings.rclone.remote_path.clone();

            tokio::spawn(async move {
                tokio::select! {
                    _ = check_cancel_token.cancelled() => {
                        // Shutdown requested before or during check
                    }
                    result = tokio::time::timeout(Duration::from_secs(10), check_backend.check_connection()) => {
                        match result {
                            Ok(Ok(())) => {
                                let _ = check_event_tx
                                    .send(AppEvent::Log(LogEntry::cloud(format!(
                                        "Rclone remote '{check_remote_path}' verified successfully"
                                    ))))
                                    .await;
                            }
                            Ok(Err(e)) => {
                                let _ = check_event_tx
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Rclone remote connection check failed: {e}"
                                    ))))
                                    .await;
                            }
                            Err(_) => {
                                let _ = check_event_tx
                                    .send(AppEvent::Log(LogEntry::warn(format!(
                                        "Rclone remote connection check timed out after 10s: '{check_remote_path}'"
                                    ))))
                                    .await;
                            }
                        }
                    }
                }
            });
        }
        Some(backend)
    } else {
        let _ = event_tx
            .send(AppEvent::Log(LogEntry::cloud(
                "'remote_path' is empty; running in local-only recording mode",
            )))
            .await;
        None
    };

    let chzzk = ChzzkClient::new(&settings.chzzk);
    let orchestrator = Arc::new(EngineOrchestrator::with_cancel_token(
        settings.clone(),
        chzzk,
        upload_backend,
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
                match item {
                    Ok(Event::Key(key)) => {
                        if key.kind != crossterm::event::KeyEventKind::Release {
                            if key.code == KeyCode::Char('q') && key.kind == crossterm::event::KeyEventKind::Press {
                                if app.is_shutting_down {
                                    // Second 'q' press triggers immediate exit
                                    app.should_quit = true;
                                    cancel_token.cancel();
                                    break;
                                }
                                app.is_shutting_down = true;
                                cancel_token.cancel();
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
                    Ok(Event::Resize(width, height)) => {
                        app.handle_event(AppEvent::Resize(width, height));
                        needs_redraw = true;
                    }
                    _ => {}
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
