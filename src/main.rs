use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossterm::cursor::Show;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use tokio_util::sync::CancellationToken;

use chzzk_load::app_path::resolve_path;
use chzzk_load::chzzk::client::ChzzkClient;
use chzzk_load::config::Settings;
use chzzk_load::drive::auth::DriveAuth;
use chzzk_load::drive::client::DriveClient;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::app::App;
use chzzk_load::tui::event::AppEvent;
use chzzk_load::tui::ui::draw_ui;

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
    // Register panic hook to restore terminal on panic
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
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
                    .send(AppEvent::Log(
                        "[INFO] Google Drive authenticated successfully".to_string(),
                    ))
                    .await;
                Some(DriveClient::new(Arc::new(auth)))
            }
            Err(e) => {
                let _ = event_tx
                    .send(AppEvent::Log(format!("[WARN] Drive auth failed: {}", e)))
                    .await;
                None
            }
        }
    } else {
        let _ = event_tx
            .send(AppEvent::Log(format!(
                "[INFO] '{}' not found; running in local-only recording mode",
                creds_path.display()
            )))
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
                    std::process::exit(130);
                } else {
                    cancel_token_ctrlc.cancel();
                }
            }
        }
    });

    let orch_handle = tokio::spawn(orchestrator.clone().run());

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    for ch in &settings.channels {
        app.channels.push(chzzk_load::tui::app::ChannelItem {
            id: ch.id.clone(),
            name: ch.name.clone(),
            is_live: false,
            is_active: false,
            title: "Checking...".to_string(),
        });
    }

    // Main TUI render loop
    let tick_rate = Duration::from_millis(100);
    let mut shutdown_start: Option<std::time::Instant> = None;

    loop {
        // Check if external shutdown signal (e.g. Ctrl+C) was received
        if cancel_token.is_cancelled() && !app.is_shutting_down {
            app.is_shutting_down = true;
            shutdown_start = Some(std::time::Instant::now());
        }

        // Check exit conditions
        if app.should_quit {
            cancel_token.cancel();
            break;
        }
        if app.is_shutting_down {
            if orch_handle.is_finished() {
                break;
            }
            if let Some(start) = shutdown_start
                && start.elapsed() >= Duration::from_secs(10)
            {
                break;
            }
        }

        terminal.draw(|f| draw_ui(f, &app))?;

        if event::poll(tick_rate)?
            && let Event::Key(key) = event::read()?
        {
            if key.code == KeyCode::Char('q') {
                if app.is_shutting_down {
                    // Second 'q' press triggers immediate exit
                    app.should_quit = true;
                    cancel_token.cancel();
                    break;
                } else {
                    app.is_shutting_down = true;
                    cancel_token.cancel();
                    shutdown_start = Some(std::time::Instant::now());
                }
            } else {
                app.handle_event(AppEvent::Key(key));
            }
        }

        while let Ok(ev) = event_rx.try_recv() {
            app.handle_event(ev);
        }

        if app.refresh_requested {
            app.refresh_requested = false;
            let _ = event_tx
                .send(AppEvent::Log(
                    "[INFO] Manual refresh triggered...".to_string(),
                ))
                .await;
            orchestrator.trigger_refresh();
        }

        // Re-check exit conditions after event processing
        if app.should_quit {
            cancel_token.cancel();
            break;
        }
        if app.is_shutting_down {
            if orch_handle.is_finished() {
                break;
            }
            if let Some(start) = shutdown_start
                && start.elapsed() >= Duration::from_secs(10)
            {
                break;
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
