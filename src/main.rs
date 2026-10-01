use std::io::IsTerminal;
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
use chzzk_load::cli::Cli;
use chzzk_load::config::Settings;
use chzzk_load::engine::EngineOrchestrator;
use chzzk_load::tui::app::App;
use chzzk_load::tui::ui::draw_ui;
use chzzk_load::tui::{AppEvent, LogEntry};
use chzzk_load::uploader::{RcloneBackend, UploadBackend};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _console_guard = chzzk_load::tui::ConsoleCodePageGuard::init();

    let args = Cli::parse();
    let is_headless = args.headless || !std::io::stdout().is_terminal();

    // Register panic hook to restore terminal on panic (only if TUI was enabled)
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        if !is_headless {
            let _ = disable_raw_mode();
            let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
            chzzk_load::tui::ConsoleCodePageGuard::restore_original();
        }
        default_panic(panic_info);
    }));

    let config_path = args
        .config
        .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.toml")));

    let settings = Settings::load_or_create_default(&config_path)?;

    // Strict semantic configuration validation
    let validation_warnings = match settings.validate() {
        Ok(warnings) => warnings,
        Err(errors) => {
            eprintln!(
                "Configuration validation failed for '{}':",
                config_path.display()
            );
            for err in errors {
                eprintln!("  - {err}");
            }
            std::process::exit(1);
        }
    };

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(1000);
    let cancel_token = CancellationToken::new();

    // Emit configuration advisory warnings
    for warn in validation_warnings {
        let _ = event_tx.send(AppEvent::Log(LogEntry::warn(warn))).await;
    }

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

    // Unified signal handling: SIGINT, SIGTERM, SIGHUP
    // First signal triggers graceful shutdown with 15s timeout; second signal forces immediate exit
    let cancel_token_signal = cancel_token.clone();
    tokio::spawn(async move {
        let mut signal_count = 0;
        while let Some(exit_code) = wait_for_signal().await {
            signal_count += 1;
            if signal_count == 1 {
                cancel_token_signal.cancel();
                if is_headless {
                    eprintln!(
                        "[SHUTDOWN] Signal received. Initiating graceful shutdown (15s timeout)..."
                    );
                }
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    if !is_headless {
                        let _ = disable_raw_mode();
                        let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
                        chzzk_load::tui::ConsoleCodePageGuard::restore_original();
                    } else {
                        eprintln!("[SHUTDOWN] Shutdown grace period expired (15s). Force exiting.");
                    }
                    std::process::exit(1);
                });
            } else {
                if !is_headless {
                    let _ = disable_raw_mode();
                    let _ = crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, Show);
                    chzzk_load::tui::ConsoleCodePageGuard::restore_original();
                }
                std::process::exit(exit_code);
            }
        }
    });

    let mut orch_handle = tokio::spawn(orchestrator.clone().run());

    if is_headless {
        run_headless(&mut event_rx, cancel_token.clone(), &mut orch_handle).await?;
    } else {
        run_tui(
            &settings,
            &mut event_rx,
            event_tx.clone(),
            orchestrator.clone(),
            cancel_token.clone(),
            &mut orch_handle,
        )
        .await?;
    }

    // Drain event_rx in background so event_tx never blocks during shutdown
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

    // Await orchestrator termination with grace period
    if !orch_handle.is_finished() {
        let _ = tokio::time::timeout(Duration::from_secs(15), orch_handle).await;
    }

    // Clean up empty stream session folders inside local recordings directory on shutdown
    let recordings_base = resolve_path(std::path::Path::new(&settings.general.recordings_dir));
    let _ = EngineOrchestrator::cleanup_empty_session_dirs(&recordings_base).await;

    Ok(())
}

async fn run_headless(
    event_rx: &mut tokio::sync::mpsc::Receiver<AppEvent>,
    cancel_token: CancellationToken,
    orch_handle: &mut tokio::task::JoinHandle<()>,
) -> anyhow::Result<()> {
    let is_tty = std::io::stdout().is_terminal();
    println!("[INFO] Running in headless mode (console logging). Press Ctrl+C to stop.");

    loop {
        if cancel_token.is_cancelled() && orch_handle.is_finished() {
            break;
        }

        tokio::select! {
            biased;

            _ = &mut *orch_handle, if cancel_token.is_cancelled() => {
                break;
            }

            Some(ev) = event_rx.recv() => {
                if let AppEvent::Log(entry) = ev {
                    let level_badge = if is_tty {
                        match entry.kind {
                            chzzk_load::tui::event::LogKind::Error => "\x1b[31m[ERROR]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Warn => "\x1b[33m[WARN]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Rec => "\x1b[32m[REC]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Chat => "\x1b[36m[CHAT]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Cloud => "\x1b[35m[CLOUD]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Clean => "\x1b[34m[CLEAN]\x1b[0m",
                            chzzk_load::tui::event::LogKind::Poll => "\x1b[90m[POLL]\x1b[0m",
                            _ => "[INFO]",
                        }
                    } else {
                        match entry.kind {
                            chzzk_load::tui::event::LogKind::Error => "[ERROR]",
                            chzzk_load::tui::event::LogKind::Warn => "[WARN]",
                            chzzk_load::tui::event::LogKind::Rec => "[REC]",
                            chzzk_load::tui::event::LogKind::Chat => "[CHAT]",
                            chzzk_load::tui::event::LogKind::Cloud => "[CLOUD]",
                            chzzk_load::tui::event::LogKind::Clean => "[CLEAN]",
                            chzzk_load::tui::event::LogKind::Poll => "[POLL]",
                            _ => "[INFO]",
                        }
                    };
                    let timestamp = chrono::Local::now().format("%H:%M:%S");
                    println!("{timestamp} {level_badge} {}", entry.message);
                }
            }

            else => {
                break;
            }
        }
    }

    Ok(())
}

async fn run_tui(
    settings: &Settings,
    event_rx: &mut tokio::sync::mpsc::Receiver<AppEvent>,
    event_tx: tokio::sync::mpsc::Sender<AppEvent>,
    orchestrator: Arc<EngineOrchestrator>,
    cancel_token: CancellationToken,
    orch_handle: &mut tokio::task::JoinHandle<()>,
) -> anyhow::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::from_settings(settings);

    // Main TUI render loop
    let mut event_reader = crossterm::event::EventStream::new();
    let mut tick_interval = tokio::time::interval(Duration::from_millis(250));
    let mut needs_redraw = true;

    loop {
        // Check if external shutdown signal was received
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
            _ = &mut *orch_handle, if app.is_shutting_down => {
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

    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal() -> Option<i32> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).ok()?;
    let mut sigterm = signal(SignalKind::terminate()).ok()?;
    let mut sighup = signal(SignalKind::hangup()).ok()?;

    tokio::select! {
        _ = sigint.recv() => Some(130),
        _ = sigterm.recv() => Some(143),
        _ = sighup.recv() => Some(129),
    }
}

#[cfg(windows)]
async fn wait_for_signal() -> Option<i32> {
    tokio::signal::ctrl_c().await.ok().map(|_| 130)
}

#[cfg(not(any(windows, unix)))]
async fn wait_for_signal() -> Option<i32> {
    tokio::signal::ctrl_c().await.ok().map(|_| 130)
}
