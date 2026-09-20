use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;

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
    let args = Cli::parse();
    let config_path = args
        .config
        .unwrap_or_else(|| resolve_path(&PathBuf::from("settings.json")));

    let settings = Settings::load_or_create_default(&config_path)?;
    let creds_path = resolve_path(&PathBuf::from(&settings.google_drive.credentials_path));
    let token_path = resolve_path(&PathBuf::from(&settings.google_drive.token_path));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<AppEvent>(100);

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

    let chzzk = ChzzkClient::new(&settings.chzzk);
    let orchestrator = Arc::new(EngineOrchestrator::new(
        settings.clone(),
        chzzk,
        drive_client,
        event_tx.clone(),
    ));
    tokio::spawn(orchestrator.run());

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
            title: "Checking...".to_string(),
        });
    }

    // Main TUI render loop
    let tick_rate = Duration::from_millis(100);
    loop {
        terminal.draw(|f| draw_ui(f, &app))?;

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
                app.handle_event(AppEvent::Key(key));
            }
        }

        while let Ok(ev) = event_rx.try_recv() {
            app.handle_event(ev);
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    let _ = disable_raw_mode();
    let _ = crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();

    Ok(())
}
