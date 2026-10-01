use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone, PartialEq, Eq)]
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
    #[arg(
        long,
        visible_alias = "no-tui",
        help = "Run in headless console mode without TUI dashboard (alias: --no-tui, auto-enabled when stdout is not a TTY)"
    )]
    pub headless: bool,
}
