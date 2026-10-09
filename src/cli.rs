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
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(clap::Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum Commands {
    #[command(about = "Consolidate recorded video and chat chunks into unified files")]
    Consolidate(ConsolidateArgs),
}

#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
pub struct ConsolidateArgs {
    #[arg(help = "Local directory or remote rclone path to recording session")]
    pub path: String,

    #[arg(
        long,
        help = "Retain original video and chat chunks after successful consolidation"
    )]
    pub keep_original: bool,

    #[arg(
        long,
        help = "Overwrite existing consolidated.mp4 or consolidated.jsonl files"
    )]
    pub overwrite: bool,

    #[arg(
        long,
        help = "Abort consolidation if chunk gaps or malformed entries are encountered"
    )]
    pub strict: bool,

    #[arg(
        long,
        default_value = "16",
        value_parser = parse_delete_concurrency,
        help = "Maximum concurrent deletion tasks when purging original chunks"
    )]
    pub delete_concurrency: usize,
}

fn parse_delete_concurrency(s: &str) -> Result<usize, String> {
    let val: usize = s
        .parse()
        .map_err(|e| format!("invalid integer '{s}': {e}"))?;
    if val == 0 {
        return Err("delete concurrency must be greater than 0".to_string());
    }
    Ok(val)
}
