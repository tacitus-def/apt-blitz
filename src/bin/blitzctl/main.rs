//! blitzctl — control utility for apt-blitz service and data (cache management).

mod cache_cli;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use apt_blitz::config::Config;

#[derive(Parser)]
#[command(
    name = "blitzctl",
    version,
    about = "Control utility for apt-blitz service and data"
)]
struct Cli {
    /// Cache directory (overrides config/env default)
    #[arg(long, global = true)]
    cache_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage the on-disk cache
    Cache {
        #[command(subcommand)]
        sub: cache_cli::CacheCmd,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cache_dir = cli.cache_dir.unwrap_or_else(Config::cache_dir_only);
    match cli.command {
        Command::Cache { sub } => cache_cli::run(cache_dir, sub).await,
    }
}
