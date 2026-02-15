//! Binary entrypoint for the market-making engine.
//!
//! Responsibilities:
//! - Parse CLI arguments (live / backtest).
//! - Load and optionally override `EngineConfig` from TOML.
//! - Initialize logging via `tracing`.
//! - Bootstrap the async engine runtime.
//!
//! All trading logic, execution modeling, and strategy behavior
//! reside in the `engine` crate.

use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use engine::{config::EngineConfig, Engine};
use tracing_subscriber;

/// Simple market-making engine CLI
#[derive(Parser, Debug)]
#[command(name = "mm")]
#[command(about = "Market making simulation", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run in live mode (connectors)
    Live {
        /// Path to config.toml
        #[arg(long)]
        config: PathBuf,

        /// Trading symbol (overrides config)
        #[arg(long)]
        symbol: Option<String>,

        /// Output directory (overrides config.output.csv_path)
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Run in backtest mode (CSV / Tardis)
    Backtest {
        /// Path to config.toml
        #[arg(long)]
        config: PathBuf,

        /// Data root or file (overrides config.data.* root)
        #[arg(long)]
        data: Option<PathBuf>,

        /// Output directory (overrides config.output.csv_path)
        #[arg(long)]
        output: Option<PathBuf>,
    },
}
/// Loads and deserializes the engine configuration from a TOML file.
///
/// # Panics
/// Panics if the file cannot be read or if deserialization fails.
fn load_config(path: &PathBuf) -> EngineConfig {
    let text = fs::read_to_string(path).expect("Failed to read config");
    toml::from_str(&text).expect("Invalid config.toml")
}
/// Application entrypoint.
///
/// Initializes logging, parses CLI arguments, applies runtime
/// configuration overrides, and starts the engine.
///
/// The process runs indefinitely until externally terminated.
#[tokio::main]
async fn main() {
    // Default to INFO log level unless RUST_LOG is set.
    use tracing_subscriber::{fmt, EnvFilter};

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();


    let cli = Cli::parse();

    let mut cfg = match &cli.command {
        Commands::Live { config, .. } => load_config(config),
        Commands::Backtest { config, .. } => load_config(config),
    };

    let engine = match cli.command {
        Commands::Live {
            symbol,
            output,
            ..
        } => {
            // Force live mode
            cfg.data.from_csv = false;

            // Override symbol if provided
            if let Some(sym) = symbol {
                cfg.strategy.symbol = sym;
            }

            // Override output path if provided
            if let Some(out) = output {
                cfg.output.csv_path = out.to_string_lossy().to_string();
            }

            Engine::init(cfg)
        }

        Commands::Backtest {
            data,
            output,
            ..
        } => {
            // Force backtest mode
            cfg.data.from_csv = true;

            // Override data root
            if let Some(data) = data {
                cfg.data.tardis_root = Some(data.to_string_lossy().to_string());
            }

            // Override output path if provided
            if let Some(out) = output {
                cfg.output.csv_path = out.to_string_lossy().to_string();
            }

            Engine::init(cfg)
        }
    };
    /// The runtime blocks until a Ctrl+C signal is received.
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    }
 