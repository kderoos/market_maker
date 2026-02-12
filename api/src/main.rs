// Here is the old main.rs that did work.
// use std::fs;
// use engine::{config::EngineConfig, Engine};
// use tracing_subscriber;

// pub fn load_config(path: &str) -> EngineConfig {
//     let text = fs::read_to_string(path).expect("Failed to read config");
//     toml::from_str(&text).expect("Invalid config.toml")
// }
// #[tokio::main]
// pub async fn main() {
//     tracing_subscriber::fmt::init();
//     let cfg = load_config("./config/avellaneda.toml");
//     let engine = Engine::init(cfg);

//     // Keep process alive
//     futures::future::pending::<()>().await;
// }

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

fn load_config(path: &PathBuf) -> EngineConfig {
    let text = fs::read_to_string(path).expect("Failed to read config");
    toml::from_str(&text).expect("Invalid config.toml")
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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
    // Keep process alive
    futures::future::pending::<()>().await;
}
 