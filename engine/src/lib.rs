//! Core asynchronous trading engine.
//!
//! The engine wires together:
//! - Market data connectors (live or historical replay)
//! - Optional data preprocessing pipeline
//! - Strategy evaluation
//! - Deterministic execution simulator
//! - Position tracking
//! - Output sinks (console / CSV)
//!
//! The architecture is message-driven and built around bounded
//! Tokio mpsc and broadcast channels to enforce backpressure
//! and deterministic replay semantics.
//!
//! # Engine Architecture
//!
//! ## Data Flow
//!
//! Connector → tx_exchange (mpsc)
//!     → fan-out task
//!         → tx_engine (execution engine)
//!         → tx_transformer (strategy preprocessing)
//!         → tx_candle_in (candle service)
//!
//! Execution Engine
//!     → ExecutionEvent
//!         → Strategy
//!         → Position Engine
//!         → Output
//!
//! Strategy
//!     → Order
//!         → Execution Engine
//!         → Output

pub mod book;
pub mod output;
mod features;
pub mod config;
mod penetration;
mod regression;
mod volatility;
mod position;
pub mod strategy_factory;
pub mod execution;

use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::info;

use common::{
    AnyUpdate, AnyWsUpdate, ChannelType, ConnectorCommand,
    ExecutionEvent, Order, Connector
};

use book::OrderBook;
use config::{DataPreprocessingType, EngineConfig};
use connectors_bitmex::BitmexConnector;
use connectors_tardis::TardisConnector;
use execution::run_deterministic;
use features::{
    passthrough::PassThroughDataTransformer,
    sequenced::AvellanedaDataTransformer,
    transform::DataTransformer,
};
use output::{
    console::ConsoleSink,
    csv::SimpleCsvSink,
    event::OutputEvent,
    sink::OutputSink
};
use position::run_position_engine;
use strategy::runner::run_strategy;
use strategy_factory::build_strategy;
use utils::candle_service::CandleService;

/// Central orchestration struct for the trading engine.
///
/// `Engine` owns the command broadcast channel and
/// initializes all async subsystems.
///
/// It does not perform trading logic itself —
/// it wires together independent tasks.
pub struct Engine {
    tx_cmd: broadcast::Sender<ConnectorCommand>,
    pub tx_ws: broadcast::Sender<AnyWsUpdate>,
    book_state: Arc<RwLock<OrderBook>>,
}

impl Engine {
    /// Initializes and spawns all engine subsystems.
    ///
    /// Responsibilities:
    /// - Create bounded async channels
    /// - Spawn connector (live or CSV replay)
    /// - Spawn preprocessing pipeline
    /// - Spawn strategy runner
    /// - Spawn deterministic execution engine
    /// - Spawn position engine
    /// - Spawn output sinks
    ///
    /// # Determinism
    /// In backtest mode, replay order is preserved via single-threaded
    /// event routing and bounded channels.
    ///
    /// # Panics
    /// May panic if required configuration fields are missing.
    pub fn init(cfg: EngineConfig) -> Self {
        info!("Initializing engine...");

        // ---- Channel Topology ----
        // Exchange updates → execution + transformer
        // Strategy orders → execution
        // Execution events → strategy + position + output
        // Output events → console + CSV

        let (tx_exchange, rx_exchange) = mpsc::channel::<AnyUpdate>(10_000);
        
        // broadcast channels for commands and ws updates.
        let (tx_cmd, _) = broadcast::channel::<ConnectorCommand>(100);
        let (tx_ws, _) = broadcast::channel::<AnyWsUpdate>(1000);
        let (tx_mid_price, _) = broadcast::channel::<AnyWsUpdate>(1000);
        
        // mpsc channel for consumers of exchange updates.
        let (tx_book, rx_book) = mpsc::channel::<AnyUpdate>(5000);
        let (tx_trade_exe, rx_trade_exe) = mpsc::channel::<AnyUpdate>(2000);
        let (tx_engine, rx_engine) = mpsc::channel::<AnyUpdate>(5000);
        
        // channel for candle service.
        let (tx_candle_in, rx_candle_in) = mpsc::channel::<AnyWsUpdate>(500);

        // Create new orderbook.
        let book_state = Arc::new(RwLock::new(OrderBook::default()));

        // Spawn data transformer to get in/output channels.
        let transformer: Box<dyn DataTransformer> = match cfg.strategy.preprocessing {
            DataPreprocessingType::NoDataTransform => {
                Box::new(PassThroughDataTransformer)
            }
            DataPreprocessingType::Avellaneda => {
                Box::new(AvellanedaDataTransformer {
                    cfg: cfg.strategy.avellaneda
                        .clone()
                        .expect("Avellaneda config must be set"),
                    symbol: cfg.strategy.symbol.clone(),
                })
            }
        };

        let (tx_transformer, rx_strategy) = transformer.spawn();

        // T intersection to forward book updates and trade updates to matching/book engine 
        // and copies all updates to data transformer (preprocessing for strategy inputs).
        let tx_ws_clone = tx_ws.clone();
        tokio::spawn(async move {
            let mut rx_exchange = rx_exchange;
            loop {
                let update = rx_exchange.recv().await;
                match update {
                    Some(update) => {
                        match &update {
                            AnyUpdate::BookUpdate(_) => {
                                let _ = tx_engine.send(update.clone()).await;
                                let _ = tx_transformer.send(update.clone()).await;
                            }
                            AnyUpdate::TradeUpdate(t) => {
                                let _ = tx_engine.send(update.clone()).await;
                                let _ = tx_transformer.send(update.clone()).await;
                                let u = AnyWsUpdate::Trade(t.clone());
                                let _ = tx_candle_in.send(u).await;
                            }
                            AnyUpdate::QuoteUpdate(quote) => {
                                let _ = tx_transformer.send(update.clone()).await;
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        });
        let (tx_candle_out, mut rx_candle_out) = mpsc::channel::<AnyWsUpdate>(100);
        // Spawn candle service
        if let Some(candle_size) = cfg.output.candle_size_min {
            info!("Starting candle service with {} min candles...", candle_size);
            let candle_service = CandleService::new(cfg.strategy.symbol.clone(),
                                                1000*60*candle_size as i64, // convert to microsecond.
                                                rx_candle_in,
                                                tx_candle_out);
            candle_service.spawn();        
        }


        // // spawn execution engine
        let (tx_exec, mut rx_exec) = mpsc::channel::<ExecutionEvent>(100);
        let (tx_order,mut rx_order) = mpsc::channel::<Order>(100);

        if cfg.data.from_csv {
            // Tardis connector from CSV files
            info!("Starting Tardis connector from CSV files...");
            // Build file paths from config
            let data_root = cfg.data.tardis_root.clone().expect("data_root must be set for CSV data source");
            let trades_path = PathBuf::from(&data_root)
                .join(cfg.data.trades_csv.as_ref().expect("missing trades_csv"));
            let quotes_path = PathBuf::from(&data_root)
                .join(cfg.data.quotes_csv.as_ref().expect("missing trades_csv"));           
            let book_path = PathBuf::from(&data_root)
                .join(cfg.data.book_csv.as_ref().expect("missing trades_csv"));           
            

            // Define Tardis paths
            let paths = common::TardisPaths {
                trades: PathBuf::from(trades_path),
                book: Some(PathBuf::from(book_path)),
                quotes: Some(PathBuf::from(quotes_path)),
            };

            // Create Tardis connector
            let mut tardis = TardisConnector::new(paths).unwrap();
            let tx_tardis_updates = tx_exchange.clone();
            let rx_tardis_cmd = tx_cmd.subscribe();
            
            // Spawn Tardis connector task
            tokio::spawn(async move {
                tardis.run(tx_tardis_updates, rx_tardis_cmd).await;
            });
        } else {
            info!("Starting live Bitmex connector...");
            // Bitmex connector
            let mut bitmex = BitmexConnector::default(); 
            let tx_bitmex_updates = tx_exchange.clone();
            let rx_bitmex_cmd = tx_cmd.subscribe();
            tokio::spawn(async move {
                bitmex.run(tx_bitmex_updates, rx_bitmex_cmd).await;
            });
            // subscibe to channels trade, quote and book.
            let msg_trade = ConnectorCommand::Subscribe{ symbol: cfg.strategy.symbol.clone(), channel: ChannelType::Trade };
            let msg_book = ConnectorCommand::Subscribe{ symbol: cfg.strategy.symbol.clone(), channel: ChannelType::Book };
            let msg_quote = ConnectorCommand::Subscribe{ symbol: cfg.strategy.symbol.clone(), channel: ChannelType::Quote };
            
            let _ = tx_cmd.send(msg_trade);
            let _ = tx_cmd.send(msg_book);
            let _ = tx_cmd.send(msg_quote);
        }


        // Build Strategy
        let strategy = build_strategy(&cfg.strategy);

        // fan-out executions to position engine and strategy
        let (tx_exec_strat, rx_exec_strat) = mpsc::channel::<ExecutionEvent>(100);
        let (tx_exec_pos, mut rx_exec_pos) = mpsc::channel::<ExecutionEvent>(100);
        let (tx_exec_out, mut rx_exec_out) = mpsc::channel::<ExecutionEvent>(100);

        // Spawn a task to forward executions to both strategy and position engine
        tokio::spawn(async move {
            while let Some(ex) = rx_exec.recv().await {
                let _ = tx_exec_strat.send(ex.clone()).await;
                let _ = tx_exec_pos.send(ex.clone()).await;
                let _ = tx_exec_out.send(ex.clone()).await;
            }
        });

        // Fan out orders to Output writer
        let (tx_order_out, mut rx_order_out) = mpsc::channel::<Order>(100);
        let (tx_order_exec, mut rx_order_exec) = mpsc::channel::<Order>(100);
        tokio::spawn(async move {
            while let Some(order) = rx_order.recv().await {
                let _ = tx_order_out.send(order.clone()).await;
                let _ = tx_order_exec.send(order.clone()).await;
            }
        });

        // Spawn strategy runner
        tokio::spawn(run_strategy(strategy,
                rx_strategy, //Receiver <StrategyInput>
                rx_exec_strat, // Receiver <ExecutionEvent>
                tx_order, // Sender <Order>
                )
        );
        //Execution engine
        tokio::spawn(run_deterministic(
            rx_engine,  //Receiver<AnyUpdate>,
            rx_order_exec,   //Receiver<Order>,
            tx_exec,    //Sender<ExecutionEvent>,
            cfg.execution.latency_ms,//latency in ms
        ));

        // Position engine
        let (tx_position, mut rx_position) = mpsc::channel::<common::PositionState>(500);
        tokio::spawn(run_position_engine(
            cfg.position.clone(), //PositionEngineConfig,
            rx_exec_pos, //Receiver<ExecutionEvent>,
            tx_position,
        ));

        let (tx_output, mut rx_output) = mpsc::channel::<OutputEvent>(1000);
        // Channel fan-in for Output writer
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(pos) = rx_position.recv() => {
                        let output_event = OutputEvent::Position(pos);
                        let _ = tx_output.send(output_event).await;
                    }
                    Some(o) = rx_order_out.recv() => {
                        let output_event = OutputEvent::Order(o);
                        let _ = tx_output.send(output_event).await;
                    }
                    Some(ex) = rx_exec_out.recv() => {
                        let output_event = OutputEvent::Trade(ex);
                        let _ = tx_output.send(output_event).await;
                    }
                    Some(c) = rx_candle_out.recv() => {
                        let c = match c {
                            AnyWsUpdate::TradeCandle(c) => {
                                c
                            },
                            _ => continue,
                        };
                        let output_event = OutputEvent::Price(c);
                        let _ = tx_output.send(output_event).await;}
                }
            }
        });
        // Fan out rx_output to multiple sinks
        let (tx_console, mut rx_console) = mpsc::channel::<OutputEvent>(1000);
        let (tx_csv, mut rx_csv) = mpsc::channel::<OutputEvent>(1000);
        tokio::spawn(async move {
            while let Some(event) = rx_output.recv().await {
                let _ = tx_console.send(event.clone()).await;
                let _ = tx_csv.send(event.clone()).await;
            }
        });

        // Console Output sink
        let mut sink = Arc::new(ConsoleSink::new());
        tokio::spawn(async move {
            while let Some(event) = rx_console.recv().await {
                sink.handle(event).await;
            }
        });
        // Build output directory if it doesn't exist
        std::fs::create_dir_all(&cfg.output.csv_path).expect("Failed to create output directory");
        // Filename for CSV output
        let filename_prefix = select_next_filename(&cfg.output.csv_path, &cfg.output.exchange, &cfg.strategy.symbol, &cfg.output.date);
        let base_path = PathBuf::from(&cfg.output.csv_path);
        // write config
        write_final_config(&cfg, &base_path, &filename_prefix).expect("Failed to write config");
        // CSV Output sink
        let sink = SimpleCsvSink::new(
            base_path,
            &filename_prefix,
        );
        // Spawn the single task that handles all CSV writing
        tokio::spawn(async move {
            sink.run(rx_csv).await;
        });

        Engine {
            tx_cmd,
            tx_ws,
            book_state,
        }
    }

    /// Sends a command to the active connector via broadcast channel.
    /// Typically used to subscribe/unsubscribe to market data streams.
    pub fn send_cmd(&self, cmd: ConnectorCommand) {
        let _ = self.tx_cmd.send(cmd);
    }
}

/// Generates a unique filename prefix by incrementing a run ID
/// if files already exist in the output directory.
fn select_next_filename(base_path: &str, exchange: &str, symbol: &str, date: &str) -> String {
    let mut run_id = 0;
    loop {
        let filename = format!("{}_{}_{}_run{}_config.toml", exchange, symbol, date, run_id);
        let path = PathBuf::from(base_path).join(filename);
        if !path.exists() {
            break;
        }
        run_id += 1;
    }
    format!("{}_{}_{}_run{}", exchange, symbol, date, run_id)
}

/// Writes the effective runtime configuration to disk
/// alongside output results for reproducibility.
fn write_final_config(cfg: &EngineConfig, base_path: &PathBuf, filename_prefix: &str) -> std::io::Result<()> {
    let path = base_path.join(format!("{}_config.toml", filename_prefix));
    let toml = toml::to_string_pretty(cfg).expect("Failed to serialize config");
    info!("Writing config to: {}", path.display());
    let mut f = File::create(path)?;
    f.write_all(toml.as_bytes())?;
    Ok(())
}
