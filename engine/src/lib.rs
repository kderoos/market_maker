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

use book::{book_engine, print_book,pub_book_depth, OrderBook};

use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::{RwLock,mpsc,broadcast};
use common::{Order, ExecutionEvent, Connector, AnyWsUpdate, AnyUpdate, ChannelType, ConnectorCommand};
use connectors_bitmex::BitmexConnector;
use connectors_bitvavo::BitvavoConnector;
use connectors_tardis::TardisConnector;
use utils::forward::ws_forward_trade_quote;
use strategy::{sequencer,runner::run_strategy, avellaneda::AvellanedaStrategy};
use position::run_position_engine;
use execution::run_deterministic;
use output::console::ConsoleSink;
use output::event::OutputEvent;
use output::sink::OutputSink;
use output::csv::SimpleCsvSink;
use features::{build::build_transformer, transform::DataTransformer, passthrough::PassThroughDataTransformer, sequenced::AvellanedaDataTransformer};
use config::{EngineConfig, DataPreprocessingType};
use std::path::PathBuf;
use tracing_subscriber;
use tracing::info;
use strategy_factory::build_strategy;

use volatility::run_tick_volatility_sample_ema;

pub struct Engine {
    tx_cmd: broadcast::Sender<ConnectorCommand>,
    pub tx_ws: broadcast::Sender<AnyWsUpdate>,
    book_state: Arc<RwLock<OrderBook>>,
}

impl Engine {
    pub fn init(cfg: EngineConfig) -> Self {
        info!("Initializing engine...");
        // let (tx_exchange, rx_exchange) = broadcast::channel(1000);
        let (tx_exchange, rx_exchange) = mpsc::channel::<AnyUpdate>(10_000);
        // broadcast channels for commands and ws updates
        let (tx_cmd, _) = broadcast::channel::<ConnectorCommand>(100);
        let (tx_ws, _) = broadcast::channel::<AnyWsUpdate>(1000);
        let (tx_mid_price, _) = broadcast::channel::<AnyWsUpdate>(1000);
        // mpsc channel for consumers of exchange updates
        let (tx_book, rx_book) = mpsc::channel::<AnyUpdate>(5000);
        let (tx_trade_exe, rx_trade_exe) = mpsc::channel::<AnyUpdate>(2000);
        let (tx_engine, rx_engine) = mpsc::channel::<AnyUpdate>(5000);
        let (tx_transformer, rx_transformer) = mpsc::channel::<AnyUpdate>(5000);

        let book_state = Arc::new(RwLock::new(OrderBook::default()));

        // Spawn data transformer to get in/output channels.
        let transformer: Box<dyn DataTransformer> = match cfg.strategy.preprocessing {
            DataPreprocessingType::NoDataTransform => Box::new(PassThroughDataTransformer),
            DataPreprocessingType::Avellaneda => Box::new(AvellanedaDataTransformer { cfg: cfg.strategy.avellaneda.clone()
                                                                    .expect("Avellaneda config must be set for Avellaneda strategy"),
                                                                symbol: cfg.strategy.symbol.clone()
                                                             }),
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
                            AnyUpdate::TradeUpdate(_) => {
                                let _ = tx_engine.send(update.clone()).await;
                                let _ = tx_transformer.send(update.clone()).await;
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

        // // spawn execution engine
        let (tx_exec, mut rx_exec) = mpsc::channel::<ExecutionEvent>(100);
        let (tx_order,mut rx_order) = mpsc::channel::<Order>(100);

        if cfg.data.from_csv {
            // Tardis connector from CSV files
            info!("Starting Tardis connector from CSV files...");
            // Build file paths from config
            let data_root = cfg.data.tardis_root.clone().expect("data_root must be set for CSV data source");
            let trades_path = data_root.clone() + cfg.data.trades_csv.expect("missing filename trades_csv").as_str();
            let quotes_path = data_root.clone() + cfg.data.quotes_csv.expect("missing filename quotes_csv").as_str();
            let book_path = data_root.clone() + cfg.data.book_csv.expect("missing filename book_csv").as_str();
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
            cfg.execution.latency_ms,         //latency in ms
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

        // CSV Output sink
        let sink = SimpleCsvSink::new(
            cfg.output.csv_path.clone(),
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

    pub fn send_cmd(&self, cmd: ConnectorCommand) {
        let _ = self.tx_cmd.send(cmd);
    }
}
