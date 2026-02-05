pub mod book;
pub mod output;
mod features;
mod config;
mod penetration;
mod regression;
mod volatility;
mod position;
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
use config::StrategyConfig;
use std::path::PathBuf;

use volatility::run_tick_volatility_sample_ema;

pub struct Engine {
    tx_cmd: broadcast::Sender<ConnectorCommand>,
    pub tx_ws: broadcast::Sender<AnyWsUpdate>,
    book_state: Arc<RwLock<OrderBook>>,
}

impl Engine {
    pub fn init() -> Self {
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

        let cfg = StrategyConfig::new("avellaneda".to_string(), "XBTUSDT".to_string());
        // Spawn data transformer to get in/output channels.
        let transformer: Box<dyn DataTransformer> = match cfg.kind.as_str() {
            "momentum" => Box::new(PassThroughDataTransformer),
            "avellaneda" => Box::new(AvellanedaDataTransformer { symbol: cfg.symbol.clone() }),
            other => panic!("Unknown strategy kind: {}", other),
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
        // // Create sequencer channels
        // let (tx_vol, mut rx_vol) = mpsc::channel::<common::VolatilityUpdate>(1000);
        // let (tx_pen, mut rx_pen) = mpsc::channel::<common::PenetrationUpdate>(1000);
        // let (tx_strategy, mut rx_strategy) = mpsc::channel::<common::StrategyInput>(1000);

        // // Spawn engine tasks
        // tokio::spawn(run_tick_volatility_sample_ema(
        //     rx_quote_vol,
        //     tx_vol,
        //     500, //window_len (ticks)
        //     0.500, //interval_s
        //     2.0, //ema_half_life (s)
        //     None, //sigma_min
        //     None, //sigma_max
        //     "XBTUSDT".to_string(),
        // ));

        // // Spawn penetration analyzer
        // tokio::spawn(penetration::engine(
        //     rx_trade_quote_pen,
        //     tx_pen,
        //     // book_state.clone(),
        //     120, //window_len
        //     500, //num_bins
        //     500, //interval_ms
        //     "XBTUSDT".to_string(),
        // ));

        // // Spawn sequencer
        // tokio::spawn(sequencer::sequencer_run(
        //     500, //interval_ms
        //     rx_vol,
        //     rx_pen,
        //     rx_quote_seq,
        //     tx_strategy.clone(),
        // ));

        // // Forward trade, quote from rx_exchange to tx_ws
        // tokio::spawn(ws_forward_trade_quote(
        //     tx_exchange.subscribe(),
        //     tx_ws.clone(),
        // ));

        // // Spawn book engine
        // let book_state_clone = book_state.clone();
        // tokio::spawn(book_engine(
        //     rx_engine,
        //     book_state_clone,
        // ));

        // // spawn execution engine
        let (tx_exec, mut rx_exec) = mpsc::channel::<ExecutionEvent>(100);
        let (tx_order,mut rx_order) = mpsc::channel::<Order>(100);

        // // Spawn connectors
        let data_root = "/opt/tardisData/datasets/".to_string();

        let trades_path = data_root.clone() + "bitmex_trades_2024-08-01_XBTUSD.csv.gz";
        let quotes_path = data_root.clone() + "bitmex_quotes_2024-08-01_XBTUSD.csv.gz";
        let book_path = data_root.clone() + "bitmex_incremental_book_L2_2024-08-01_XBTUSD.csv.gz";


        // Tardis connector
        let paths = common::TardisPaths {
            trades: PathBuf::from(trades_path),
            book: Some(PathBuf::from(book_path)),
            quotes: Some(PathBuf::from(quotes_path)),
        };

        let mut tardis = TardisConnector::new(paths).unwrap();
        let tx_tardis_updates = tx_exchange.clone();
        let rx_tardis_cmd = tx_cmd.subscribe();
        tokio::spawn(async move {
            tardis.run(tx_tardis_updates, rx_tardis_cmd).await;
        });

        // // Bitmex connector
        // let mut bitmex = BitmexConnector::default();
        // let tx_bitmex_updates = tx_exchange.clone();
        // let rx_bitmex_cmd = tx_cmd.subscribe();
        // tokio::spawn(async move {
        //     bitmex.run(tx_bitmex_updates, rx_bitmex_cmd).await;
        // });
        
        // Avellaneda strategy
        let mut strategy = AvellanedaStrategy::new(
            "XBTUSDT".to_string(),
            0.01,  // tick size
            1,    // quote size
            100,   // max position
            0.1,   // gamma
            0.2,   // delta
            0.1,   // xi
        );
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
            0,         //latency in ms
        ));

        // Position engine
        let (tx_position, mut rx_position) = mpsc::channel::<common::PositionState>(500);
        tokio::spawn(run_position_engine(
            rx_exec_pos, //Receiver<ExecutionEvent>,
            tx_position,
        ));

        let (tx_output, mut rx_output) = mpsc::channel::<OutputEvent>(1000);
        // Output channel fan-in
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
        // let mut rx_csv = rx_output.clone();
        tokio::spawn(async move {
            while let Some(event) = rx_console.recv().await {
                sink.handle(event).await;
            }
        });

        // CSV Output sink
        let sink = SimpleCsvSink::new("/tmp/engine_output");
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
