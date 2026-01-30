pub mod book;
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
        let (tx_trade_quote_pen, rx_trade_quote_pen) = mpsc::channel::<AnyUpdate>(2000);
        let (tx_quote_vol, rx_quote_vol) = mpsc::channel::<AnyUpdate>(2000);
        let (tx_quote_seq, mut rx_quote_seq) = mpsc::channel::<common::QuoteUpdate>(1000);
        let (tx_engine, rx_engine) = mpsc::channel::<AnyUpdate>(5000);

        let book_state = Arc::new(RwLock::new(OrderBook::default()));
        // Fan out rx_exchange to multiple engines
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
                            }
                            AnyUpdate::TradeUpdate(_) => {
                                let _ = tx_engine.send(update.clone()).await;
                                let _ = tx_trade_quote_pen.send(update.clone()).await;
                            }
                            AnyUpdate::QuoteUpdate(quote) => {
                                let _ = tx_trade_quote_pen.send(update.clone()).await;
                                let _ = tx_quote_vol.send(update.clone()).await;
                                let _ = tx_quote_seq.send(quote.clone()).await;
                                // let _ = tx_ws_clone.send(AnyWsUpdate::Quote(quote.clone())).unwrap();
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        });
        // Create sequencer channels
        let (tx_vol, mut rx_vol) = mpsc::channel::<common::VolatilityUpdate>(1000);
        let (tx_pen, mut rx_pen) = mpsc::channel::<common::PenetrationUpdate>(1000);
        let (tx_strategy, mut rx_strategy) = mpsc::channel::<common::StrategyInput>(1000);

        // Spawn engine tasks
        tokio::spawn(run_tick_volatility_sample_ema(
            rx_quote_vol,
            tx_vol,
            500, //window_len (ticks)
            0.500, //interval_s
            2.0, //ema_half_life (s)
            None, //sigma_min
            None, //sigma_max
            "XBTUSDT".to_string(),
        ));

        // Spawn penetration analyzer
        tokio::spawn(penetration::engine(
            rx_trade_quote_pen,
            tx_pen,
            // book_state.clone(),
            120, //window_len
            500, //num_bins
            500, //interval_ms
            "XBTUSDT".to_string(),
        ));

        // Spawn sequencer
        tokio::spawn(sequencer::sequencer_run(
            500, //interval_ms
            rx_vol,
            rx_pen,
            rx_quote_seq,
            tx_strategy.clone(),
        ));

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
        // Spawn a task to forward executions to both strategy and position engine
        tokio::spawn(async move {
            while let Some(ex) = rx_exec.recv().await {
                let _ = tx_exec_strat.send(ex.clone()).await;
                let _ = tx_exec_pos.send(ex.clone()).await;
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
            rx_order,   //Receiver<Order>,
            tx_exec,    //Sender<ExecutionEvent>,
            0,         //latency in ms
        ));

        // //Mock execution engine by directly sending executions on order events
        // tokio::spawn(async move {
        //     while let Some(order) = rx_order.recv().await {
        //         println!("🧪 Order received (debug exec): {:?}", order);
            
        //         use common::{ExecutionEvent, Order};
            
        //         let (symbol, side, price, size, client_id) = match order {
        //             Order::Limit {
        //                 symbol,
        //                 side,
        //                 price,
        //                 size,
        //                 client_id,
        //                 ..
        //             } => (symbol, side, price, size, client_id),
        //             _ => continue, // only handle limit orders in this mock
                
        //         };
        //         let now = chrono::Utc::now().timestamp_micros();
        //         let exec = ExecutionEvent {
        //             action: "Fill".to_string(), // important
        //             symbol,
        //             order_id: client_id.unwrap_or(0) as i64,               // mock fill
        //             side,
        //             price,
        //             size,
        //             ts_exchange: Some(now),      // mock fill
        //             ts_received: now,
        //         };
            
        //         // broadcast::Sender::send is non-async
        //         let _ = tx_exec.send(exec);
        //     }
        // });


        tokio::spawn(run_position_engine(
            rx_exec_pos, //Receiver<ExecutionEvent>,
            // tx_position,
        ));

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
