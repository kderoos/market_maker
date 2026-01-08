pub mod book;
mod penetration;
mod regression;
mod volatility;
mod position;
pub mod execution;

use book::{book_engine, print_book,pub_book_depth, OrderBook};

use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::{RwLock,broadcast};
use common::{Connector, AnyWsUpdate, ChannelType, ConnectorCommand};
use connectors_bitmex::BitmexConnector;
use connectors_bitvavo::BitvavoConnector;
use utils::forward::ws_forward_trade_quote;
use strategy::{runner::run_strategy, avellaneda::AvellanedaStrategy};
use position::run_position_engine;

pub struct Engine {
    tx_cmd: broadcast::Sender<ConnectorCommand>,
    pub tx_ws: broadcast::Sender<AnyWsUpdate>,
    book_state: Arc<RwLock<OrderBook>>,
}

impl Engine {
    pub fn init() -> Self {
        let (tx_exchange, rx_exchange) = broadcast::channel(1000);
        let (tx_cmd, _) = broadcast::channel(100);
        let (tx_ws, _) = broadcast::channel(1000);
        let (tx_mid_price, _) = broadcast::channel(1000);

        let book_state = Arc::new(RwLock::new(OrderBook::default()));
        
        // Spawn engine tasks
        let book_state_clone = book_state.clone();
        tokio::spawn(book_engine(rx_exchange, book_state_clone));
        // Spawn mid_price publisher and volatility engine
        let book_state_clone = book_state.clone();
        tokio::spawn(volatility::midprice_sampler(
            tx_mid_price.clone(),
            book_state_clone,
            1000, //interval_ms
            "XBTUSDT".to_string(),
        ));
        tokio::spawn(volatility::volatility_engine(
            tx_mid_price.subscribe(),
            tx_ws.clone(),
            60,    //window_len
            1000.0,  //sample_interval_ms
            "XBTUSDT".to_string(),
        ));

        // Spawn penetration analyzer
        let rx_penetration = tx_exchange.subscribe(); 
        tokio::spawn(penetration::engine(
            rx_penetration,
            tx_ws.clone(),
            book_state.clone(),
            120, //window_len
            500, //num_bins
            500, //interval_ms
            "XBTUSDT".to_string(),
        ));
        // Forward trade, quote from rx_exchange to tx_ws
        tokio::spawn(ws_forward_trade_quote(
            tx_exchange.subscribe(),
            tx_ws.clone(),
        ));
        

        // spawn execution engine
        let (tx_exec, _) = broadcast::channel(100);
        let (tx_order,_) = broadcast::channel(100);
        let book_state_exec = book_state.clone();
        tokio::spawn(execution::run(
            book_state_exec,
            tx_ws.subscribe(), // Receiver <AnyWsUpdate>
            tx_order.subscribe(), //Receiver <Order>
            tx_exec.clone(), // Sender <ExecutionEvent>
        ));

        // Spawn connectors
        let mut bitmex = BitmexConnector::default();
        let tx_bitmex_updates = tx_exchange.clone();
        let rx_bitmex_cmd = tx_cmd.subscribe();
        tokio::spawn(async move {
            bitmex.run(tx_bitmex_updates, rx_bitmex_cmd).await;
        });

        // Avellaneda strategy
        let mut strategy = AvellanedaStrategy::new(
            "XBTUSDT".to_string(),
            1,    // quote size
            100,   // max position
            0.1,   // gamma
            0.2,   // delta
            0.1,   // xi
        );
        tokio::spawn(run_strategy(strategy,
                tx_ws.subscribe(), //Receiver <AnyWsUpdate>
                tx_exec.subscribe(), // Receiver <ExecutionEvent>
                tx_order, // Sender <Order>
                )
        );

        tokio::spawn(run_position_engine(
            tx_exec.subscribe(),
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
