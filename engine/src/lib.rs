pub mod book;
mod penetration;
mod regression;
mod volatility;
pub mod execution;

use book::{book_engine, print_book,pub_book_depth, OrderBook};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use common::{Connector, AnyWsUpdate, ChannelType, ConnectorCommand};
use connectors_bitmex::BitmexConnector;
use connectors_bitvavo::BitvavoConnector;

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
        tokio::spawn(volatility::mid_price_sampler(
            tx_mid_price.clone(),
            book_state_clone,
            1000, //interval_ms
        ));
        tokio::spawn(volatility::engine(
            tx_mid_price.subscribe(),
            tx_ws.clone(),
            60,    //window_len
            1000,  //sample_interval_ms
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

        // spawn execution engine
        tokio::spawn(execution::run(
            
        ));

        // Spawn connectors
        let mut bitmex = BitmexConnector::default();
        let tx_bitmex_updates = tx_exchange.clone();
        let rx_bitmex_cmd = tx_cmd.subscribe();
        tokio::spawn(async move {
            bitmex.run(tx_bitmex_updates, rx_bitmex_cmd).await;
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
