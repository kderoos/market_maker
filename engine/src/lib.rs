pub mod book;
mod penetration;
mod regression;
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

        let book_state = Arc::new(RwLock::new(OrderBook::default()));
        
        // Spawn engine tasks
        let book_state_clone = book_state.clone();
        tokio::spawn(book_engine(rx_exchange, book_state_clone));
        tokio::spawn(pub_book_depth(tx_ws.clone(), book_state.clone()));
        // tokio::spawn(penetration::midprice_sampler(
        //     tx_exchange.clone(), // Mid-price sampler sends to engine instead of ws.
        //     book_state.clone(),
        //     1000,
        //     "XBTUSDT".to_string(),
        // ));
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
