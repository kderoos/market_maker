mod book;
mod penetration;
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

        let book_state = Arc::new(RwLock::new(OrderBook {
            timestamp: 0,
            bids: book::OrderBookSide { entries: HashMap::new() },
            asks: book::OrderBookSide { entries: HashMap::new() },
        }));

        // Spawn engine tasks
        let book_state_clone = book_state.clone();
        tokio::spawn(book_engine(rx_exchange, book_state_clone));
        // tokio::spawn(print_book(book_state.clone()));
        tokio::spawn(pub_book_depth(tx_ws.clone(), book_state.clone()));
        tokio::spawn(penetration::midprice_sampler(
            tx_exchange.clone(), // Mid-price sampler sends to engine instead of ws.
            book_state.clone(),
            1000,
            "XBTUSDT".to_string(),
        ));
        let rx_penetration = tx_exchange.subscribe(); 
        tokio::spawn(penetration::engine(
            rx_penetration,
            tx_ws.clone(),
            book_state.clone(),
            1000,
            "XBTUSDT".to_string(),
        ));

        // //test receiving on tx_exchange
        // let mut rx_test = tx_exchange.subscribe();
        // tokio::spawn(async move {
        //     loop {
        //         match rx_test.recv().await {
        //             ok(update) => { 
        //                 println!("test received update: {:?}", update);
        //             }
        //             err(e) => {
        //                 eprintln!("test receiver error: {}", e);
        //                 break;
        //             }
        //         }
        //     }
        // });
        // Spawn connectors
        let mut bitmex = BitmexConnector::default();
        let tx_bitmex_updates = tx_exchange.clone();
        let rx_bitmex_cmd = tx_cmd.subscribe();
        tokio::spawn(async move {
            bitmex.run(tx_bitmex_updates, rx_bitmex_cmd).await;
        });

        // let mut bitvavo = BitvavoConnector;
        // let tx_bitvavo_updates = tx_exchange.clone();
        // let rx_bitvavo_cmd = tx_cmd.subscribe();
        // tokio::spawn(async move {
        //     bitvavo.run(tx_bitvavo_updates, rx_bitvavo_cmd).await;
        // });

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

// #[tokio::main]
// async fn main() {
//     let engine = Engine::init();

//     // Subscribe to channels
//     engine.send_cmd(ConnectorCommand::Subscribe { channel: ChannelType::Book, symbol: "XBTUSDT".to_string() });
//     engine.send_cmd(ConnectorCommand::Subscribe { channel: ChannelType::Trade, symbol: "XBTUSDT".to_string() });

//     tokio::signal::ctrl_c().await.unwrap();
// }