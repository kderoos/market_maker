pub mod types;
pub use types::{BitmexConvertable, BitmexTradeMsg, BitmexTradeTick, BitmexQuoteMsg, BitmexQuoteTick, BitmexOrderBookMsg, BitmexOrderBookDataRow};
use tokio_tungstenite::{WebSocketStream,MaybeTlsStream,connect_async};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message,Error};
use futures_util::{StreamExt, SinkExt, stream::{SplitSink,SplitStream}};
use reqwest;
use std::collections::{HashSet,HashMap};

use chrono::{DateTime};
use async_trait::async_trait;
use common::{OrderSide,InstrumentData, Connector, ConnectorCommand, Subscription, AnyUpdate, QuoteUpdate, TradeUpdate, BookUpdate, BookEntry};
// use tokio::sync::mpsc::Sender;
use tokio::sync::broadcast::{Sender,Receiver};


pub struct BitmexConnector{
    pub ws_url: String,
    subscriptions: HashSet<Subscription>,
    instruments: HashMap<String, InstrumentData>,
}
impl Default for BitmexConnector{
    fn default() -> Self {
        Self{
            ws_url: "wss://www.bitmex.com/realtime".to_string(),
            subscriptions: HashSet::new(),
            instruments: HashMap::new(),
        }
    }
}

fn parse_ts_bitmex(ts: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.timestamp_micros())
}
async fn fetch_instrument_data(symbol: &str) -> InstrumentData {
   let url = format!("https://www.bitmex.com/api/v1/instrument?symbol={}&count=1&reverse=false",symbol);
   let resp = reqwest::get(&url)
            .await
            .unwrap();

   let json: serde_json::Value = resp.json().await.unwrap();
   let instrument = &json[0];
   InstrumentData {
       symbol: symbol.to_string(),
       tick_size: instrument["tickSize"].as_f64().unwrap(),
       lot_size: instrument["lotSize"].as_u64().unwrap(),
       maker_fee: instrument["makerFee"].as_f64().unwrap(),
       taker_fee: instrument["takerFee"].as_f64().unwrap(),
   }
}

#[async_trait]
impl Connector for BitmexConnector {
    fn name(&self) -> &'static str {
        "bitmex"
    }

    async fn run(&mut self, tx: Sender<AnyUpdate>, mut rx: Receiver<ConnectorCommand>) {
        println!("BitMex connector starting...");

        // let url = "wss://ws.bitmex.com/realtime";
        let (ws_stream, _) = connect_async(&self.ws_url).await.expect("Failed to connect");
        println!("Connected to BitMex WS");

        let (mut write, mut read) = ws_stream.split();
        
        loop {
            tokio::select! {
                Ok(cmd) = rx.recv() => {
                    self.handle_cmd(cmd, &mut write, &mut read).await;
                }
                Some(message) = read.next() => {
                    self.handle_message(message, &tx).await;
                }
            }
        }
    }
}
impl BitmexConnector {
    async fn handle_cmd(
        &mut self,
        cmd: ConnectorCommand, 
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        read: &mut SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ){
        match cmd {
            ConnectorCommand::Subscribe { symbol, channel } => {
                let sub = Subscription { symbol, channel };
                if self.subscriptions.insert(sub.clone()) {
                    let msg = format!(r#"{{"op":"subscribe","args":["{}:{}"]}}"#, sub.channel.to_bitmex(), sub.symbol);
                    write.send(Message::Text(msg)).await.unwrap();
                    self.confirm_action_timeout(read).await;
                }
            }
            ConnectorCommand::Unsubscribe { symbol, channel } => {
                let sub = Subscription { symbol, channel };
                if self.subscriptions.remove(&sub) {
                    let msg = format!(r#"{{"op":"unsubscribe","args":["{}:{}"]}}"#, sub.channel.to_bitmex(), sub.symbol);
                    write.send(Message::Text(msg)).await.unwrap();
                    println!("[Bitmex] Unsubscribed from {}:{}", sub.channel.to_bitmex(), sub.symbol);
                    self.confirm_action_timeout(read).await;
                }
            }
            ConnectorCommand::ResubscribeAll => {
                for sub in self.subscriptions.iter() {
                    let msg = format!(r#"{{"op":"subscribe","args":["{}:{}"]}}"#, sub.channel.to_bitmex(), sub.symbol);
                    write.send(Message::Text(msg)).await.unwrap();
                    println!("[Bitmex] Resubscribed to {}:{}", sub.channel.to_bitmex(), sub.symbol);
                    self.confirm_action_timeout(read).await;
                }
            }
            ConnectorCommand::Shutdown => {
                println!("[Bitmex] Shutdown command received. Closing connection.");
                let msg = r#"{"op":"unsubscribe","args":["*"]}"#;
                write.send(Message::Text(msg.into())).await.unwrap();
                write.close().await.unwrap();
            }

        }
    
        for sub in self.subscriptions.iter() {
            if !self.instruments.contains_key(&sub.symbol) {
                let instrument_data = fetch_instrument_data(&sub.symbol).await;
                self.instruments.insert(sub.symbol.clone(), instrument_data);
            }
        }
    }    

    async fn handle_message(
        &self,
        message: Result<Message,Error>,
        tx: &Sender<AnyUpdate>,
    ){
        match message {
            Ok(msg) => {
                if msg.is_text() {
                    let text = msg.into_text().unwrap();
                     println!("Raw WS message: {}", text);
                
                    // Parse trade messages
                    if text.contains("\"table\":\"trade\"") {
                        println!("Parsing trade message: {}", text);
                        match serde_json::from_str::<BitmexTradeMsg>(&text) {
                            Ok(msg) => {
                                if msg.table == "trade" && msg.action == "insert" {
                                    for ticker in msg.data {
                                        let tick_size = self.instruments.get(&ticker.market)
                                            .map(|inst| inst.tick_size)
                                            .unwrap();
                                        let side = ticker.side.parse::<OrderSide>().unwrap_or(OrderSide::Sell);
                                        let update = TradeUpdate {
                                            exchange: "bitmex".into(),
                                            tick_size: tick_size,
                                            base:  ticker.market[0..3].to_string(), // e.g., "XBT" from "XBTUSDT"
                                                                    //   .replace("XBT", "BTC"),
                                            quote:  ticker.market[3..].to_string(), // e.g., "USDT" from "XBTUSDT"
                                                                    //   .replace("USDT", "USD"),    
                                            side: side,
                                            price: ticker.price,
                                            size: ticker.size as i64,
                                            ts_exchange: parse_ts_bitmex(&ticker.ts_exchange),
                                            ts_received: chrono::Utc::now().timestamp_micros(),
                                        };
                                        // tx.send(AnyUpdate::TradeUpdate(update)).await.expect("Failed to send price update to engine");
                                        tx.send(AnyUpdate::TradeUpdate(update)).expect("Failed to send price update to engine");
                                    }
                                }
                            }
                            Err(e) => {  
                                eprintln!("Failed to parse BitmexTradeMsg: {}", e);
                            }
                        }
                    } else
                    if text.contains("\"table\":\"quote\"") {
                        match serde_json::from_str::<BitmexQuoteMsg>(&text) {
                            Ok(msg) => {
                                if msg.table == "quote" && msg.action == "insert" {
                                    for ticker in msg.data {
                                        let tick_size = self.instruments.get(&ticker.market)
                                            .map(|inst| inst.tick_size)
                                            .unwrap();
                                        let update = QuoteUpdate {
                                            exchange: "bitmex".into(),
                                            tick_size: tick_size,
                                            base:  ticker.market[0..3].to_string() // e.g., "XBT" from "XBTUSDT"
                                                                      .replace("XBT", "BTC"),
                                            quote:  ticker.market[3..].to_string() // e.g., "USDT" from "XBTUSDT"
                                                                      .replace("USDT", "USD"),    
                                            fee: "0.0005".to_string(), // Bitmex maker/taker fee
                                            best_bid: ticker.best_bid,
                                            best_ask: ticker.best_ask,
                                            ts_exchange: parse_ts_bitmex(&ticker.ts_exchange),
                                            ts_received: chrono::Utc::now().timestamp_micros(),
                                        };
                                        // tx.send(AnyUpdate::QuoteUpdate(update)).await.expect("Failed to send price update to engine");
                                        tx.send(AnyUpdate::QuoteUpdate(update)).expect("Failed to send price update to engine");
                                    }
                                }
                            }
                            Err(e) => {  
                                eprintln!("Failed to parse BitmexQuoteMsg: {}", e);
                            }
                        }
                    } else if text.contains("\"table\":\"orderBookL2\"") {
                        // println!("Received orderBookL2 message (not processed): {}", text);
                       match serde_json::from_str::<BitmexOrderBookMsg>(&text) {
                            Ok(msg) => {
                                if msg.table == "orderBookL2" {
                                    let symbol =  msg.data.first().unwrap().market.clone();
                                    let tick_size = self.instruments.get(&symbol)
                                        .map(|inst| inst.tick_size)
                                        .unwrap();
                                    let update = BookUpdate{
                                                exchange: "bitmex".into(),
                                                symbol: symbol,
                                                tick_size: tick_size,
                                                action: msg.action.clone(),
                                                data: msg.data.iter().map(|row| (
                                                    row.id.to_string(), // For Bitmex, use the unique ID as the key
                                                    BookEntry {
                                                        side: row.side.clone(),
                                                        size: row.size.unwrap_or(0),
                                                        price: row.price.unwrap(),
                                                    }
                                                )).collect(),
                                                ts_exchange: msg.data.first().and_then(|d| parse_ts_bitmex(&d.timestamp)), // Approximate with first entry's timestamp
                                                ts_received: chrono::Utc::now().timestamp_micros(),
                                        };
                                    // tx.send(AnyUpdate::BookUpdate(update)).await.expect("Failed to send order book update to engine");
                                    tx.send(AnyUpdate::BookUpdate(update)).expect("Failed to send order book update to engine");
                                }
                            }
                            Err(e) => {  
                                eprintln!("Failed to parse BitmexOrderBookMsg: {}", e);
                            }
                        } 
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                // break;
            }
        }
    }
    async fn confirm_action_timeout(
        &self,
        read: &mut SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ){
        // Wait for confirmation message or timeout
        use tokio::time::{timeout, Duration};
        let duration = Duration::from_secs(5);
        let mut confirmed = false;
        while !confirmed {
            match timeout(duration, read.next()).await {
                Ok(Some(Ok(msg))) => {
                    if msg.is_text() {
                        let text = msg.into_text().unwrap();
                        if text.contains("\"success\"") {
                            println!("[Bitmex] Action confirmed: {}", text);
                            confirmed = true;
                        }
                        // if text.contains("\"success\":true") && text.contains(&format!("\"subscribe\":\"{}:{}\"", sub.channel.to_bitmex(), sub.symbol)) {
                            // println!("[Bitmex] Subscription to {}:{} confirmed.", sub.channel.to_bitmex(), sub.symbol);
                            // confirmed = true;
                        // }
                    }
                }
                Ok(Some(Err(e))) => {
                    eprintln!("Error receiving confirmation: {}", e);
                }
                Ok(None) => {
                    eprintln!("Connection closed before confirmation");
                }
                Err(_) => {
                    eprintln!("Timeout waiting for subscription confirmation");
                    break;
                }
            }
        }
    }
}


