use serde::{Serialize, Deserialize};
use std::collections::HashMap;

pub type OrderBook = HashMap<String, BookEntry>; // symbol -> list of entries

#[derive(Serialize,Deserialize,Debug, PartialEq, Eq, Hash, Clone)]
pub enum ChannelType {
    Trade,
    Quote,
    Book,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookUpdate {
    pub exchange: String,
    pub symbol: String,
    pub tick_size: f64,
    pub action: String,
    pub data: Vec<(String, BookEntry)>, // (id, entry)
    pub ts_exchange: Option<i64>,
    pub ts_received: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookEntry {
    //pub id: i64,
    pub side: String,
    pub size: i64,
    pub price: f64,
}

#[derive(Debug, Clone,Serialize, Deserialize)]
pub struct QuoteUpdate {
    pub exchange: String,
    pub base: String,
    pub quote: String,
    pub tick_size: f64,
    pub fee: String,
    pub best_ask: Option<f64>, 
    pub best_bid: Option<f64>,
    pub ts_exchange: Option<i64>,
    pub ts_received: i64,
}
impl QuoteUpdate {
    pub fn new(
        exchange: String,
        base: String,
        quote: String,
        tick_size: f64,
        fee: String,
        best_ask: Option<f64>,
        best_bid: Option<f64>,
        ts_exchange: Option<i64>,
        ts_received: i64,
    ) -> Option<Self> {
        if best_ask.is_none() && best_bid.is_none() {
            None
        } else {
            Some(Self {
                exchange,
                base,
                quote,
                tick_size,
                fee,
                best_ask,
                best_bid,
                ts_exchange,
                ts_received,
            })
        }
    }
}