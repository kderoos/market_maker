use serde::Deserialize;
use common::ChannelType;

pub trait BitmexConvertable {
    fn to_bitmex(&self) -> &'static str;
}

impl BitmexConvertable for ChannelType {
    fn to_bitmex(&self) -> &'static str {
        match self {
            ChannelType::Book  => "orderBookL2",
            ChannelType::Quote => "quote",
            ChannelType::Trade => "trade",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BitmexTradeMsg {
    pub table: String,
    pub action: String,
    pub data: Vec<BitmexTradeTick>,
}

#[derive(Debug, Deserialize)]
pub struct BitmexTradeTick {
    #[serde(rename = "timestamp")]
    pub ts_exchange: String,
    #[serde(rename = "symbol")]
    pub market: String,
    #[serde(rename = "price")]
    pub price: f64,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "size")]
    pub size: f64,
}

#[derive(Debug, Deserialize)]
pub struct BitmexQuoteMsg {
    pub table: String,
    pub action: String,
    pub data: Vec<BitmexQuoteTick>,
}

#[derive(Debug, Deserialize)]
pub struct BitmexQuoteTick {
    #[serde(rename = "timestamp")]
    pub ts_exchange: String,
    #[serde(rename = "symbol")]
    pub market: String,
    #[serde(rename = "askPrice")]
    pub best_ask: Option<f64>,
    #[serde(rename = "bidPrice")]
    pub best_bid: Option<f64>
}

#[derive(Debug, Deserialize)]
pub struct BitmexOrderBookMsg {
    pub table: String, // "orderBookL2"
    pub action: String,//  "insert", "update", "partial", "delete"
    pub data: Vec<BitmexOrderBookDataRow>,
}
#[derive(Debug, Deserialize)]
pub struct BitmexOrderBookDataRow {
    #[serde(rename = "symbol")]
    pub market: String,
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "size")]
    pub size: Option<i64>,
    #[serde(rename = "price")]
    pub price: Option<f64>,
    pub timestamp: String,
}