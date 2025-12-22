pub mod types;
pub use types::{ChannelType, QuoteUpdate, BookUpdate, BookEntry};

use serde::{Serialize, Deserialize};
use async_trait::async_trait;
// use tokio::sync::mpsc::Sender;
use tokio::sync::broadcast::{Sender, Receiver};

#[derive(Debug)]
pub struct InstrumentData {
    pub symbol: String,
    pub tick_size: f64,
    pub lot_size: u64,
    pub maker_fee: f64,
    pub taker_fee: f64,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExchangeConnectorCommand {
    pub exchange: String,
    pub cmd: ConnectorCommand,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Order {
    Market{ symbol: String, side: OrderSide, size: i64 },
    Limit{ symbol: String, side: OrderSide, price: f64, size: i64, client_id: Option<u64> },
    Cancel{ order_id: i64 },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub action: String,
    pub symbol: String,
    pub order_id: i64,
    pub side: OrderSide,
    pub price: f64,
    pub size: i64,
    pub ts_exchange: Option<i64>,
    pub ts_received: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeUpdate {
    pub exchange: String,
    pub tick_size: f64,
    pub base: String,
    pub quote: String,
    pub side: OrderSide,
    pub price: f64,
    pub size: i64,
    pub ts_exchange: Option<i64>,
    pub ts_received: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")] // serializes as "Buy"/"Sell"
pub enum OrderSide {
    Buy,
    Sell,
}
impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { OrderSide::Buy => write!(f, "Buy"), OrderSide::Sell => write!(f, "Sell") }
    }
}

impl std::str::FromStr for OrderSide {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Buy" | "buy" => Ok(OrderSide::Buy),
            "Sell" | "sell" => Ok(OrderSide::Sell),
            _ => Err(()),
        }
    }
}
#[derive(Debug, Clone)]
pub enum AnyUpdate {
    QuoteUpdate(QuoteUpdate),
    TradeUpdate(TradeUpdate),
    BookUpdate(BookUpdate),
}
#[derive(Debug,Clone, Serialize)]
pub struct DepthSnapshot{
    pub timestamp: i64,
    pub len: u32,
    pub bid: Vec<(f64, u64)>,
    pub ask: Vec<(f64, u64)>,
}
#[derive(Clone, Debug, Serialize)]
pub struct PenetrationUpdate{
    pub timestamp: u64,
    pub symbol: String,
    pub counts: Vec<u64>,
    pub fit_A: Option<f64>,
    pub fit_k: Option<f64>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Candle {
    pub ts_start: i64,   // epoch ms for the start of candle bucket
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
#[derive(Clone, Debug, Serialize)]
pub struct MidPriceUpdate {
    pub timestamp: i64,
    pub ts_exchange: i64,
    pub symbol: String,
    pub mid_price: f64,
}
#[derive(Clone, Debug, Serialize)]
pub struct VolatilityUpdate {
    pub symbol: String,
    pub sigma: f64,
    pub timestamp: i64,
}
#[derive(Debug,Clone, Serialize)]
pub enum AnyWsUpdate {
    Depth(DepthSnapshot),
    Trade(TradeUpdate),
    Quote(QuoteUpdate),
    Penetration(PenetrationUpdate),
    TradeCandle(Candle),
    MidPrice(MidPriceUpdate),
    Volatility(VolatilityUpdate),
}
//Traits required by HashSet
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct Subscription {
    pub channel: ChannelType,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectorCommand {
    Subscribe{ channel: ChannelType, symbol: String },
    Unsubscribe{ channel: ChannelType, symbol: String },
    ResubscribeAll,
    Shutdown,
}
#[async_trait]
pub trait Connector: Send + Sync {
    fn name(&self) -> &'static str;
    // fn supported_symbols(&self) -> Vec<String>;
    async fn run(&mut self, tx: Sender<AnyUpdate>, mut rx: Receiver<ConnectorCommand>);
}

