use serde::Deserialize;
use anyhow::Result;
use common::{AnyUpdate,TradeUpdate,OrderSide};

use crate::cursor::generic::{CsvCursor, DomainEventRow};
use crate::cursor::event::{DomainEvent, EventClass};

//Corresponds to one row in the Tardis trade CSV 
#[derive(Debug, Deserialize)]
pub struct TradeRow {
    exchange: String,
    symbol: String,
    timestamp: String,
    local_timestamp: String,
    id: String,
    side: String,
    price: f64,
    amount: String,
}

// convert parsed row into DomainEvent
impl DomainEventRow for TradeRow {
    fn into_domain_event(self, seq: u64) -> DomainEvent {
        let order_side: OrderSide = self.side.parse()
            .expect(&format!("invalid order side: {}", self.side));
        
        let ts_ex = Some(self.timestamp.parse::<i64>().expect("invalid exchange timestamp"));
        let ts_local = self.local_timestamp.parse::<i64>().expect("invalid local timestamp");

        let payload = AnyUpdate::TradeUpdate(TradeUpdate{
                exchange: self.exchange,
                tick_size: 0.01, //TODO: get from config when implemented
                base: "XBT".to_string(), // TODO: AnyUpdate should have symbol field
                quote: "USDT".to_string(), 
                // symbol: self.symbol,
                side: order_side,
                price: self.price,
                size: self.amount.parse::<i64>().unwrap(), //TODO: quantize using lot_size
                ts_exchange: ts_ex,
                ts_received: ts_local
        });

        DomainEvent {
            local_ts: ts_local,
            exchange_ts: ts_ex,
            class: EventClass::Trade,
            seq,
            payload,
        }
    }
}
pub type TradeCursor = CsvCursor<TradeRow>;
