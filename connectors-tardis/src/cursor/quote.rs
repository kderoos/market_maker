use serde::Deserialize;
use anyhow::Result;
use common::{AnyUpdate,QuoteUpdate,OrderSide};

use crate::cursor::generic::{CsvCursor, DomainEventRow};
use crate::cursor::event::{DomainEvent, EventClass};

//Corresponds to one row in the Tardis quote CSV 
#[derive(Debug, Deserialize)]
pub struct QuoteRow {
    exchange: String,
    symbol: String,
    timestamp: String,
    local_timestamp: String,
    bid_price: f64,
    bid_amount: String,
    ask_price: f64,
    ask_amount: String,
}

// convert parsed row into DomainEvent
impl DomainEventRow for QuoteRow {
    fn into_domain_event(self, seq: u64) -> DomainEvent {
        let ts_ex = Some(self.timestamp.parse::<i64>().expect("invalid exchange timestamp"));
        let ts_local = self.local_timestamp.parse::<i64>().expect("invalid local timestamp");

        let payload = AnyUpdate::QuoteUpdate(QuoteUpdate{
            exchange: self.exchange,
            base: "XBT".to_string(), // TODO: extract from symbol
            quote: "USD".to_string(), // TODO: extract from symbol
            tick_size: 0.01, // TODO: get from config when implemented
            fee: "0.0005".to_string(), // TODO: get from config when implemented
            best_ask: Some(self.ask_price),
            best_bid: Some(self.bid_price),
            ts_exchange: ts_ex,
            ts_received: ts_local,
        });

        DomainEvent {
            local_ts: ts_local,
            exchange_ts: ts_ex,
            class: EventClass::Quote,
            seq,
            payload,
        }
    }
}
pub type QuoteCursor = CsvCursor<QuoteRow>;