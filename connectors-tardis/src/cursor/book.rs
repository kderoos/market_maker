use serde::Deserialize;
use crate::error::TardisError;
use common::{AnyUpdate,BookUpdate,BookEntry, OrderSide};
use std::path::Path;

use crate::cursor::generic::{CsvCursor, DomainEventRow};
use crate::cursor::event::{DomainEvent, EventClass};
use crate::cursor::merge::EventCursor;

//Corresponds to one row in the Tardis book CSV 
#[derive(Debug, Deserialize)]
pub struct BookRow {
    exchange: String,
    symbol: String,
    timestamp: String,
    local_timestamp: String,
    is_snapshot: bool,
    side: String,
    price: f64,
    amount: String,
   }

// convert parsed row into DomainEvent
impl DomainEventRow for BookRow {
    fn into_domain_event(self, seq: u64) -> DomainEvent {
        // All row parsing validation done here
        // One DomainEvent per row
        let ts_ex = Some(self.timestamp.parse::<i64>().expect("invalid exchange timestamp"));
        let ts_local = self.local_timestamp.parse::<i64>().expect("invalid local timestamp");

        let payload = AnyUpdate::BookUpdate(BookUpdate{
            exchange: self.exchange,
            symbol: self.symbol,
            tick_size: 0.01, //TODO: get from config when implemented
            // Emit partial on snapshots, update otherwise
            action: if self.is_snapshot { "Partial".to_string() } else { "Update".to_string() }, 
            data: vec![(self.price.to_string(), BookEntry{
                side: self.side,
                size: self.amount.parse::<i64>().unwrap(), //TODO: quantize using lot_size
                price: self.price,
            })],
            ts_exchange: ts_ex,
            ts_received: ts_local,
        }); 

        DomainEvent {
            local_ts: ts_local,
            exchange_ts: ts_ex,
            class: EventClass::Book,
            seq,
            payload,
        }
    }
}

pub struct BookCursor {
    // BookCursor wraps CsvCursor<BookRow> to handle snapshot logic
    // i.e. marking first snapshot row as Partial to clear orderbook. 
    // other rows in snapshot remain Update.
    inner: CsvCursor<BookRow>,
    last_was_partial: bool,
    next_event: Option<DomainEvent>,
}
impl BookCursor {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, TardisError> {
        let mut inner = CsvCursor::open(path)?;
        let mut cursor = Self {
            last_was_partial: false,
            next_event: None,
            inner,
        };
        cursor.preload_next()?;
        Ok(cursor)
    }
    fn preload_next(&mut self) -> Result<(),TardisError> {
        // take next event from inner cursor
        match self.inner.take_next()? {
            Some(mut row_event) => {
                let row_is_partial = match &row_event.payload {
                    AnyUpdate::BookUpdate(u) => u.action == "Partial",
                    _ => false,
                };
                // check for snapshot event and modify action
                if row_is_partial && !self.last_was_partial {
                    // mark first snapshot row as Partial
                    if let AnyUpdate::BookUpdate(ref mut u) = row_event.payload {
                        u.action = "Partial".to_string();
                    }
                } else if row_is_partial && self.last_was_partial {
                    // Partial already sent. Mark action "Update"
                    // to avoid clearing book again
                    if let AnyUpdate::BookUpdate(ref mut u) = row_event.payload {
                        u.action = "Update".to_string();
                    }
                }
                self.last_was_partial = row_is_partial;
                self.next_event = Some(row_event);
            }
            None => {
                self.next_event = None;
            }
        }
        Ok(())
    }
}
impl EventCursor for BookCursor {
    fn peek(&self) -> Option<&DomainEvent> {
        self.next_event.as_ref()
    }
    fn advance(&mut self) -> Result<(), TardisError> {
        // move to next row and apply snapshot logic preemptively
        self.preload_next()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use flate2::{Compression, write::GzEncoder};
    use tempfile::tempdir;

    fn write_gz_csv(contents: &str) -> std::path::PathBuf {
        let dir = tempdir().unwrap();
        let path = dir.path().join("book.csv.gz");

        let file = File::create(&path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(contents.as_bytes()).unwrap();
        encoder.finish().unwrap();

        // keep tempdir alive by leaking it (test-only)
        std::mem::forget(dir);
        path
    }

    #[test]
    fn book_cursor_emits_partial_once_per_snapshot() {
        let csv = r#"
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance,BTCUSDT,1000,1001,true,bid,100.0,1
binance,BTCUSDT,1000,1002,true,ask,101.0,2
binance,BTCUSDT,1001,1003,false,bid,100.5,3
binance,BTCUSDT,1000,1002,true,ask,101.0,2
"#;

        let path = write_gz_csv(csv);

        let mut cursor = BookCursor::new(path).unwrap();

        // ---- first row (snapshot → Partial)
        let e1 = cursor.peek().unwrap();
        match &e1.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Partial"),
            _ => panic!("expected BookUpdate"),
        }
        cursor.advance();

        // ---- second row (still snapshot → Update)
        let e2 = cursor.peek().unwrap();
        match &e2.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Update"),
            _ => panic!("expected BookUpdate"),
        }
        cursor.advance();

        // ---- third row (non-snapshot → Update)
        let e3 = cursor.peek().unwrap();
        match &e3.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Update"),
            _ => panic!("expected BookUpdate"),
        }
        cursor.advance();

        // ---- last row (snapshot → Partial)
        let e4 = cursor.peek().unwrap();
        match &e4.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Partial"),
            _ => panic!("expected BookUpdate"),
        }
        cursor.advance();
        // ---- end of stream
        assert!(cursor.peek().is_none());
    }
}
