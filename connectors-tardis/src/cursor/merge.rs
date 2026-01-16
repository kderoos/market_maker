use crate::cursor::event::DomainEvent;
use crate::error::TardisError;

pub struct MergeCursor {
    cursors: Vec<Box<dyn EventCursor>>,
}
// Decides what DomainEvent is next.
impl MergeCursor {
    pub fn new(cursors: Vec<Box<dyn EventCursor>>) -> Self {
        Self { cursors }
    }
    /// Return Ok(Some(event)) for the next event, Ok(None) on EOF, Err on cursor errors
    pub fn next(&mut self) -> Result<Option<DomainEvent>, TardisError> {
        let mut best = None;

        for (i, c) in self.cursors.iter().enumerate() {
            if let Some(e) = c.peek() {
                // Set key. Use local_ts as timestamp for order
                let key = (e.local_ts, e.class as u8, e.seq);
                // set best or compare key to current best.
                if best.map_or(true, |(_, k)| key < k) {
                    best = Some((i, key));
                }
            }
        }

        let (idx, _) = match best {
            Some(v) =>v,
            None => return Ok(None),
        };

        let event = self.cursors[idx].peek().cloned();
        //propagate possible error
        self.cursors[idx].advance()?;
        Ok(event)
    }
}

pub trait EventCursor: Send + Sync {
    // peek inspects the next DomainEvent for cursor
    // used to decide what cursor to advance next.
    fn peek(&self) -> Option<&DomainEvent>;
    // advance moves the cursor to the next event.
    fn advance(&mut self) -> Result<(),TardisError>;
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use flate2::{Compression, write::GzEncoder};
    use tempfile::tempdir;

    use crate::cursor::book::BookCursor;
    use crate::cursor::trade::TradeCursor;
    use common::AnyUpdate;

    fn write_gz_csv(name: &str, contents: &str) -> std::path::PathBuf {
        let dir = tempdir().unwrap();
        let path = dir.path().join(name);

        let file = File::create(&path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(contents.as_bytes()).unwrap();
        encoder.finish().unwrap();

        std::mem::forget(dir);
        path
    }

    #[test]
    fn merge_cursor_orders_book_and_trade_correctly() {
        let book_csv = r#"
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance,BTCUSDT,1000,1001,true,bid,100.0,1
binance,BTCUSDT,1000,1003,true,ask,101.0,2
"#;

        let trade_csv = r#"
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
binance,BTCUSDT,1000,1002,00000000-006d-1000-0000-0009e6e65676,buy,100.5,1
"#;

        let book_path = write_gz_csv("book.csv.gz", book_csv);
        let trade_path = write_gz_csv("trade.csv.gz", trade_csv);

        let book = BookCursor::new(book_path).unwrap();
        let trade = TradeCursor::new(trade_path).unwrap();

        let mut merge = MergeCursor::new(vec![
            Box::new(book),
            Box::new(trade),
        ]);



        // ---- event 1: book @ 1001 → Partial
        let e1 = merge.next().expect("cursor error").expect("expected event");
        assert_eq!(e1.local_ts, 1001);
        match e1.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "partial"),
            _ => panic!("expected BookUpdate"),
        }

        // ---- event 2: trade @ 1002
        let e2 = merge.next().expect("cursor error").expect("expected event");
        assert_eq!(e2.local_ts, 1002);
        match e2.payload {
            AnyUpdate::TradeUpdate(_) => {}
            _ => panic!("expected Trade"),
        }

        // ---- event 3: book @ 1003 → Update
        let e3 = merge.next().expect("cursor error").expect("expected event");
        assert_eq!(e3.local_ts, 1003);
        match e3.payload {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "update"),
            _ => panic!("expected BookUpdate"),
        }

        // ---- end
        assert!(merge.next().expect("cursor error").is_none());
    }
}

