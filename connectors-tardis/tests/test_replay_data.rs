use std::path::Path;

use connectors_tardis::cursor::{trade::TradeCursor,merge::{MergeCursor}, event::DomainEvent,book::BookCursor, quote::QuoteCursor};
use common::AnyUpdate;

#[test]
fn replay_tardis_trades_csv_gz() {
    let path = Path::new("./tests/bitmex_trades_sample.csv.gz");
    assert!(path.exists(), "trades.csv.gz not found");

    // create cursor
    let trade_cursor = TradeCursor::open(path).expect("failed to open TradeCursor");

    // wire into merge cursor
    let mut merge = MergeCursor::new(vec![Box::new(trade_cursor)]);

    let mut last_ts = None;
    let mut count = 0;

    while let Ok(Some(DomainEvent { local_ts, payload, .. })) = merge.next() {
        // monotonic local timestamp
        if let Some(prev) = last_ts {
            assert!(
                local_ts >= prev,
                "local_ts went backwards: {} < {}",
                local_ts,
                prev
            );
        }
        last_ts = Some(local_ts);

        // check update
        match payload {
            AnyUpdate::TradeUpdate(t) => {
                assert!(t.price > 0.0);
                assert!(t.size > 0);
            }
            other => panic!("unexpected update: {:?}", other),
        }

        count += 1;

        // safety guard for very large files
        if count > 1_000_000 {
            break;
        }
    }

    assert!(count > 0, "no trades emitted");
}
#[test]
fn replay_tardis_quotes_csv_gz() {
    let path = Path::new("./tests/bitmex_quotes_sample.csv.gz");
    assert!(path.exists(), "quotes.csv.gz not found");

    // create cursor
    let quote_cursor = QuoteCursor::open(path).expect("failed to open QuoteCursor");

    // wire into merge cursor
    let mut merge = MergeCursor::new(vec![Box::new(quote_cursor)]);

    let mut last_ts = None;
    let mut count = 0;

    while let Ok(Some(DomainEvent { local_ts, payload, .. })) = merge.next() {
        // monotonic local timestamp
        if let Some(prev) = last_ts {
            assert!(
                local_ts >= prev,
                "local_ts went backwards: {} < {}",
                local_ts,
                prev
            );
        }
        last_ts = Some(local_ts);

        // check update
        match payload {
            AnyUpdate::QuoteUpdate(q) => {
                assert!(q.best_bid >= Some(0.0));
                assert!(q.best_ask >= Some(0.0));
            }
            other => panic!("unexpected update: {:?}", other),
        }

        count += 1;

        // safety guard for very large files
        if count > 1_000_000 {
            break;
        }
    }

    assert!(count > 0, "no quotes emitted");
}
#[test]
fn replay_book() {
    let path = Path::new("./tests/bitmex_book_sample.csv.gz");
    assert!(path.exists(), "book.csv.gz not found");

     // create cursor
     let book_cursor = BookCursor::new(path).expect("failed to open BookCursor");

     // wire into merge cursor
     let mut merge = MergeCursor::new(vec![Box::new(book_cursor)]);

     let mut last_ts = None;
     let mut count = 0;

     while let Ok(Some(DomainEvent { local_ts, payload, .. })) = merge.next() {
         // monotonic local timestamp
         if let Some(prev) = last_ts {
             assert!(
                 local_ts >= prev,
                 "local_ts went backwards: {} < {}",
                 local_ts,
                 prev
             );
         }
         last_ts = Some(local_ts);

         // check update
         match payload {
             AnyUpdate::BookUpdate(b) => {
                 assert!(b.data.len() > 0 );
             }
             other => panic!("unexpected update: {:?}", other),
         }

         count += 1;

         // safety guard for very large files
         if count > 1_000_000 {
             break;
         }
     }

     assert!(count > 0, "no book updates emitted");
}