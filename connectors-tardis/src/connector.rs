use crate::cursor::merge::{MergeCursor,EventCursor};
// use crate::normalize::normalize;
use async_trait::async_trait;
use common::{AnyUpdate, Connector, ConnectorCommand, TardisPaths};
use tokio::sync::broadcast::{Receiver, Sender};
use crate::cursor::{generic::CsvCursor,trade::TradeCursor,quote::QuoteCursor,book::BookCursor};
use crate::error::TardisError;
use tracing;


pub struct TardisConnector {
    cursor: MergeCursor,
}
impl TardisConnector {
    pub fn new(paths: TardisPaths) -> Result<Self, TardisError> {
        let TardisPaths { trades, quotes, book } = paths;

        let mut cursors: Vec<Box<dyn EventCursor>> = Vec::new();
        //trade cursor is mandatory
        cursors.push(Box::new(TradeCursor::new(trades)?));
        //optional cursors
        if let Some(quotes_path) = quotes {
            cursors.push(Box::new(QuoteCursor::new(quotes_path)?));
        }
        if let Some(book_path) = book {
            cursors.push(Box::new(BookCursor::new(book_path)?));
        }

        Ok(Self {
            cursor: MergeCursor::new(cursors),
        })
    }
}
#[async_trait]
impl Connector for TardisConnector {
    fn name(&self) -> &'static str {
        "tardis"
    }

    async fn run(
        &mut self,
        tx: Sender<AnyUpdate>,
        mut rx: Receiver<ConnectorCommand>,
    ) {
        loop {
            // handle control commands first
            match rx.try_recv() {
                    Ok(ConnectorCommand::ReplayStop) => break,
                    Ok(_) => {},
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break, 
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => todo!(),
                }

            let event = match self.cursor.next() {
                Ok(Some(e)) => e,  // event
                Ok(None) => break, // EOF
                Err(e) => {
                    tracing::error!("TardisConnector cursor error: {:#?}", e);
                    break;
                }
            };

            // emit update
            if tx.send(event.payload).is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use flate2::{Compression, write::GzEncoder};
    use tempfile::tempdir;

    use common::{AnyUpdate, ConnectorCommand, TardisPaths};
    use tokio::sync::broadcast;

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

    #[tokio::test]
    async fn tardis_connector_replays_events_in_order() {
        let book_csv = r#"
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance,BTCUSDT,1000,1001,true,bid,100.0,1
binance,BTCUSDT,1000,1003,true,ask,101.0,2
"#;

        let trade_csv = r#"
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
binance,BTCUSDT,1000,1002,silly-id,buy,100.5,1
"#;

        let quote_csv = r#"
exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount
binance,BTCUSDT,1000,1000,99.5,1,100.5,1
"#;

        let paths = TardisPaths {
            trades: write_gz_csv("trades.csv.gz", trade_csv),
            book: Some(write_gz_csv("book.csv.gz", book_csv)),
            quotes: Some(write_gz_csv("quotes.csv.gz", quote_csv)),
        };

        let mut connector = TardisConnector::new(paths).unwrap();

        let (tx_updates, mut rx_updates) = broadcast::channel(16);
        let (_tx_cmd, rx_cmd) = broadcast::channel(4);

        connector.run(tx_updates, rx_cmd).await;

        let mut updates = Vec::new();
        while let Ok(u) = rx_updates.try_recv() {
            updates.push(u);
        }

        // ---- we expect 4 updates total (quote, book, trade, book)
        assert_eq!(updates.len(), 4);

        // ---- ordering by local_timestamp
        match &updates[0] {
            AnyUpdate::QuoteUpdate(_) => {}
            _ => panic!("expected Quote first"),
        }

        match &updates[1] {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Partial"),
            _ => panic!("expected Book Partial"),
        }

        match &updates[2] {
            AnyUpdate::TradeUpdate(_) => {}
            _ => panic!("expected Trade"),
        }

        match &updates[3] {
            AnyUpdate::BookUpdate(u) => assert_eq!(u.action, "Update"),
            _ => panic!("expected Book Update"),
        }
    }
    #[tokio::test]
    async fn tardis_connector_handles_missing_optional_files() {
        let trade_csv = r#"
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
binance,BTCUSDT,1000,1000,silly-id,buy,100.5,1
"#;
        let paths = TardisPaths {
            trades: write_gz_csv("trades.csv.gz", trade_csv),
            book: None,
            quotes: None,
        };

        let mut connector = TardisConnector::new(paths).unwrap();

        let (tx_updates, mut rx_updates) = broadcast::channel(16);
        let (_tx_cmd, rx_cmd) = broadcast::channel(4);

        connector.run(tx_updates, rx_cmd).await;

        let mut updates = Vec::new();
        while let Ok(u) = rx_updates.try_recv() {
            updates.push(u);
        }

        // ---- we expect only 1 update (the trade)
        assert_eq!(updates.len(), 1);

        match &updates[0] {
            AnyUpdate::TradeUpdate(_) => {}
            _ => panic!("expected Trade"),
        }
    }
    async fn tardis_connector_error_on_path_not_found() {
        let paths = TardisPaths {
            trades: std::path::PathBuf::from("non_existent_trades.csv.gz"),
            book: None,
            quotes: None,
        };

        let result = TardisConnector::new(paths);
        assert!(result.is_err(), "expected error on missing trades file");
    }
    use crate::cursor::book::BookRow;
    use crate::error::TardisError;
    #[test]
    fn csv_parse_error_on_open_reports_file_and_row() {
        let csv = r#"
    exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
    binance,BTCUSDT,1000,1001,true,bid,100.0,NOT_A_NUMBER
    "#;

        let path = write_gz_csv("bad.csv.gz", csv);

        let err = match CsvCursor::<BookRow>::open(&path) {
            Err(TardisError::CsvParse { path: p, row, .. }) => {
                assert!(p.ends_with("bad.csv.gz"));
                assert_eq!(row, 0); // seq before increment
            }
            _ => panic!("unexpected error type"),
        };
    }

}


