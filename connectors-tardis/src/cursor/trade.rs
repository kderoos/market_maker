use std::fs::File;
use flate2::read::GzDecoder;
use csv::Reader;

use crate::cursor::event::{DomainEvent, DomainPayload, EventClass};
use crate::cursor::merge::EventCursor;

pub struct TradeCursor {
    reader: Reader<GzDecoder<File>>,
    next: Option<DomainEvent>,
    seq: u64,
}
impl TradeCursor {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let decoder = GzDecoder::new(file);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(decoder);

        let mut cursor = Self {
            reader,
            next: None,
            seq: 0,
        };

        cursor.read_next()?; // preload first event
        Ok(cursor)
    }

    fn read_next(&mut self) -> anyhow::Result<()> {
        let mut record = csv::StringRecord::new();

        if !self.reader.read_record(&mut record)? {
            self.next = None;
            return Ok(());
        }

        let local_ts: i64 = record
            .get(LOCAL_TS_IDX)
            .unwrap()
            .parse()?;

        let exchange_ts: i64 = record
            .get(EXCHANGE_TS_IDX)
            .unwrap()
            .parse()?;

        let price: f64 = record.get(PRICE_IDX).unwrap().parse()?;
        let qty: f64 = record.get(QTY_IDX).unwrap().parse()?;
        let side = record.get(SIDE_IDX).unwrap();

        let trade = Trade {
            price,
            qty,
            side: parse_side(side),
        };

        self.seq += 1;

        self.next = Some(DomainEvent {
            local_ts,
            exchange_ts,
            class: EventClass::Trade,
            seq: self.seq,
            payload: DomainPayload::Trade(trade),
        });

        Ok(())
    }
}

