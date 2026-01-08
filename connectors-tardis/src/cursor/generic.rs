use std::fs::File;
use flate2::read::GzDecoder;
use csv;
use serde::de::DeserializeOwned;
use anyhow::Result;

use crate::cursor::event::DomainEvent;

/// Row -> DomainEvent converter each row type implements
pub trait DomainEventRow {
    fn into_domain_event(self, seq: u64) -> DomainEvent;
}

/// Generic CSV cursor for gzipped CSV files where each row type T implements Deserialize + DomainEventRow
pub struct CsvCursor<T> {
    iter: csv::DeserializeRecordsIntoIter<GzDecoder<File>, T>,
    next: Option<DomainEvent>,
    seq: u64,
}

impl<T> CsvCursor<T>
where
    T: DeserializeOwned + DomainEventRow,
{
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let decoder = GzDecoder::new(file);
        let iter = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(decoder)
            .into_deserialize::<T>(); // consumes reader, owns decoder/file

        let mut cursor = Self {
            iter,
            next: None,
            seq: 0,
        };
        cursor.read_next()?; // preload first event
        Ok(cursor)
    }

    fn read_next(&mut self) -> Result<()> {
        if let Some(record) = self.iter.next() {
            let row: T = record?; // deserialize or return error
            self.seq = self.seq.saturating_add(1);
            self.next = Some(row.into_domain_event(self.seq));
        } else {
            self.next = None;
        }
        Ok(())
    }

    /// take and return the current preloaded DomainEvent (if any) and advance
    pub fn take_next(&mut self) -> Option<DomainEvent> {
        let out = self.next.take();
        if out.is_some() {
            // best-effort: ignore read_next error here or propagate as you prefer
            let _ = self.read_next();
        }
        out
    }
}
