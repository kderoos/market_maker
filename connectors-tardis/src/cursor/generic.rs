use std::fs::File;
use std::path::{Path,PathBuf};
use flate2::read::GzDecoder;
use csv;
use serde::de::DeserializeOwned;
use anyhow::Result;
use crate::error::TardisError;
use crate::cursor::event::DomainEvent;
use crate::cursor::merge::EventCursor;


/// Row -> DomainEvent converter each row type implements
pub trait DomainEventRow {
    fn into_domain_event(self, seq: u64) -> DomainEvent;
}

/// Generic CSV cursor for gzipped Tardis CSV files 
pub struct CsvCursor<T> {
    iter: csv::DeserializeRecordsIntoIter<GzDecoder<File>, T>,
    next: Option<DomainEvent>,
    seq: u64,
    path: PathBuf,
}

impl<T> CsvCursor<T>
where
    T: DeserializeOwned + DomainEventRow ,
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, TardisError> {
        Self::open(path)
    }
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, TardisError> {
        let path_buf = path.as_ref().to_path_buf();
    
        let file = File::open(&path_buf)
            .map_err(|e| TardisError::OpenFile {
                path: path_buf.clone(),
                source: e,
            })?;

        let decoder = GzDecoder::new(file);
        let iter = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(decoder)
            .into_deserialize::<T>(); // consumes reader, owns decoder/file

        let mut cursor = Self {
            iter,
            next: None,
            seq: 0,
            path: path_buf,
        };
        cursor.read_next()?; // preload first event
        Ok(cursor)
    }

    fn read_next(&mut self) -> Result<(), TardisError> {
        if let Some(record) = self.iter.next() {

            let row: T = record.map_err(|e| TardisError::CsvParse {
                path: self.path.clone(),
                row: self.seq,
                source: e,
            })?;

            self.seq = self.seq.saturating_add(1);
            self.next = Some(row.into_domain_event(self.seq)); 
        } else {
            self.next = None;
        }
        Ok(())
    }

    /// take and return the current preloaded DomainEvent (if any) and advance
    pub fn take_next(&mut self) -> Result<Option<DomainEvent>, TardisError> {
        let out = self.next.take();
        if out.is_some() {
            self.read_next()?;
        }
        Ok(out)
    }
}
impl<T> EventCursor for CsvCursor<T>
where
    T: DeserializeOwned + DomainEventRow + Send + Sync,
{
    /// peek at the current preloaded DomainEvent (if any) without advancing
    fn peek(&self) -> Option<&DomainEvent> {
        self.next.as_ref()
    }
    /// advance to next DomainEvent
    fn advance(&mut self) -> Result<(),TardisError>{
        //consume current event and preload next
        self.take_next().map(|_| ())
    }
}
