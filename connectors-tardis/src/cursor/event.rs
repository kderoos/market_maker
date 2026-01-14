use common::AnyUpdate;
#[repr(u8)]
// Set order of events with equal ts
#[derive(Debug,Copy, Clone, Eq, PartialEq)]
pub enum EventClass {
    Book  = 0,
    Quote = 1,
    Trade = 2,
}

// Expose the fields for chronological ordering
#[derive(Clone, Debug)]
pub struct DomainEvent {
    pub local_ts: i64,
    pub exchange_ts: Option<i64>,
    pub class: EventClass,
    // sequence number in datafile.
    pub seq: u64,
    pub payload: AnyUpdate,
}



