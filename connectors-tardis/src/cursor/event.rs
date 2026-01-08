#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq)]
// Set order of events with equal ts
pub enum EventClass {
    Book  = 0,
    Quote = 1,
    Trade = 2,
}

// Expose the fields for chronological ordering
pub struct DomainEvent {
    pub local_ts: i64,
    pub exchange_ts: i64,
    pub class: EventClass,
    // sequence number in OG datafile.
    pub seq: u64,
    pub payload: DomainPayload,
}

pub enum DomainPayload {
    Trade(Trade),
    Quote(Quote),
    Book(BookEvent),
}

