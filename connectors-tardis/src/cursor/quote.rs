pub struct QuoteCursor;

impl EventCursor for QuoteCursor {
    fn peek(&self) -> Option<&DomainEvent> {
        None
    }
    fn advance(&mut self) {}
}

