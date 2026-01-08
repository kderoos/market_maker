pub struct BookCursor;

impl EventCursor for BookCursor {
    fn peek(&self) -> Option<&DomainEvent> {
        None
    }
    fn advance(&mut self) {}
}

