use crate::cursor::event::DomainEvent;

pub struct MergeCursor {
    cursors: Vec<Box<dyn EventCursor>>,
}
// Decides what DomainEvent is next.
impl MergeCursor {
    pub fn next(&mut self) -> Option<DomainEvent> {
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

        let (idx, _) = best?;
        let event = self.cursors[idx].peek().cloned();
        self.cursors[idx].advance();
        event
    }
}

pub trait EventCursor {
    fn peek(&self) -> Option<&DomainEvent>;
    fn advance(&mut self);
}

