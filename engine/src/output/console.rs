use async_trait::async_trait;
use crate::output::event::OutputEvent;
use crate::output::sink::OutputSink;

pub struct ConsoleSink;

impl ConsoleSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl OutputSink for ConsoleSink {
    async fn handle(&self, event: OutputEvent) {
        match event {
            OutputEvent::Position(pos) => {
                println!(
                    "[POSITION] ts={} position={} avg_price={} realized_pnl={}, cash={}, fees_paid= {}",
                    pos.timestamp, pos.position, pos.avg_price, pos.realized_pnl, pos.cash, pos.fees_paid
                );
            }
            _ => {}
        }
    }
}
