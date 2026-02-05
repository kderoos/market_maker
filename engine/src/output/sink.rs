use async_trait::async_trait;
use crate::output::event::OutputEvent;

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn handle(&self, event: OutputEvent);
}
