use common::{AnyWsUpdate, Order, ExecutionEvent};

pub trait Strategy: Send {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order>;
    fn on_fill(&mut self, fill: &ExecutionEvent);
}

