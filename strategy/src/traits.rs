use common::{StrategyInput, Order, ExecutionEvent};

pub trait Strategy: Send {
    fn on_market(&mut self, update: &StrategyInput) -> Vec<Order>;
    fn on_fill(&mut self, fill: &ExecutionEvent);
}

