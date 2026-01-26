use tokio::sync::{broadcast,mpsc};
use tokio;
use crate::traits::Strategy;
use common::{StrategyInput,AnyWsUpdate, Order, ExecutionEvent};

pub async fn run_strategy<S: Strategy + 'static>(
    mut strategy: S,
    mut rx_market: mpsc::Receiver<StrategyInput>,
    mut rx_exec: broadcast::Receiver<ExecutionEvent>,
    tx_order: broadcast::Sender<Order>,
) {
    loop {
        tokio::select! {
            Some(update) = rx_market.recv() => {
                for order in strategy.on_market(&update) {
                    let _ = tx_order.send(order);
                }
            }
            Ok(fill)   = rx_exec.recv() => {
                strategy.on_fill(&fill);
            }
        }
    }
}

