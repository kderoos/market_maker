use tokio::sync::{mpsc};
use tokio;
use crate::traits::Strategy;
use common::{StrategyInput,AnyWsUpdate, Order, ExecutionEvent};

pub async fn run_strategy(
    mut strategy: Box<dyn Strategy + Send>,
    mut rx_market: mpsc::Receiver<StrategyInput>,
    mut rx_exec: mpsc::Receiver<ExecutionEvent>,
    tx_order: mpsc::Sender<Order>,
) {
    loop {
        tokio::select! {
            Some(update) = rx_market.recv() => {
                for order in strategy.on_market(&update) {
                    if let Err(err) = tx_order.send(order).await {
                        eprintln!("Failed to send order from strategy: {:?}", err);
                    }
                }
            }
            Some(fill)   = rx_exec.recv() => {
                strategy.on_fill(&fill);
            }
        }
    }
}

