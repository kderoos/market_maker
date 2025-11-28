use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast::{Sender, Receiver};
use tokio::sync::RwLock;
use common::{TradeUpdate, Order};
use book::OrderBook;
// Simulates order handling at exchange. It maintains order requests in it's state, the relative 
// position within the price level and uses the "trade" feed to determine what orders might be 
// filled. Since we only receive accumulated volume at a price level and not the individual 
// orders at a specific price we approximate "Fill" events using a statistical transaction 
// probability (Monte Carlo). 
struct RestingOrder {
    id: u64,
    side: Side,
    price: f64,
    qty_total: f64,
    qty_remaining: f64,
    size_ahead: f64,  // updated when levels change
}
struct ExecutionState {
    pub tick_size: f64,
    pub buy_orders: HashSet<u64, RestingOrder>, // keyed by order id
    pub sell_orders: HashSet<u64, RestingOrder>, 
}
impl ExecutionState {
    pub fn default() -> Self {
        Self {
            tick_size: 0.0,
            buy_orders: HashSet::new(),
            sell_orders: HashSet::new(),
        }
    }
    pub fn place_or_cancel(&mut self, order: Order, orderbook: &Arc<RwLock<OrderBook>>) {
        match order {
            Order::Market{ symbol, side, size } => {
                // Place market order logic
                notimplemented!();
            }
            Order::Limit{ symbol, side, price, size } => {
                if self.tick_size > 0.0 {
                    match side
                }
            }
            Order::Cancel{ order_id } => {
                // Cancel order logic
                notimplemented!();
            }
        }
    }
}

pub async fn run(orderbook: Arc<RwLock<OrderBook>>,
                 mut trade_rx: Receiver<TradeUpdate>,
                 mut quote_rx: Receiver<Order>,
                 exec_tx: Sender<ExecutionEvent>) {
    let mut state = ExecutionState::default();

    loop {
        tokio::select! {
            Some(q) = order_rx.recv() => {
                state.place_or_cancel(q, &orderbook).await;
            }

            Some(trade) = trade_rx.recv() => {
                if let Some(fill) = state.on_trade(trade, &orderbook).await {
                    exec_tx.send(fill).await.unwrap();
                }
            }
        }
    }
}