use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast::{Sender, Receiver};
use tokio::sync::RwLock;
use common::{TradeUpdate, Order, Side, ExecutionEvent};
use book::OrderBook;
use rand;
// Simulates order handling at exchange. It maintains order requests in it's state, the relative 
// position within the price level and uses the "trade" feed to determine what orders might be 
// filled. Since we only receive accumulated volume at a price level and not the individual 
// orders at a specific price we approximate "Fill" events using a statistical transaction 
// probability (Monte Carlo). 
struct RestingOrder {
    id: u64,
    side: Side,
    price: f64,
    qty_total: u64,
    qty_remaining: u64,
    size_ahead: u64,  // updated when levels change
}
struct ExecutionState {
    pub tick_size: f64,
    pub buy_orders: HashMap<u64, Vec<RestingOrder>>, // keyed by order id
    pub sell_orders: HashSet<u64, Vec<RestingOrder>>, 
}
impl ExecutionState {
    pub fn Default() -> Self {
        Self {
            tick_size: 0.0,
            bid_orders: HashMap::new(),
            ask_orders: HashMap::new(),
        }
    }
    pub async fn place_or_cancel(&mut self, order: Order, orderbook: &Arc<RwLock<OrderBook>>) {
        match order {
            Order::Market{ symbol, side, size } => {
                // Place market order logic
                notimplemented!();
            }
            Order::Limit{ symbol: String, side: String, price: f64, size: u64} => {
                if self.tick_size > 0.0 {
                    let key = (price/tick_size) as u64;
                    match side.as_str() {
                        "Buy" => {
                            let level = orderbook.read().await.bids.entries.get(&key);
                            let size_ahead = match level {
                                Some(lvl) => lvl.size,
                                None => 0,
                            };
                            let order = RestingOrder {
                                id: key,
                                side: Side::Buy,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            self.buy_orders.insert(order.id, order);
                        }
                       "Sell" => {
                            let level = orderbook.read().await.asks.entries.get(&key);
                            let size_ahead = match level {
                                Some(lvl) => lvl.size,
                                None => 0,
                            };
                            let order = RestingOrder {
                                id: key,
                                side: Side::Sell,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            self.sell_orders.insert(order.id, order);
                        }
                        _ => {
                            println!("Unknown side: {}", side);
                        } 
                    }
                }
            }
            Order::Cancel{ order_id } => {
                // Cancel order logic
                notimplemented!();
            }
        }
    }
    pub async fn on_trade(&mut self, trade: TradeUpdate, orderbook: &Arc<RwLock<OrderBook>>) -> Option<ExecutionEvent> {
        /// Process trade update, check against resting orders, and return fills
        if self.tick_size == 0.0 {
            self.tick_size = trade.tick_size;
        }
        let key = (trade.price / self.tick_size) as u64;
        match trade.side.as_str() {
            "Buy" => {
                if let Some(orders) = self.ask_orders.get_mut(&key) {
                    let qty_level = orderbook.read().await.asks.entries.get(&key).map_or(0, |lvl| lvl.size);
                    for order in orders.iter_mut() {
                        // Scale probabilty of fill by relative position in queue
                        let p = 1.0 - (order.size_ahead / qty_level);
                        let r: f64 = rand::random();
                        // random trial
                        if r < p {
                            return self.process_fill(order, &trade).await;
                        } else {
                            // Update position in queue
                            order.size_ahead = order.size_ahead.saturating_sub(trade.size as u64);
                        }
                    }
                }
            }
            "Sell" => {}
        }
    }
    async fn process_fill(&mut self, order: &mut RestingOrder, trade: &TradeUpdate) -> Option<ExecutionEvent> {
        let fill_size = std::cmp::min(order.qty_remaining, trade.size as u64);
        order.qty_remaining -= fill_size;
        if order.qty_remaining == 0 {
            match order.side {
                Side::Buy => {
                    self.buy_orders.remove(&order.id);
                }
                Side::Sell => {
                    self.sell_orders.remove(&order.id);
                }
            }
            Some(ExecutionEvent {
                order_id: order.id,
                side: order.side.clone(),
                price: order.price,
                size: fill_size as u64,
                ts_exchange: trade.ts_exchange,
                ts_received: trade.ts_received,
            })       
        } else {
            None
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