use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::{Sender, Receiver};
use tokio::sync::RwLock;
use common::{BookEntry, TradeUpdate, Order, OrderSide, ExecutionEvent};
use common::OrderSide::{Buy, Sell};
use crate::book::OrderBook;
use rand;
use chrono;
// Simulates order handling at exchange. It maintains order requests in it's state, the relative 
// position within the price level and uses the "trade" feed to determine what orders might be 
// filled. Since we only receive accumulated volume at a price level and not the individual 
// orders at a specific price we approximate "Fill" events using a statistical transaction 
// probability (Monte Carlo). 
struct RestingOrder {
    id: i64,
    side: OrderSide,
    price: f64,
    qty_total: i64,
    qty_remaining: i64,
    size_ahead: i64,  // updated when levels change
}
struct ExecutionState {
    pub tick_size: f64,
    pub bid_orders: HashMap<i64, Vec<RestingOrder>>, // keyed by order id
    pub ask_orders: HashMap<i64, Vec<RestingOrder>>, 
}
impl ExecutionState {
    pub fn Default() -> Self {
        Self {
            //set tick size later from trade msg.
            tick_size: 0.0,
            bid_orders: HashMap::new(),
            ask_orders: HashMap::new(),
        }
    }
    pub async fn place_or_cancel(&mut self, order: Order, orderbook: &Arc<RwLock<OrderBook>>) {
        match order {
            Order::Market{ symbol, side, size } => {
                // Place market order logic
                unimplemented!();
            }
            Order::Limit{ symbol, side, price, size} => {
                if self.tick_size > 0.0 {
                    let key = (price/self.tick_size) as i64;
                    let ob = orderbook.read().await;
                    match side {
                        Buy => {
                            // let level = orderbook.read().await.bids.entries.get(&key);
                            let size_ahead = ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: key,
                                side: Buy,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            // Push to Vec or existing or create new Vec
                            self.bid_orders.entry(key).or_insert_with(Vec::new).push(resting);
                        }
                       Sell => {
                            let size_ahead = ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: key,
                                side: Sell,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            self.ask_orders.entry(key).or_insert_with(Vec::new).push(resting);
                        }
                        _ => {
                            println!("Unknown side: {}", side.to_string());
                        } 
                    }
                }
            }
            Order::Cancel{ order_id } => {
                // Cancel order logic
                unimplemented!();
            }
        }
    }
    pub async fn on_trade(&mut self, trade: TradeUpdate, orderbook: &Arc<RwLock<OrderBook>>) -> Option<ExecutionEvent> {
        /// Process trade update, check against resting orders, and return fills
        if self.tick_size == 0.0 {
            self.tick_size = trade.tick_size;
        }
        let key = (trade.price / self.tick_size) as i64;

        match trade.side {
            Buy => {
                // Do the async read first (no mutable borrow of self while awaiting)
                let qty_level = {
                    let ob = orderbook.read().await;
                    ob.asks.entries.get(&key).map_or(0, |lvl| lvl.size)
                };

                if let Some(orders) = self.ask_orders.get_mut(&key) {
                    // Iterate mutably, perform fill inline (no awaits)
                    let mut maybe_event: Option<ExecutionEvent> = None;
                    for order in orders.iter_mut() {
                        // avoid division by zero
                        let p = if qty_level > 0 {
                            1.0 - (order.size_ahead as f64 / qty_level as f64)
                        } else {
                            1.0
                        };
                        let r: f64 = rand::random();
                        if r < p {
                            // compute fill inline
                            let fill_size = std::cmp::min(order.qty_remaining, trade.size);
                            order.qty_remaining -= fill_size;
                            maybe_event = Some(ExecutionEvent {
                                order_id: order.id,
                                side: order.side.clone(),
                                price: order.price,
                                size: fill_size,
                                ts_exchange: trade.ts_exchange,
                                ts_received: trade.ts_received,
                            });
                            break;
                        } else {
                            order.size_ahead = order.size_ahead.saturating_sub(trade.size);
                        }
                    }
                    // drop mutable borrow to self.ask_orders by ending scope
                    if let Some(ev) = maybe_event {
                        // remove fully-filled orders now that mutable borrow ended
                        if let Some(vec) = self.ask_orders.get_mut(&key) {
                            vec.retain(|o| o.qty_remaining > 0);
                            if vec.is_empty() {
                                self.ask_orders.remove(&key);
                            }
                        }
                        return Some(ev);
                    }
                }
                None
            }
            Sell => {
                // read level first
                let qty_level = {
                    let ob = orderbook.read().await;
                    ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size)
                };

                if let Some(orders) = self.bid_orders.get_mut(&key) {
                    let mut maybe_event: Option<ExecutionEvent> = None;
                    for order in orders.iter_mut() {
                        let p = if qty_level > 0 {
                            1.0 - (order.size_ahead as f64 / qty_level as f64)
                        } else {
                            1.0
                        };
                        let r: f64 = rand::random();
                        if r < p {
                            let fill_size = std::cmp::min(order.qty_remaining, trade.size);
                            order.qty_remaining -= fill_size;
                            maybe_event = Some(ExecutionEvent {
                                order_id: order.id,
                                side: order.side.clone(),
                                price: order.price,
                                size: fill_size,
                                ts_exchange: trade.ts_exchange,
                                ts_received: trade.ts_received,
                            });
                            break;
                        } else {
                            order.size_ahead = order.size_ahead.saturating_sub(trade.size);
                        }
                    }
                    if let Some(ev) = maybe_event {
                        if let Some(vec) = self.bid_orders.get_mut(&key) {
                            vec.retain(|o| o.qty_remaining > 0);
                            if vec.is_empty() {
                                self.bid_orders.remove(&key);
                            }
                        }
                        return Some(ev);
                    }
                }
                None
            }
        }
    }
}

pub async fn run(orderbook: Arc<RwLock<OrderBook>>,
                 mut trade_rx: Receiver<TradeUpdate>,
                 mut order_rx: Receiver<Order>,
                 exec_tx: Sender<ExecutionEvent>) {
    let mut state = ExecutionState::Default();

    loop {
        tokio::select! {
            Ok(q) = order_rx.recv() => {
                state.place_or_cancel(q, &orderbook).await;
            }

            Ok(trade) = trade_rx.recv() => {
                if let Some(fill) = state.on_trade(trade, &orderbook).await {
                    exec_tx.send(fill).unwrap();
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_execution_engine() {
        
        let orderbook = Arc::new(RwLock::new(OrderBook::default()));
        let (trade_tx, trade_rx) = tokio::sync::broadcast::channel(100);
        let (order_tx, order_rx) = tokio::sync::broadcast::channel(100);
        let (exec_tx, mut exec_rx) = tokio::sync::broadcast::channel(100);
        tokio::spawn(run(orderbook.clone(), trade_rx, order_rx, exec_tx));
        // create some qty_ahead in order book
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(5000000, BookEntry {side: "Ask".to_string(), price: 50000.0, size: 10 });
            ob.asks.entries.insert(4999999, BookEntry {side: "Ask".to_string(), price: 49999.99, size: 1000 });
        }
        // Place limit buy order
        order_tx.send(Order::Limit{ symbol: "XBTUSDT".to_string(), side: Buy, price: 50000.0, size: 100 }).unwrap();

        // Simulate trade that should fill the order
        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Sell,
           price: 50000.0,
           size: 10,
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();
         
        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Sell,
           price: 50000.0,
           size: 100,
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();        

        // Check for execution event
        let fill = exec_rx.recv().await.unwrap();
        assert_eq!(fill.size, 100);
        assert_eq!(fill.price, 50000.0);
    }
}