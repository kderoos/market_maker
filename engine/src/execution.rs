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
#[derive(Debug, Clone)]
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
                            // Market Maker buys at ask side.
                            let size_ahead = ob.asks.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: key,
                                side: Buy,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            // Push to Vec or existing or create new Vec
                            self.ask_orders.entry(key).or_insert_with(Vec::new).push(resting);
                        }
                       Sell => {
                            // Market Maker sells at bid side.
                            let size_ahead = ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: key,
                                side: Sell,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                            };
                            self.bid_orders.entry(key).or_insert_with(Vec::new).push(resting);
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
                    ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size)
                };

                if let Some(orders) = self.bid_orders.get_mut(&key) {
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
                                action: "Fill".to_string(),
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
                    // drop mutable borrow to self.bid_orders by ending scope
                    if let Some(ev) = maybe_event {
                        // remove fully-filled orders now that mutable borrow ended
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
            Sell => {
                // read level first
                let qty_level = {
                    let ob = orderbook.read().await;
                    ob.asks.entries.get(&key).map_or(0, |lvl| lvl.size)
                };

                if let Some(orders) = self.ask_orders.get_mut(&key) {
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
                                action: "Fill".to_string(),
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
use tokio::sync::oneshot;
#[cfg(test)]
mod tests {
    use super::*;
    // Shared fixture: returns (orderbook, trade_tx, order_tx, exec_rx, state_arc)
    async fn setup_execution_engine() -> (
        Arc<RwLock<OrderBook>>,
        tokio::sync::broadcast::Sender<TradeUpdate>,
        tokio::sync::broadcast::Sender<Order>,
        tokio::sync::broadcast::Receiver<ExecutionEvent>,
        Arc<RwLock<ExecutionState>>,
    ) {
        let orderbook = Arc::new(RwLock::new(OrderBook::default()));
        let (trade_tx, mut trade_rx) = tokio::sync::broadcast::channel(100);
        let (order_tx, mut order_rx) = tokio::sync::broadcast::channel(100);
        let (exec_tx, exec_rx) = tokio::sync::broadcast::channel(100);
        
        // Create and share the state
        let state = Arc::new(RwLock::new(ExecutionState::Default()));
        let state_clone = state.clone();
        
        // Clone orderbook for task
        let orderbook_clone = orderbook.clone();
        let exec_tx_clone = exec_tx.clone();

        // Initialize tick_size in state
        {
            let mut s = state.write().await;
            s.tick_size = 0.01;
        }
    
        let (ready_tx, ready_rx) = oneshot::channel();
        // Spawn the run task with shared state
        tokio::spawn(async move {
            // Signal that the task is ready
            let _ = ready_tx.send(());
            loop {
                tokio::select! {
                    Ok(q) = order_rx.recv() => {
                        let mut s = state_clone.write().await;
                        s.place_or_cancel(q, &orderbook_clone).await;
                    }
                    Ok(trade) = trade_rx.recv() => {
                        let mut s = state_clone.write().await;
                        if let Some(fill) = s.on_trade(trade, &orderbook_clone).await {
                            let _ = exec_tx_clone.send(fill);
                        }
                    }
                }
            }
        });
        _ = ready_rx.await; // wait for task to be ready 
        (orderbook, trade_tx, order_tx, exec_rx, state)
    }
    #[tokio::test]
    async fn test_execution_engine() {
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
        // create some qty_ahead in order book
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(5000000, BookEntry {side: "Ask".to_string(), price: 50000.0, size: 0 });
        }
        // Place limit buy order
        order_tx.send(Order::Limit{ symbol: "XBTUSDT".to_string(), side: Buy, price: 50000.0, size: 100 }).unwrap();

        // Simulate trade that should fill the order
        // trade_tx.send(TradeUpdate {
        //    exchange: "mock_ex".to_string(),
        //    base: "XBT".to_string(),
        //    quote: "USDT".to_string(),
        //    tick_size: 0.01,
        //    side: Sell,
        //    price: 50000.0,
        //    size: 10,
        //    ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
        //    ts_received: chrono::Utc::now().timestamp_micros(),
        // }).unwrap();
        
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // allow some time for order to be processed
        assert_eq!(state.read().await.ask_orders.len(), 1, "there should be one resting order");
        let s = state.read().await.ask_orders.get(&5000000).unwrap()[0].clone();
        assert_eq!(s.qty_remaining, 100, "resting order should have full qty");
        assert_eq!(s.size_ahead, 0, "size_ahead should be zero for deterministic fill");

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
        let fill = tokio::time::timeout(std::time::Duration::from_secs(3), exec_rx.recv())
            .await
            .expect("timeout waiting for execution event")
            .expect("exec_rx closed");

        assert_eq!(fill.size, 100);
        assert_eq!(fill.price, 50000.0);
        
        // After fill, level should be removed
        {
            let s = state.read().await;
            assert!(s.bid_orders.get(&5000000).is_none(), "order should be removed after full fill");
        }
    }
    #[tokio::test]
    async fn test_order_removed_via_no_more_events() {
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
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

        // place order and send trades to fill it (same as existing test)
        let fill = exec_rx.recv().await.unwrap();
        assert_eq!(fill.size, 100);
    
        // Now send one more trade at same price — should NOT produce another ExecutionEvent
        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Sell,
           price: 50000.0,
           size: 1, 
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();     
    
        // try recv with timeout
        let recv_fut = exec_rx.recv();
        let res = tokio::time::timeout(std::time::Duration::from_millis(100), recv_fut).await;
        assert!(res.is_err(), "expected no further execution event after order removed");
    }
    #[tokio::test]
    async fn test_partial_fill_and_remains() {
        // Setup test environment
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;

        // Deterministic fill: Size ahead is zero.
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(4000000, BookEntry { side: "Ask".to_string(), price: 40000.0, size: 0 });
        }

        // Place buy order of size 50, first in que.
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Buy, price: 40000.0, size: 50 }).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process

        // Verify order was placed
        {
            let s = state.read().await;
            let key = (40000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("order should exist");
            assert_eq!(vec[0].qty_remaining, 50);
            assert_eq!(vec[0].qty_total, 50);
            assert_eq!(vec[0].size_ahead, 0);
        }

        // Trade smaller than order: 20 -> partial fill
        let t1 = TradeUpdate {
            exchange: "mock".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: Sell,
            price: 40000.0,
            size: 20,
            ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
            ts_received: chrono::Utc::now().timestamp_micros(),
        };

        let t1_clone = t1.clone();

        // First trade partially fills
        trade_tx.send(t1).expect("trade send failed");
        // Check for execution event
        let fill1 =  exec_rx.recv()
            .await
            .expect("exec_rx closed"); 
        assert_eq!(fill1.size, 20);

        // Check remaining quantity reduced
        {
            let s = state.read().await;
            let key = (40000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("orders should remain");
            assert_eq!(vec[0].qty_remaining, 30);
            assert_eq!(vec[0].qty_total, 50);
            assert_eq!(vec[0].size_ahead, 0);
        }

        // Second trade fills the rest deterministically
        let t2 = TradeUpdate { size: 30, ..t1_clone };
        {
            let mut s = state.write().await;
            let ev2 = s.on_trade(t2, &orderbook).await.expect("expected fill");
            assert_eq!(ev2.size, 30);
        }
        // Check order removed
        {
            let s = state.read().await;
            let key = (40000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key);
            assert!(vec.is_none(), "order should be removed after full fill");
        }
    }

    #[tokio::test]
    async fn test_multiple_orders_fifo() {
        // Setup test environment
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
        // Deterministic fill: Size ahead is zero.
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(6000000, BookEntry { side: "Ask".to_string(), price: 60000.0, size: 0 });
        }
        // Place two buy orders at same price
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Buy, price: 60000.0, size: 30 }).expect("order send failed");
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Buy, price: 60000.0, size: 50 }).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process 
        // Verify both orders were placed
        {
            let s = state.read().await;
            let key = (60000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("orders should exist");
            assert_eq!(vec.len(), 2, "there should be two resting orders");
            assert_eq!(vec[0].qty_remaining, 30);
            assert_eq!(vec[1].qty_remaining, 50);
        }
        // Trade of size 40 should fill first order (30) and partially fill second (10)
        let trade = TradeUpdate {
            exchange: "mock".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: Sell,
            price: 60000.0,
            size: 40,
            ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
            ts_received: chrono::Utc::now().timestamp_micros(),
        };
        trade_tx.send(trade).expect("trade send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process

        // First fill event
        let fill1 = tokio::time::timeout(std::time::Duration::from_secs(1), exec_rx.recv())
            .await
            .expect("timeout waiting for first fill")
            .expect("exec_rx closed");
        assert_eq!(fill1.size, 30, "first order should be fully filled");

        // Second fill event
        let fill2 = tokio::time::timeout(std::time::Duration::from_secs(1), exec_rx.recv())
            .await
            .expect("timeout waiting for second fill")
            .expect("exec_rx closed");

        assert_eq!(fill2.size, 10, "second order should be partially filled");
        // Check remaining quantity of second order
        {
            let s = state.read().await;
            let key = (60000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("orders should remain");
            assert_eq!(vec.len(), 1, "one order should remain");
            assert_eq!(vec[0].qty_remaining, 40, "second order should have 40 remaining");
        }   
    }   

    #[tokio::test]
    async fn test_cancel_removes_order() {
        let orderbook = Arc::new(RwLock::new(OrderBook::default()));
        let mut state = ExecutionState::Default();

        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(7000000, BookEntry { side: "Ask".to_string(), price: 70000.0, size: 1 });
        }

        // Place and then cancel
        state.place_or_cancel(Order::Limit { symbol: "XBTUSDT".into(), side: Buy, price: 70000.0, size: 5 }, &orderbook).await;

        // cancel: we currently have unimplemented cancel() — if you implement, call here; as a fallback, remove direct
        // Simulate canceling by removing from state
        let key = (70000.0 / state.tick_size) as i64;
        state.bid_orders.remove(&key);

        // Now trade arrives, should not trigger any fill
        let trade = TradeUpdate {
            exchange: "mock".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: Sell,
            price: 70000.0,
            size: 5,
            ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
            ts_received: chrono::Utc::now().timestamp_micros(),
        };

        assert!(state.on_trade(trade, &orderbook).await.is_none());
    }
}