use std::collections::{HashMap, hash_map::Entry};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
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

// Order ID counter
static NEXT_ORDER_ID: AtomicI64 = AtomicI64::new(1);

fn next_order_id() -> i64 {
    NEXT_ORDER_ID.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug, Clone)]
struct RestingOrder {
    id: i64,
    client_id: Option<u64>,
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
    
    // cached sorted keys for quick iteration.
    pub bid_keys: Vec<i64>,
    pub ask_keys: Vec<i64>,

    //map client_id to order_id for easy replacement/cancellation
    pub by_client_id: HashMap<u64, i64>,
}
impl ExecutionState {
    pub fn Default() -> Self {
        Self {
            //set tick size later from trade msg.
            tick_size: 0.0,
            bid_orders: HashMap::new(),
            ask_orders: HashMap::new(),
            bid_keys: Vec::new(),
            ask_keys: Vec::new(),
            by_client_id: HashMap::new(),
        }
    }
    // insert into sorted keys vec if missing
    fn insert_key(keys: &mut Vec<i64>, key: i64) {
        match keys.binary_search(&key) {
            Ok(_) => {} // already present
            Err(pos) => keys.insert(pos, key),
        }
    }

    // remove key from sorted keys vec
    fn remove_key(keys: &mut Vec<i64>, key: i64) {
        if let Ok(pos) = keys.binary_search(&key) {
            keys.remove(pos);
        }
    }
    
    async fn replace_limit(
        &mut self,
        order: Order,
        orderbook: &Arc<RwLock<OrderBook>>,
    ) {
        let client_id = match &order {
            Order::Limit { client_id, .. } => *client_id,
            _ => None,
        };

        // Cancel existing order if present
        if let Some(cid) = client_id {
            if let Some(order_id) = self.by_client_id.remove(&cid) {
                self.place_or_cancel(Order::Cancel { order_id }, orderbook).await;
            }
        }

        // Place new order
        self.place_or_cancel(order, orderbook).await;
    }

    pub async fn place_or_cancel(&mut self, order: Order, orderbook: &Arc<RwLock<OrderBook>>) {
        match order {
            Order::Market{ symbol, side, size } => {
                // Place market order logic
                unimplemented!();
            }
            Order::Limit{ symbol, side, price, size, client_id} => {
                if self.tick_size > 0.0 {
                    let key = (price/self.tick_size) as i64;
                    let ob = orderbook.read().await;

                    let order_id = next_order_id();
                    if let Some(cid) = client_id {
                        self.by_client_id.insert(cid, order_id);
                    }
                    match side {
                        Buy => {
                            // Limit Buy rests at bid side.
                            let size_ahead = ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: order_id,
                                side: Buy,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                                client_id,

                            };
                            // Push resting order and keep keys cached
                            match self.bid_orders.entry(key) {
                                Entry::Occupied(mut occ) => occ.get_mut().push(resting),
                                Entry::Vacant(vac) => {
                                    vac.insert(vec![resting]);
                                    Self::insert_key(&mut self.bid_keys, key);
                                }
                            }
                        }
                       Sell => {
                            // Limit sell rests at ask side.
                            let size_ahead = ob.asks.entries.get(&key).map_or(0, |lvl| lvl.size);
                            let resting = RestingOrder {
                                id: order_id,
                                side: Sell,
                                price,
                                qty_total: size,
                                qty_remaining: size,
                                size_ahead,
                                client_id,
                            };
                            // Push resting order and keep keys cached
                            match self.ask_orders.entry(key) {
                                Entry::Occupied(mut occ) => occ.get_mut().push(resting),
                                Entry::Vacant(vac) => {
                                    vac.insert(vec![resting]);
                                    Self::insert_key(&mut self.ask_keys, key);
                                }
                            }
                        }
                        _ => {
                            println!("Unknown side: {}", side.to_string());
                        } 
                    }
                }
            }
            Order::Cancel{ order_id } => {
                // Remove order and clean up empty levels
                self.bid_orders.retain(|k, orders| {
                    orders.retain(|o| o.id != order_id);
                    let keep = !orders.is_empty();
                    if !keep { Self::remove_key(&mut self.bid_keys, *k); }
                    keep                    
                });
                self.ask_orders.retain(|k, orders| {
                    orders.retain(|o| o.id != order_id);
                    let keep = !orders.is_empty();
                    if !keep { Self::remove_key(&mut self.ask_keys, *k); }
                    keep
                });
                // remove client_id
                self.by_client_id.retain(|_, &mut v| v != order_id);
            }
        }
    }
        // Process one price level: mutate orders Vec, consume remaining_trade_size, return fills
    fn process_level(
        orders: &mut Vec<RestingOrder>,
        qty_level: i64,
        remaining_trade_size: &mut i64,
        trade: &TradeUpdate,
    ) -> (Vec<ExecutionEvent>, Vec<i64>) {
        let mut fills = Vec::new();
        let mut fill_ids = Vec::new();
    
        for order in orders.iter_mut() {
            if *remaining_trade_size <= 0 {
                break;
            }
        
            let p = if qty_level > 0 {
                1.0 - (order.size_ahead as f64 / qty_level as f64)
            } else {
                1.0
            };
            let r: f64 = rand::random();
            if r < p {
                let fill_size = std::cmp::min(order.qty_remaining, *remaining_trade_size);
                order.qty_remaining -= fill_size;
                *remaining_trade_size -= fill_size;
                
                let action = if order.qty_remaining > 0 {
                    "Partial".to_string()
                } else {
                    "Full".to_string()
                };

                // collect filled order ids
                if order.qty_remaining == 0 {
                    fill_ids.push(order.id);
                }
            
                fills.push(ExecutionEvent {
                    action,
                    symbol: trade.base.clone() + &trade.quote,
                    order_id: order.id,
                    side: order.side.clone(),
                    price: order.price,
                    size: fill_size,
                    ts_exchange: trade.ts_exchange,
                    ts_received: trade.ts_received,
                });
                // continue to allow a single large trade to hit multiple orders
            } else {
                // move trade forward in queue
                order.size_ahead = order.size_ahead.saturating_sub(trade.size);
            }
        }
    
        // cleanup fully-filled orders
        orders.retain(|o| o.qty_remaining > 0);
        
        (fills, fill_ids)
    }
    pub async fn on_trade(&mut self, trade: TradeUpdate, orderbook: &Arc<RwLock<OrderBook>>) -> Vec<ExecutionEvent> {
        if self.tick_size == 0.0 { self.tick_size = trade.tick_size; }
        let trade_price = trade.price;
        let mut remaining_trade_size = trade.size;
        let mut fills = Vec::new();
        let mut keys_to_remove = Vec::new(); // collect keys first, then remove
        match trade.side {
            //Market Buy consumes from the ask side (resting sells)
            Buy => {
                // iterate ask_keys ascending (best asks first)
                for &key in self.ask_keys.iter() {
                    let ob_level = key as f64 * self.tick_size;
                    if ob_level > trade_price { break; }
                    else if ob_level < trade_price {
                        // If price moved past level without fill, 
                        // correct by filling all orders at level.
                        if let Some(orders) = self.ask_orders.get_mut(&key){
                            for order in orders{
                                fills.push(ExecutionEvent{
                                    action: "Full".to_string(),
                                    symbol: trade.base.clone() + &trade.quote,
                                    order_id: order.id,
                                    side: Sell,
                                    price: order.price,
                                    size: order.qty_remaining,
                                    ts_exchange: trade.ts_exchange,
                                    ts_received: trade.ts_received,
                                });
                                self.by_client_id.retain(|_, &mut v| v != order.id);
 
                            }
                        }
                        // Remove key.
                        keys_to_remove.push(key);
                        // Continue with next key.
                        continue;
                    } else {
                        // Trade price matches level — process fills statistically.
                        if remaining_trade_size <= 0 { break; }
                        let qty_level = {
                            let ob = orderbook.read().await;
                            ob.asks.entries.get(&key).map_or(0, |lvl| lvl.size)
                        };
                        if let Some(orders) = self.ask_orders.get_mut(&key) {
                            let (new_fills, fill_ids) = Self::process_level(orders, qty_level, &mut remaining_trade_size, &trade);
                            fills.extend(new_fills);
                            // remove filled orders from by_client_id
                            for &fill_id in &fill_ids {
                                self.by_client_id.retain(|_, &mut v| v != fill_id);
                            }
                            // cleanup empty levels
                            if orders.is_empty() {
                                self.ask_orders.remove(&key);
                                keys_to_remove.push(key);
                            }
                        }
                    }
                }
                //remove key
                for key in keys_to_remove {
                    Self::remove_key(&mut self.ask_keys, key);
                }
            }
            //Market Sell consumes from the bid side (resting buys)
            Sell => {
                // iterate bid_keys descending (best bids first)
                for &key in self.bid_keys.iter().rev() {
                    let ob_level = key as f64 * self.tick_size;
                    if ob_level < trade_price { break; }
                    else if ob_level > trade_price {
                        // Fill order
                        if let Some(orders) = self.bid_orders.get_mut(&key){
                            for order in orders{
                                fills.push(ExecutionEvent{
                                    action: "Full".to_string(),
                                    symbol: trade.base.clone() + &trade.quote,
                                    order_id: order.id,
                                    side: Buy,
                                    price: order.price,
                                    size: order.qty_remaining,
                                    ts_exchange: trade.ts_exchange,
                                    ts_received: trade.ts_received,
                                });
                                self.by_client_id.retain(|_, &mut v| v != order.id);
 
                            }
                        }
                        // Remove key.
                        keys_to_remove.push(key);
                        // Continue with next key.
                        continue;
                    } else {
                        if remaining_trade_size <= 0 { break; }
                        let qty_level = {
                            let ob = orderbook.read().await;
                            ob.bids.entries.get(&key).map_or(0, |lvl| lvl.size)
                        };
                        if let Some(orders) = self.bid_orders.get_mut(&key) {
                            let (new_fills, fill_ids) = Self::process_level(orders, qty_level, &mut remaining_trade_size, &trade);
                            fills.extend(new_fills);
                            // remove filled orders from by_client_id
                            for &fill_id in &fill_ids {
                                self.by_client_id.retain(|_, &mut v| v != fill_id);
                            }
                            // cleanup empty levels
                            if orders.is_empty() {
                                self.bid_orders.remove(&key);
                                keys_to_remove.push(key);
                            }
                        }
                    }
                }
                //remove key
                for key in keys_to_remove {
                    Self::remove_key(&mut self.bid_keys, key);
                }
            }
        }

        fills
    }
}

pub async fn run(orderbook: Arc<RwLock<OrderBook>>,
                 mut trade_rx: Receiver<AnyWsUpdate>,
                 mut order_rx: Receiver<Order>,
                 exec_tx: Sender<ExecutionEvent>) {
    let mut state = ExecutionState::Default();

    loop {
        tokio::select! {
            Ok(q) = order_rx.recv() => {
                state.replace_limit(q,&orderbook).await;
            }

            Ok(update) = trade_rx.recv() => {
                match update {
                    AnyWsUpdate::Trade(trade) => {
                        let fills = state.on_trade(trade, &orderbook).await;
                        for fill in fills {
                            exec_tx.send(fill).unwrap();
                        }                       
                    }
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
        // Ready signal for test to ensure task is running
        // when orders is sent.
        let (ready_tx, ready_rx) = oneshot::channel();
        // Spawn the run task with shared state
        tokio::spawn(async move {
            // Signal that the task is ready
            let _ = ready_tx.send(());
            loop {
                tokio::select! {
                    Ok(q) = order_rx.recv() => {
                        let mut s = state_clone.write().await;
                        s.replace_limit(q, &orderbook_clone).await;
                    }
                    Ok(trade) = trade_rx.recv() => {
                        let mut s = state_clone.write().await;
                        let fills = s.on_trade(trade, &orderbook_clone).await;
                        for fill in fills {
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
        order_tx.send(Order::Limit{ symbol: "XBTUSDT".to_string(), side: Sell, price: 50000.0, size: 100, client_id: Some(1)}).unwrap();

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
           side: Buy,
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
            assert!(s.ask_orders.get(&5000000).is_none(), "order should be removed after full fill");
        }
    }
    #[tokio::test]
    async fn test_order_fill_no_more_events() {
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
        // Size ahead zero for deterministic fill
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(5000000, BookEntry {side: "Ask".to_string(), price: 50000.0, size: 0 });
        }
        // Place limit sell order
        order_tx.send(Order::Limit{ symbol: "XBTUSDT".to_string(), side: Sell, price: 50000.0, size: 100, client_id: Some(1)}).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // allow some time to process

        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Buy,
           price: 50000.0,
           size: 100,
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();        

        // Check order filled by trade.
        let fill = tokio::time::timeout(std::time::Duration::from_secs(3), exec_rx.recv())
            .await
            .expect("timeout waiting for execution event")
            .expect("exec_rx closed");
        assert_eq!(fill.size, 100);
    
        // Now send one more trade at same price — should NOT produce another ExecutionEvent
        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Buy,
           price: 50000.0,
           size: 1, 
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();     
    
        // try recv with timeout
        let recv_fut = exec_rx.recv();
        let res = tokio::time::timeout(std::time::Duration::from_millis(1000), recv_fut).await;
        assert!(res.is_err(), "expected no further execution event after order removed");
    }
    #[tokio::test]
    async fn test_partial_fill_and_remains() {
        // Setup test environment
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;

        // Deterministic fill: Size ahead is zero.
        {
            let mut ob = orderbook.write().await;
            ob.bids.entries.insert(4000000, BookEntry { side: "Bid".to_string(), price: 40000.0, size: 0 });
        }

        // Place buy order of size 50, first in que.
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Buy, price: 40000.0, size: 50, client_id: Some(1)}).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process

        // Verify order was placed
        {
            let s = state.read().await;
            let key = (40000.0 / s.tick_size) as i64;
            let vec = s.bid_orders.get(&key).expect("order should exist");
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
            let vec = s.bid_orders.get(&key).expect("orders should remain");
            assert_eq!(vec[0].qty_remaining, 30);
            assert_eq!(vec[0].qty_total, 50);
            assert_eq!(vec[0].size_ahead, 0);
        }

        // Second trade fills the rest deterministically
        let t2 = TradeUpdate { size: 30, ..t1_clone };
        {
            let mut s = state.write().await;
            let ev2 = s.on_trade(t2, &orderbook).await.pop().expect("should have fill event");
            assert_eq!(ev2.size, 30);
        }
        // Check order removed
        {
            let s = state.read().await;
            let key = (40000.0 / s.tick_size) as i64;
            let vec = s.bid_orders.get(&key);
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
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Sell, price: 60000.0, size: 30, client_id: Some(1) }).expect("order send failed");
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Sell, price: 60000.0, size: 50, client_id: Some(2) }).expect("order send failed");
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
            side: Buy,
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
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;

        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(7000000, BookEntry { side: "Ask".to_string(), price: 70000.0, size: 1 });
        }

        // Place order
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Sell, price: 70000.0, size: 5, client_id: Some(1) }).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Get the order ID that was just placed
        let order_id = {
            let s = state.read().await;
            let key = (70000.0 / s.tick_size) as i64;
            s.ask_orders.get(&key).map(|vec| vec[0].id).expect("order should exist")
        };

        // Cancel the order via channel
        order_tx.send(Order::Cancel { order_id }).expect("cancel send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify order was cancelled
        {
            let s = state.read().await;
            let key = (70000.0 / s.tick_size) as i64;
            assert!(s.ask_orders.get(&key).is_none(), "order should be removed after cancel");
        }

        // Now trade arrives — should not produce fill
        let trade = TradeUpdate {
            exchange: "mock".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: Buy,
            price: 70000.0,
            size: 5,
            ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
            ts_received: chrono::Utc::now().timestamp_micros(),
        };

        trade_tx.send(trade).expect("trade send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Timeout — no fill should arrive
        let recv_fut = exec_rx.recv();
        let res = tokio::time::timeout(std::time::Duration::from_millis(100), recv_fut).await;
        assert!(res.is_err(), "expected no execution event after cancel");
    }
    #[tokio::test]
    async fn test_order_queueing() {
        //Setup test
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
        // Create order book level with size ahead
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(8000000, BookEntry { side: "Ask".to_string(), price: 80000.0, size: 180 });
        }
        // Place limit buy order
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Sell, price: 80000.0, size: 20, client_id: Some(1) }).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process

        // Verify order was placed with correct size_ahead
        {
            let s = state.read().await;
            let key = (80000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("order should exist");
            assert_eq!(vec[0].qty_remaining, 20);
            assert_eq!(vec[0].size_ahead, 180);
        }

        // Trade smaller than size_ahead untill order gets filled
        for _ in 0..10 {
            let trade = TradeUpdate {
                exchange: "mock".into(),
                base: "XBT".into(),
                quote: "USDT".into(),
                tick_size: 0.01,
                side: Buy,
                price: 80000.0,
                size: 20,
                ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
                ts_received: chrono::Utc::now().timestamp_micros(),
            };
            trade_tx.send(trade).expect("trade send failed");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await; // let task process
        }
        // Check for execution event
        let fill = tokio::time::timeout(std::time::Duration::from_secs(3), exec_rx.recv())
            .await
            .expect("timeout waiting for execution event")
            .expect("exec_rx closed");
        assert_eq!(fill.size, 20, "order should be filled after sufficient trades");
    }
    #[tokio::test]
    async fn fill_all_order_trade_crossed_level() {
        //setup test
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;
        // Create order book level with size ahead
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(9000000, BookEntry { side: "Ask".to_string(), price: 90000.0, size: 100 });
        }
        // Place limit buy order
        order_tx.send(Order::Limit { symbol: "XBTUSDT".into(), side: Sell, price: 90000.0, size: 50, client_id: Some(1)}).expect("order send failed");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let task process
        // Verify order was placed with correct size_ahead
        {
            let s = state.read().await;
            let key = (90000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("order should exist");
            assert_eq!(vec[0].qty_remaining, 50);
            assert_eq!(vec[0].size_ahead, 100);
        }
        // Trade with price > 90000.
        trade_tx.send(TradeUpdate {
           exchange: "mock_ex".to_string(),
           base: "XBT".to_string(),
           quote: "USDT".to_string(),
           tick_size: 0.01,
           side: Buy,
           price: 90001.0,
           size: 1, 
           ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
           ts_received: chrono::Utc::now().timestamp_micros(),
        }).unwrap();
        
        // Check fill at 9000.
        let fill = tokio::time::timeout(std::time::Duration::from_secs(3), exec_rx.recv())
            .await
            .expect("timeout waiting for execution event")
            .expect("exec_rx closed");
        assert_eq!(fill.price,90000.0);
    }
    #[tokio::test]
    async fn test_replace_by_client_id() {
        let (orderbook, trade_tx, order_tx, mut exec_rx, state) = setup_execution_engine().await;

        // Prepare two price levels
        {
            let mut ob = orderbook.write().await;
            ob.asks.entries.insert(5000000, BookEntry {
                side: "Ask".to_string(),
                price: 50000.0,
                size: 0,
            });
            ob.asks.entries.insert(4900000, BookEntry {
                side: "Ask".to_string(),
                price: 51000.0,
                size: 0,
            });
        }

        // Client places first order
        order_tx.send(Order::Limit {
            symbol: "XBTUSDT".into(),
            side: Sell,
            price: 50000.0,
            size: 100,
            client_id: Some(42),
        }).expect("send failed");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify first order exists
        let first_order_id = {
            let s = state.read().await;
            let key = (50000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&key).expect("first order should exist");
            assert_eq!(vec.len(), 1);
            vec[0].id
        };

        // Same client places replacement order at new price
        order_tx.send(Order::Limit {
            symbol: "XBTUSDT".into(),
            side: Sell,
            price: 51000.0,
            size: 50,
            client_id: Some(42),
        }).expect("send failed");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify old order removed, new order exists
        {
            let s = state.read().await;

            let old_key = (50000.0 / s.tick_size) as i64;
            assert!(
                s.ask_orders.get(&old_key).is_none(),
                "old order should be cancelled on replace"
            );

            let new_key = (51000.0 / s.tick_size) as i64;
            let vec = s.ask_orders.get(&new_key).expect("new order should exist");
            assert_eq!(vec.len(), 1);
            assert_eq!(vec[0].qty_remaining, 50);
            assert_ne!(vec[0].id, first_order_id);

            // by_client_id should map to new order id
            let mapped = s.by_client_id.get(&42).copied().expect("client id missing");
            assert_eq!(mapped, vec[0].id);
        }

        // Trade arrives at new price, should fill ONLY replacement order
        trade_tx.send(TradeUpdate {
            exchange: "mock".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: Buy,
            price: 51000.0,
            size: 50,
            ts_exchange: Some(chrono::Utc::now().timestamp_micros()),
            ts_received: chrono::Utc::now().timestamp_micros(),
        }).expect("trade send failed");

        let fill = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            exec_rx.recv(),
        )
        .await
        .expect("timeout")
        .expect("exec closed");

        assert_eq!(fill.size, 50);
        assert_eq!(fill.price, 51000.0);

        // No more orders should remain
        {
            let s = state.read().await;
            assert!(s.ask_orders.is_empty(), "all orders should be gone after fill");
            assert!(s.by_client_id.is_empty(), "client map should be empty");
        }
    }
}