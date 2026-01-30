use std::collections::{HashMap, hash_map::Entry};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::broadcast::{Sender, Receiver};
use common::{BookEntry, TradeUpdate, Order, OrderSide, AnyWsUpdate, AnyUpdate, BookUpdate, ExecutionEvent};
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
pub struct ExecutionState {
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
        orderbook: &mut OrderBook,
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

    pub async fn place_or_cancel(&mut self, order: Order, orderbook: &mut OrderBook) {
        match order {
            Order::Market{ symbol, side, size } => {
                // Place market order logic
                unimplemented!();
            }
            Order::Limit{ symbol, side, price, size, client_id, ts_received } => {
                if self.tick_size > 0.0 {
                    let key = (price/self.tick_size) as i64;
                    let ob = orderbook;

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
    pub async fn on_trade(&mut self, trade: TradeUpdate, orderbook: &OrderBook) -> Vec<ExecutionEvent> {
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
                            orderbook.asks.entries.get(&key).map_or(0, |lvl| lvl.size)
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
                            orderbook.bids.entries.get(&key).map_or(0, |lvl| lvl.size)
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

struct DeterministicEngine {
    // market
    orderbook: OrderBook,          
    exec: ExecutionState,

    // time
    book_watermark_ts: i64,         // last applied book/trade timestamp

    // order gating
    pending_orders: std::collections::VecDeque<(i64, Order)>, // (ts_received, order)
}
pub async fn run_deterministic(
    mut rx_ws: mpsc::Receiver<AnyUpdate>,
    mut order_rx: mpsc::Receiver<Order>,
    exec_tx: mpsc::Sender<ExecutionEvent>,
    latency_ms: i64, // microseconds
) {
    let ex_latency = latency_ms * 1000; // to microseconds
    let mut engine = DeterministicEngine {
        orderbook: OrderBook::default(),
        exec: ExecutionState::Default(),
        book_watermark_ts: 0,
        pending_orders: std::collections::VecDeque::new(),
    };

    loop {
        tokio::select! {
            Some(update) = rx_ws.recv() => {
                match update {
                    AnyUpdate::BookUpdate(book_update) => {
                        if engine.exec.tick_size == 0.0 {
                            engine.exec.tick_size = book_update.tick_size;
                        }
                        apply_book_update(book_update,&mut engine.orderbook);
                        engine.book_watermark_ts = engine.orderbook.timestamp;
                        //
                    }

                    AnyUpdate::TradeUpdate(trade) => {
                        engine.book_watermark_ts =
                            engine.book_watermark_ts.max(trade.ts_received);

                        let fills = engine.exec.on_trade(trade, &engine.orderbook).await;
                        for fill in fills {
                            if let Err(e) = exec_tx.send(fill).await {
                                eprintln!("failed to send execution event: {:?}", e);
                            }
                        }
                    }

                    _ => {}
                }

                // Try releasing orders after market update
                release_orders(&mut engine, ex_latency).await;
            }

            //Buffer incoming orders
            Some(order) = order_rx.recv() => {
                match &order {
                    Order::Market {  .. } => {
                    }
                    Order::Limit { ts_received, .. } => {
                        engine.pending_orders.push_back((*ts_received, order));
                    }
                    Order::Cancel { .. } => {
                        // TODO: remove from pending orders if present

                        // Remove resting order if present.
                        engine.exec.place_or_cancel(order, &mut engine.orderbook).await;
                    }
                    _ => {}
                }
                // Check for orders
                release_orders(&mut engine, ex_latency).await;
            }
        }
    }
}
// Synchronous function to apply book updates.
pub fn apply_book_update(book_update: BookUpdate, book_state: &mut OrderBook) {
        // Process BookUpdate messages
        {
            let mut state = book_state;
            // Update the order book state based on the action
            match book_update.action.as_str() {
                "insert" => {
                    for (id, entry) in book_update.data {
                        let side = if entry.side == "Buy" { &mut state.bids } else { &mut state.asks };
                        let key = (entry.price/book_update.tick_size) as i64;
                        side.entries.insert(key, entry);
                        state.timestamp = book_update.ts_received;
                    }
                }
                "update" => {
                    for (id, entry) in book_update.data {
                        let side = if entry.side == "Buy" { &mut state.bids } else { &mut state.asks };
                        let key = (entry.price/book_update.tick_size) as i64;
                        side.entries.insert(key, entry);
                    }
                }
                "delete" => {
                    for (id, entry) in book_update.data {
                        let side = if entry.side == "Buy" { &mut state.bids } else { &mut state.asks };
                        let key = (entry.price/book_update.tick_size) as i64;
                        side.entries.remove(&key);
                    }
                }
                "partial" => {
                    // Replace the entire order book for the symbol
                    state.bids.entries.clear();
                    state.asks.entries.clear();
                    for (id, entry) in book_update.data {
                        let side = if entry.side == "Buy" { &mut state.bids } else { &mut state.asks };
                        let key = (entry.price/book_update.tick_size) as i64;
                        side.entries.insert(key, entry);
                    }
                }
                _ => {
                    eprintln!("Unknown action: {}", book_update.action);
                }
            }
        }
    }
async fn release_orders(engine: &mut DeterministicEngine, ex_latency: i64) {
    while let Some((ts, _)) = engine.pending_orders.front() {
        if *ts <= engine.book_watermark_ts - ex_latency {
            let (_, order) = engine.pending_orders.pop_front().unwrap();
            engine.exec.replace_limit(order, &mut engine.orderbook).await;
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod deterministic_execution_tests {
    use super::*;
    use tokio::sync::mpsc::channel;
    use chrono::Utc;
    use common::{
        AnyUpdate, BookEntry, BookUpdate, ExecutionEvent,
        Order, OrderSide::{Buy, Sell}, TradeUpdate,
    };

    fn now_us() -> i64 {
        Utc::now().timestamp_micros()
    }

    struct Harness {
        ws_tx: mpsc::Sender<AnyUpdate>,
        order_tx: mpsc::Sender<Order>,
        exec_rx: mpsc::Receiver<ExecutionEvent>,
    }

    async fn spawn_engine(latency_ms: i64) -> Harness {
        let (ws_tx, ws_rx) = channel(32);
        let (order_tx, order_rx) = channel(32);
        let (exec_tx, exec_rx) = channel(32);

        tokio::spawn(run_deterministic(
            ws_rx,
            order_rx,
            exec_tx,
            latency_ms,
        ));

        Harness { ws_tx, order_tx, exec_rx }
    }

    async fn send_book(
        h: &Harness,
        price: f64,
        size: i64,
        ts: i64,
    ) {
        h.ws_tx.send(AnyUpdate::BookUpdate(BookUpdate {
            exchange: "test".into(),
            symbol: "XBTUSDT".into(),
            action: "insert".into(),
            tick_size: 0.01,
            data: vec![(
                "lvl".into(),
                BookEntry {
                    side: "Sell".into(),
                    price,
                    size,
                },
            )],
            ts_exchange: None,
            ts_received: ts,
        }))
        .await
        .unwrap();
    }

    async fn send_trade(
        h: &Harness,
        side: common::OrderSide,
        price: f64,
        size: i64,
        ts: i64,
    ) {
        h.ws_tx.send(AnyUpdate::TradeUpdate(TradeUpdate {
            exchange: "test".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side,
            price,
            size,
            ts_exchange: None,
            ts_received: ts,
        }))
        .await
        .unwrap();
    }

    async fn send_limit(
        h: &Harness,
        side: common::OrderSide,
        price: f64,
        size: i64,
        client_id: u64,
        ts: i64,
    ) {
        h.order_tx.send(Order::Limit {
            symbol: "XBTUSDT".into(),
            side,
            price,
            size,
            client_id: Some(client_id),
            ts_received: ts,
        })
        .await
        .unwrap();
    }
    async fn flush(h: &Harness, ts: i64) {
        h.ws_tx.send(AnyUpdate::BookUpdate(BookUpdate {
            exchange: "test".into(),
            symbol: "XBTUSDT".into(),
            action: "update".into(),
            tick_size: 0.01,
            data: vec![],
            ts_exchange: None,
            ts_received: ts,
        }))
        .await
        .unwrap();
    }


    #[tokio::test]
    async fn full_fill_single_order() {
        let mut h = spawn_engine(0).await;
        let t0 = 1_000;

        // Create a level in orderbook
        send_book(&h, 50_000.0, 0, t0).await;
        // Limit order in queue, 
        send_limit(&h, Sell, 50_000.0, 100, 1, t0).await;
        // time advances past latency_us, order -> restingOrder.
        send_trade(&h, Buy, 49_000.0, 100, t0 + 2).await; // flush
        // Resting Order get matched with trade.
        send_trade(&h, Buy, 50_000.0, 100, t0 + 3).await;

        let fill = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.exec_rx.recv(),
            ).await.unwrap().unwrap();

        assert_eq!(fill.size, 100);
        assert_eq!(fill.price, 50_000.0);

    }

    #[tokio::test]
    async fn partial_then_full_fill() {
        let mut h = spawn_engine(0).await;
        let t0 = 2_000;

        send_book(&h, 40_000.0, 0, t0).await;
        send_limit(&h, Buy, 40_000.0, 50, 1, t0).await;

        send_trade(&h, Sell, 40_000.0, 20, t0 + 1).await;
        let f1 = h.exec_rx.recv().await.unwrap();
        assert_eq!(f1.size, 20);

        send_trade(&h, Sell, 40_000.0, 30, t0 + 2).await;
        let f2 = h.exec_rx.recv().await.unwrap();
        assert_eq!(f2.size, 30);
    }

    #[tokio::test]
    async fn fifo_two_orders() {
        let mut h = spawn_engine(0).await;
        let t0 = 3_000;

        send_book(&h, 60_000.0, 0, t0).await;

        send_limit(&h, Sell, 60_000.0, 30, 1, t0).await;

        send_book(&h, 60_000.0, 0, t0 + 1).await;

        send_limit(&h, Sell, 60_000.0, 50, 2, t0 + 1).await;
        // Order flush
        send_trade(&h, Buy, 59_000.0, 40, t0 + 2).await;
        // Trade to match both orders
        send_trade(&h, Buy, 60_000.0, 40, t0 + 3).await;

        let f1 = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.exec_rx.recv(),
            ).await.unwrap().unwrap();
        let f2 = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                h.exec_rx.recv(),
            ).await.unwrap().unwrap();

        assert_eq!(f1.size, 30);
        assert_eq!(f2.size, 10);
    }

    #[tokio::test]
    async fn replace_by_client_id() {
        let mut h = spawn_engine(0).await;
        let t0 = 1_000;

        send_book(&h, 50_000.0, 0, t0).await;
        send_book(&h, 51_000.0, 0, t0).await;

        send_limit(&h, Sell, 51_000.0, 100, 42, t0).await;
        send_limit(&h, Sell, 50_000.0, 50, 42, t0 + 2).await;
        // Trade to flush orders.
        send_trade(&h, Buy, 49_000.0, 100, t0 + 4).await;

        send_trade(&h, Buy, 50_000.0, 50, t0 + 5).await;

        let fill = h.exec_rx.recv().await.unwrap();
        assert_eq!(fill.price, 50_000.0);
        assert_eq!(fill.size, 50);
        
        send_trade(&h, Buy, 51_000.0, 100, t0 + 6).await;
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            h.exec_rx.recv(),
        ).await;
        assert!(res.is_err(), "no further fills expected");
    }

    #[tokio::test]
    async fn order_blocked_until_latency_passes() {
        let mut h = spawn_engine(1).await; // 1 ms latency
        let t0 = now_us();

        send_book(&h, 50_000.0, 0, t0).await;
        send_limit(&h, Sell, 50_000.0, 10, 1, t0 + 100).await;

        send_trade(&h, Buy, 50_000.0, 10, t0 + 500).await;

        let early = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            h.exec_rx.recv(),
        )
        .await;

        assert!(early.is_err(), "order must not be released early");

        // advance watermark
        send_book(&h, 50_000.0, 0, t0 + 2_000).await;
        send_trade(&h, Buy, 50_000.0, 10, t0 + 2_001).await;

        let fill = h.exec_rx.recv().await.unwrap();
        assert_eq!(fill.size, 10);
    }
}