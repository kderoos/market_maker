use common::{
    ExecutionEvent,AnyUpdate, BookEntry, BookUpdate, Order, OrderSide, TradeUpdate,
};
use crate::book::OrderBook;
use tokio::sync::mpsc::{Receiver, Sender,channel};
use crate::execution::ExecutionState;

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
    mut rx_ws: Receiver<AnyUpdate>,
    mut order_rx: Receiver<Order>,
    exec_tx: Sender<ExecutionEvent>,
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
                        apply_book_update(book_update,&mut engine.orderbook);
                        engine.book_watermark_ts = engine.orderbook.timestamp;
                    }

                    AnyUpdate::TradeUpdate(trade) => {
                        engine.book_watermark_ts =
                            engine.book_watermark_ts.max(trade.ts_received);

                        let fills = engine.exec.on_trade(trade, &engine.orderbook).await;
                        for fill in fills {
                            let _ = exec_tx.send(fill);
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
                        unimplemented!("Market orders not supported yet");
                    }
                    Order::Limit { ts_received, .. } => {
                        engine.pending_orders.push_back((*ts_received, order));
                    }
                    _ => {}
                }
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
            engine.exec.replace_limit(order, &engine.orderbook).await;
        } else {
            break;
        }
    }
}
#[cfg(test)]
mod deterministic_tests {
    use super::*;
    use tokio::sync::broadcast;
    use chrono::Utc;

    const EX_LATENCY: i64 = 1_000; // 1 ms

    fn now_us() -> i64 {
        Utc::now().timestamp_micros()
    }

    #[tokio::test]
    async fn order_is_blocked_until_book_latency_passes() {
        let (ws_tx, ws_rx) = channel(16);
        let (order_tx, order_rx) = channel(16);
        let (exec_tx, mut exec_rx) = channel(16);

        // Spawn deterministic engine
        tokio::spawn(run_deterministic(
            ws_rx,
            order_rx,
            exec_tx,
            EX_LATENCY,
        ));

        let t0 = now_us();

        //Send BOOK UPDATE at t0
        ws_tx.send(AnyUpdate::BookUpdate(common::BookUpdate {
            exchange: "test".into(),
            symbol: "XBTUSDT".into(),
            action: "insert".into(),
            tick_size: 0.01,
            data: vec![(
                "ask".into(),
                BookEntry {
                    side: "Sell".into(),
                    price: 50_000.0,
                    size: 0,
                },
            )],
            ts_exchange: None,
            ts_received: t0,
        }))
        .await.unwrap();

        // send ORDER (too fresh, must be blocked)
        let order_ts = t0 + 100; // 100 µs after book
        order_tx
            .send(Order::Limit {
                symbol: "XBTUSDT".into(),
                side: OrderSide::Sell,
                price: 50_000.0,
                size: 10,
                client_id: Some(1),
                ts_received: order_ts,
            })
            .await.unwrap();

        // Trade arrives BEFORE latency window expires
        ws_tx.send(AnyUpdate::TradeUpdate(TradeUpdate {
            exchange: "test".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: OrderSide::Buy,
            price: 50_000.0,
            size: 10,
            ts_exchange: None,
            ts_received: t0 + 500, // < EX_LATENCY
        }))
        .await.unwrap();

        // No fill must happen
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            exec_rx.recv(),
        )
        .await;

        assert!(
            res.is_err(),
            "order must NOT fill before latency expires"
        );

        // Advance book watermark beyond latency
        ws_tx.send(AnyUpdate::BookUpdate(common::BookUpdate {
            exchange: "test".into(),
            symbol: "XBTUSDT".into(),
            action: "update".into(),
            tick_size: 0.01,
            data: vec![],
            ts_exchange: None,
            ts_received: t0 + EX_LATENCY + 1,
        }))
        .await.unwrap();

        // Trade AFTER release
        ws_tx.send(AnyUpdate::TradeUpdate(TradeUpdate {
            exchange: "test".into(),
            base: "XBT".into(),
            quote: "USDT".into(),
            tick_size: 0.01,
            side: OrderSide::Buy,
            price: 50_000.0,
            size: 10,
            ts_exchange: None,
            ts_received: t0 + EX_LATENCY + 2,
        }))
        .await.unwrap();

        // Now fill MUST happen
        let fill = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            exec_rx.recv(),
        )
        .await
        .expect("expected fill")
        .expect("exec channel closed");

        assert_eq!(fill.size, 10);
        assert_eq!(fill.price, 50_000.0);
    }
}

