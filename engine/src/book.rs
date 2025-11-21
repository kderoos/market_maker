use std::sync::{Arc, RwLock};
// use tokio::sync::mpsc;
use tokio::sync::broadcast;
use common::{AnyUpdate, BookEntry,BookUpdate};
use std::collections::{HashMap}; 

// use std::any::type_name;

// type BTreeOrderBook = OrderBook<BTreeMap<i64, BookEntry>>;
// type HashOrderBook = OrderBook<HashMap<i64, BookEntry>>;
// pub type OrderBook = HashMap<String, BookEntry>; // key: "symbol:id"

pub struct OrderBookSide {
    // pub entries: BTreeMap<i64,BookEntry>,// price*100 as key
    pub entries: HashMap<i64,BookEntry>,// price*100 as key
}

pub struct OrderBook{
    pub timestamp: i64,
    pub bids: OrderBookSide, // sorted descending
    pub asks: OrderBookSide, // sorted ascending
}
impl OrderBook {
    pub fn get_midprice_tick(&self) -> Option<u64> {
        let best_bid_tick = self
            .bids
            .entries
            .keys()
            .max()
            .copied();
        let best_ask_tick = self
            .asks
            .entries
            .keys()
            .min()
            .copied();

        match (best_bid_tick, best_ask_tick) {
            (Some(bid), Some(ask)) => Some((bid + ask).wrapping_div(2) as u64),
            _ => None,
        }
    } 
}
pub async fn update_book_state(book_update: BookUpdate, book_state: Arc<RwLock<OrderBook>>) {
        // println!("Book update received: action: {}, entries: {}", book_update.action, book_update.data.len());
        // Process BookUpdate messages
        {
            let mut state = book_state.write().unwrap();
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
        // let elapsed = start.elapsed().as_nanos();
        // count += 1;
        // avg_time = ((avg_time * (count - 1)) + elapsed) / count;
        // println!("Book {} processed in {:?} (av: {})", book_update.action.as_str(), elapsed, avg_time);
    }
pub async fn pub_book_depth(tx_ws: broadcast::Sender<common::AnyWsUpdate>, book_state: Arc<RwLock<OrderBook>>) {
    println!("Book depth publisher starting...");
    loop {
        {
            let len = 100;
            let state = book_state.read().unwrap();

            let top_cum_bids = top_n_with_padding(&state.bids.entries, len, true)
                .iter()
                .scan(0u64, |sum, (price, size)| {
                    *sum += *size as u64;
                    Some((*price, *sum))
                })
                .collect::<Vec<(f64, u64)>>();
            let top_cum_asks = top_n_with_padding(&state.asks.entries, len, false)
                .iter()
                .scan(0u64, |sum, (price, size)| {
                    *sum += *size as u64;
                    Some((*price, *sum))
                })
                .collect::<Vec<(f64, u64)>>();

            let snapshot = common::DepthSnapshot {
                len: len as u32,
                bid: top_cum_bids,
                ask: top_cum_asks,
                timestamp: state.timestamp,
            };
            if snapshot.timestamp > 0 {
                let depth_update = common::AnyWsUpdate::Depth(snapshot);
                let _ = tx_ws.send(depth_update);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}
fn top_n_with_padding(entries: &HashMap<i64, BookEntry>, n: usize, descending: bool) -> Vec<(f64, i64)> {
    let mut levels: Vec<_> = entries.values().collect();
    if descending {
        levels.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
    } else {
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
    }
    let mut result: Vec<(f64, i64)> = levels.iter().take(n).map(|e| (e.price, e.size)).collect();
    // Pad with zeros if less than n
    while result.len() < n {
        result.push((0.0, 0));
    }
    result
}

pub async fn book_engine(mut rx: broadcast::Receiver<AnyUpdate>, book_state: Arc<RwLock<OrderBook>>) {
    println!("Book engine starting...");
    // let mut count: u128 = 0;
    // let mut avg_time: u128 = 0;
    while let Ok(update) = rx.recv().await {
        if let AnyUpdate::BookUpdate(book_update) = update {
            // println!("Book engine received update with {} entries, action: {}", book_update.data.len(), book_update.action);
            // let start = std::time::Instant::now();
            update_book_state(book_update, book_state.clone()).await;
            let state = book_state.read().unwrap();
            // println!("Book has {} bids and {} asks",state.bids.entries.len(),state.asks.entries.len());
            let best_bid = top_n_with_padding(&state.bids.entries,1,true);
            // println!("Best bid price: {} and size: {}",best_bid[0].0,best_bid[0].1);
            // let elapsed = start.elapsed().as_nanos();
            // count += 1;
            // avg_time = ((avg_time * (count - 1)) + elapsed) / count;
            // println!("Book engine processed update in {:?} (av:{:?})", elapsed, avg_time); 
            }
    }
}
pub async fn print_book(book_state: Arc<RwLock<OrderBook>>) {
    let mut count: u128 = 0;
    let mut avg_time: u128 = 0;
    loop {
        let start= std::time::Instant::now();
        { //HashMap book print
            let state = book_state.read().unwrap();
            println!("Order Book Snapshot:");

            // Asks: sort ascending by price
            let mut asks: Vec<_> = state.asks.entries.values().collect();
            asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
            println!("Asks:");
            for entry in asks.iter().take(5) {
                println!("Price: {}, Size: {}", entry.price, entry.size);
            }
            // Bids: sort descending by price
            let mut bids: Vec<_> = state.bids.entries.values().collect();
            bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
            println!("Bids:");
            for entry in bids.iter().take(5) {
                println!("Price: {:.1}, Size: {}", entry.price, entry.size);
            }

        }

        let elapsed = start.elapsed().as_nanos();
        count += 1;
        avg_time = ((avg_time * (count - 1)) + elapsed) / count;
        println!("Top-of-book extraction took {:?} (av:{:?})", elapsed, avg_time);

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn init_book() -> OrderBook {
        OrderBook {
            timestamp: 0,
            bids: OrderBookSide { entries: HashMap::new() },
            asks: OrderBookSide { entries: HashMap::new() },
        }
    }
    fn mock_book_entry(side: &str, price: f64, size: i64) -> (String, BookEntry) {
        let entry = BookEntry {
            side: side.to_string(),
            price,
            size,
        };
        let id = format!("{}-{}", side, (price*100.0) as i64);
        (id, entry)
    }
    #[tokio::test]
    async fn test_book_engine_insert_and_delete() {
        let book_state = Arc::new(RwLock::new(init_book()));
        let (tx, rx) = broadcast::channel(10);
        let book_state_clone = book_state.clone();
        tokio::spawn(book_engine(rx, book_state_clone));

        // Insert entries
        let insert_update = AnyUpdate::BookUpdate(common::BookUpdate {
            exchange: "test_ex".to_string(),
            symbol: "TEST".to_string(),
            action: "insert".to_string(),
            tick_size: 0.01,
            data: vec![
                mock_book_entry("Buy", 100.0, 10),
                mock_book_entry("Sell", 101.0, 15),
                mock_book_entry("Buy", 99.5, 5),
            ],
            ts_exchange: None,
            ts_received: 123456789000000,
        });
        tx.send(insert_update).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait for processing

        {
            let state = book_state.read().unwrap();
            assert_eq!(state.bids.entries.len(), 2);
            assert_eq!(state.asks.entries.len(), 1);
            assert_eq!(state.bids.entries.get(&10000).unwrap().size, 10);
            assert_eq!(state.asks.entries.get(&10100).unwrap().size, 15);
        }

        // Delete an entry
        let delete_update = AnyUpdate::BookUpdate(common::BookUpdate {
            exchange: "test_ex".to_string(),
            symbol: "TEST".to_string(),
            action: "delete".to_string(),
            tick_size: 0.01,
            data: vec![
                mock_book_entry("Buy", 100.0, 0), // size is irrelevant for delete
            ],
            ts_exchange: None,
            ts_received: 2,
        });
        tx.send(delete_update).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait for processing

        {
            let state = book_state.read().unwrap();
            assert_eq!(state.bids.entries.len(), 1);
            assert!(state.bids.entries.get(&10000).is_none());
            assert_eq!(state.bids.entries.get(&9950).unwrap().size, 5);
        }
    }
}
// pub async fn print_book_btree(book_state: Arc<RwLock<OrderBook>>) {
//     let mut count: u128 = 0;
//     let mut avg_time: u128 = 0;
//     loop {
//         let start= std::time::Instant::now();
//         {
//             let state = book_state.read().unwrap();
//             println!("Order Book Snapshot:");
//             println!("Bids:");
//             for (_, entry) in state.bids.entries.iter().rev().take(5) {
//                 println!("Price: {:.1}, Size: {}", entry.price, entry.size);
//             }
//             println!("Asks:");
//             for (_, entry) in state.asks.entries.iter().take(5) {
//                 println!("Price: {}, Size: {}", entry.price, entry.size);
//             }
//         }
//         let elapsed = start.elapsed().as_nanos();
//         count += 1;
//         avg_time = ((avg_time * (count - 1)) + elapsed) / count;
//         println!("Top-of-book extraction took {:?} (av:{:?})", elapsed, avg_time);

//         tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
//     }
// }