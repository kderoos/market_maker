use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::{Sender, Receiver};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::book::OrderBook;
use common::{AnyUpdate,TradeUpdate, PenetrationUpdate, AnyWsUpdate};

#[derive(Clone, Debug)]
pub struct MidPrice {
    pub timestamp: i64,
    pub ts_exchange: i64,
    pub symbol: String,
    pub mid_price: f64,
}

pub async fn midprice_sampler(
    tx: Sender<AnyUpdate>, 
    book_state: Arc<RwLock<OrderBook>>, 
    interval_ms: u64,
    symbol: String
) {
    let interval = tokio::time::Duration::from_millis(interval_ms);
    loop {
        {
            let state = book_state.read().unwrap();

            let best_bid = state
                .bids
                .entries
                .values()
                .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

            let best_ask = state
                .asks
                .entries
                .values()
                .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

            if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                let mid = (bid.price + ask.price) / 2.0;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                let mp = MidPrice {
                    timestamp: now,
                    ts_exchange: state.timestamp,
                    symbol: symbol.clone(),
                    mid_price: mid,
                };
                // let _ = tx.send(AnyUpdate(mp));
                // panic!("MidPrice sampler not yet implemented");
                // println!("MidPrice sampler: {:?}", mp);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

// Counts[i] is the number of trades crossed at least i price levels.
// pub type Counts = Vec<u64>; 
#[derive(Clone, Debug, Default)]
pub struct Counts(pub Vec<u64>);

impl Counts {
    pub fn new(size: usize) -> Self {
        Counts(vec![0u64; size])
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn add(&mut self, other: &Counts) {
        if self.len() != other.len() {
            panic!("Penetration Depth: Mismatched histogram sizes in add operation");
        } else {
            for (i, v) in other.0.iter().enumerate() {
                // self.0[i] = self.0[i].saturating_add(*v);
                self.0[i] += *v;
            }
        }

    }
    pub fn sub(&mut self, other : &Counts) {
        if self.len() != other.len() {
            panic!("Penetration Depth: Mismatched histogram sizes in sub operation");
        } else {
            for (i, v) in other.0.iter().enumerate() {
                self.0[i] = self.0[i].saturating_sub(*v);
            }
        }

    }
    pub fn reset(&mut self) {
        for v in self.0.iter_mut() {
            *v = 0;
        }
    }
    pub fn scale(&mut self, scalar: f64)  {
        for v in self.0.iter_mut() {
            *v = (*v as f64 * scalar).round() as u64;
        }
    }
    pub fn as_vec(&self) -> Vec<u64> {
        self.0.clone()
    }
    pub fn count_levels_crossed(&mut self, price_delta: u32) {
        let max_index = { 
            if price_delta as usize <= self.len() {
                price_delta as usize
            } else {
                self.len()-1
            }
        };
        for i in 0..max_index {
            self.0[i] += 1;
        }
    }
}

#[derive(Default,Clone, Debug)]
pub struct TradePricedeltaHist {
    pub timestamp: u64,
    pub midprice_tick: u64,
    pub bins: Counts,
}


impl TradePricedeltaHist {
    pub fn new(num_bins: usize) -> Self {
        Self {
            timestamp: 0,
            midprice_tick : 0,
            bins: Counts::new(num_bins),
        }
    }
    pub fn record(&mut self, price_delta: u32) {
        self.bins.count_levels_crossed(price_delta);
    }
    pub async fn record_trade(&mut self, trade: TradeUpdate) {
        let trade_tick = (trade.price / trade.tick_size).round() as u64;
        let price_delta_ticks = if trade_tick > self.midprice_tick {
            trade_tick - self.midprice_tick
        } else {
            self.midprice_tick - trade_tick
        };
        println!("Recording trade at {} with price {}, midprice tick {}, delta ticks {}", trade.ts_received, trade.price, self.midprice_tick, price_delta_ticks);
        self.record(price_delta_ticks as u32);
    }
    pub fn collect_and_reset(&mut self, new_midprice_tick: u64) -> TradePricedeltaHist {
        let copy_self = self.clone();
        self.midprice_tick = new_midprice_tick; 
        self.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        // println!("new midprice tick set at {} to {}", self.timestamp,new_midprice_tick);
        self.bins.reset();
        // for b in &mut self.bins { *b = 0; }
        copy_self
    }

}   
#[derive(Clone, Debug)]
pub struct PenetrationAggregator{
    pub current: TradePricedeltaHist,
    pub window_len: usize,
    pub count: usize,
    pub data_window: VecDeque<TradePricedeltaHist>,
    pub aggregated_counts: Counts,
}
impl PenetrationAggregator {
    pub fn new(window_len: usize, number_bins: usize) -> Self {
        Self {
            current: TradePricedeltaHist::new(number_bins),
            window_len,
            count: 0,
            data_window: VecDeque::with_capacity(window_len+1),
            aggregated_counts: Counts::new(number_bins),
        }
    }
    pub fn rotate(&mut self, new: TradePricedeltaHist ) -> Option<TradePricedeltaHist>{
        let new_hist_copy = new.clone();
        self.data_window.push_front(new);
        self.aggregated_counts.add(&new_hist_copy.bins);
        self.count += 1;

        if self.count > self.window_len {
            if let Some(old) = self.data_window.pop_back(){
                self.count -= 1;
                self.aggregated_counts.sub(&old.bins); // subtract old from aggregate
                Some(old)
            } else {
                None
            }
        } else {
            None
        }
    }
}
// pub struct PenetrationTracker{
//     // current accumulator (protected by Mutex for simplicity)
//     pub current: Mutex<PenetrationBuckets>,
//     // optional history: last N snapshots
//     pub history: Mutex<VecDeque<PenetrationBuckets>>,
//     pub history_size: usize,
// }

// impl PenetrationTracker {
//     pub fn new(history_size: usize) -> Self {
//         Self {
//             current: Mutex::new(PenetrationBuckets::default()),
//             history: Mutex::new(VecDeque::with_capacity(history_size)),
//             history_size,
//         }
//     }

//     pub fn record_ticks(&self, ticks: i64) {
//         let mut cur = self.current.lock().unwrap();
//         cur.record(ticks);
//     }

//     pub fn snapshot_and_rotate(&self) -> PenetrationBuckets {
//         let mut cur = self.current.lock().unwrap();
//         let snap = cur.clone();
//         cur.reset();
//         let mut hist = self.history.lock().unwrap();
//         hist.push_back(snap.clone());
//         if hist.len() > self.history_size { hist.pop_front(); }
//         snap
//     }
// }

// // need Clone for snapshot push
// impl Clone for PenetrationBuckets {
//     fn clone(&self) -> Self { PenetrationBuckets { bins: self.bins } }
// }
pub async fn engine(
    mut rx: Receiver<AnyUpdate>, 
    tx_ws: Sender<AnyWsUpdate>,
    // mut aggregator: PenetrationAggregator,
    book_state: Arc<RwLock<OrderBook>>, 
    interval_ms: u64,
    symbol: String,
) {
    let mut aggregator = PenetrationAggregator::new(60, 500); // window size 100, 50 bins
    let interval = tokio::time::Duration::from_millis(interval_ms);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    println!("Penetration engine started for symbol {}", symbol);
    loop {
        tokio::select!{
            // Process trade updates
            Ok(update) = rx.recv() => {
                // println!("Penetration engine received update: {:?}", update);
                match update {
                    AnyUpdate::TradeUpdate(trade) => {
                        // if symbol == trade.quote + &trade.base { // remove ?
                        {
                            let mid_price_tick = aggregator.current.midprice_tick;
                            // println!("Penetration engine received trade update: {:?} mid-price:{}", trade,mid_price_tick);
                            // let price_delta = match trade.side.as_str() {
                            //     "Buy" => {
                            //         (trade.price/trade.tick_size) as u64 - mid_price_tick
                            //     } 
                            //     "Sell" => {
                            //         mid_price_tick - (trade.price/trade.tick_size) as u64
                            //     }
                            //     _ => {
                            //         eprintln!("Unknown trade side in penetration engine: {}", trade.side);
                            //         continue;
                            //     }
                            // };
                            // let price_delta = mid_price_tick.abs_diff((trade.price/trade.tick_size) as u64);
                            // aggregator.current.record(price_delta as u32);
                            aggregator.current.record_trade(trade).await;
                        }   
                    }
                    //Ignore other updates
                    _ => { 
                        continue;
                    }
                }
            }
            // Each interval, fetch midprice and rotate aggregator
            _ = ticker.tick() => {
                    let mid_price_tick = {
                        let state = book_state.read().unwrap();
                        state.get_midprice_tick()
                    };
                    // Hadle empty orderbook
                    match mid_price_tick {
                        Some(new_midprice_tick) => { 
                            // println!("Penetration engine: New midprice tick: {}", new_midprice_tick);
                            let last = aggregator.current.collect_and_reset(new_midprice_tick);
                            let depth_snapshot = PenetrationUpdate {
                                timestamp: last.timestamp.clone(),
                                symbol: symbol.clone(),
                                counts: aggregator.aggregated_counts.clone().as_vec(), // Keep Counts implementation local.
                            };

                            // println!("Penetration engine: {} trades received", aggregator.current.bins.0.first().unwrap());
                            // println!("Penetration engine: snapshot {:?}", depth_snapshot);
                            // println!("Aggregator before: {:?}", aggregator);
                            // println!("Collected {} of {} snapshots", aggregator.count, aggregator.window_len);
                            _ = aggregator.rotate(last);
                            // println!("Aggregator after: {:?}", aggregator);

                            let sh = depth_snapshot.counts.clone();
                            let top = sh.iter().take(10).cloned().collect::<Vec<u64>>();
                            // println!("Penetration engine: Sent penetration update with top 10 bins: {:?}", top);
                            
                            let ws_update = AnyWsUpdate::Penetration(depth_snapshot);
                            let _ = tx_ws.send(ws_update);
                        }
                        None => {
                            // eprintln!("Penetration engine: Unable to get midprice for symbol {}", symbol);
                            continue;
                        }
                    };

            }


        }
    }
}
