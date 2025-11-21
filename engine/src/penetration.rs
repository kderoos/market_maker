use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::{Sender, Receiver};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::book::OrderBook;
use nalgebra::{DMatrix, DVector};
use crate::regression::{SimpleSLR, RegressionEngine};
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
                panic!("MidPrice sampler not yet implemented");
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
        self.bins.reset();
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

pub async fn engine(
    mut rx: Receiver<AnyUpdate>, 
    tx_ws: Sender<AnyWsUpdate>,
    // mut aggregator: PenetrationAggregator,
    book_state: Arc<RwLock<OrderBook>>, 
    window_len: usize,
    num_bins: usize,
    interval_ms: u64,
    symbol: String,
) {
    let mut aggregator = PenetrationAggregator::new(window_len, num_bins); // window size 100, 50 bins
    let interval = tokio::time::Duration::from_millis(interval_ms);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    println!("Penetration engine started for symbol {}", symbol);
    loop {
        tokio::select!{
            // Process trade updates
            Ok(update) = rx.recv() => {
                match update {
                    AnyUpdate::TradeUpdate(trade) => {
                        let mid_price_tick = aggregator.current.midprice_tick;
                        aggregator.current.record_trade(trade).await;
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
                    // Handle empty orderbook
                    match mid_price_tick {
                        Some(new_midprice_tick) => { 
                            let last = aggregator.current.collect_and_reset(new_midprice_tick);
                            
                            _ = aggregator.rotate(last);
                            
                            let depth_snapshot = PenetrationUpdate {
                                timestamp: last.timestamp.clone(),
                                symbol: symbol.clone(),
                                counts: aggregator.aggregated_counts.clone().as_vec(), // Keep Counts implementation local.
                            };


                            let ws_update = AnyWsUpdate::Penetration(depth_snapshot);
                            let _ = tx_ws.send(ws_update);
                        }
                        None => {
                            continue;
                        }
                    };

            }


        }
    }
}
impl From<&Counts> for SimpleSLR {
    //y = X*beta
    fn from(counts: &Counts)-> Self{
        let n = counts.len();
        let mut y_data = Vec::with_capacity(n);
        let mut x_data = Vec::with_capacity(n*2); //2 columns: intercept and x values
        for (i, &count) in counts.0.iter().enumerate(){
            let x = i as f64;
            let c = (count as f64 + 1.0).ln(); //linearize like c -> ln(1+c)  so c can be 0. 

            y_data.push(c.ln());
            x_data.push(1.0); //intercept
            x_data.push(x);
        }
        let y = DVector::from_vec(y_data);
        let x = DMatrix::from_row_slice(y.len(), 2, &x_data);
        
        SimpleSLR{
            Y:y,
            X:x,
            beta: None,
        }
    }
}
impl SimpleSLR {
    pub fn to_exp_params(&self) -> Result<(f64,f64), String> {
        if let Some(beta) = &self.beta {
            let A = beta[0].exp();
            let k = -beta[1];
            Ok((A,k))
        } else {
            Err("SimpleSLR: Model not fitted yet".to_string())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_counts_add_sub() {
        let mut c1 = Counts::new(5);
        let mut c2 = Counts::new(5);
        c1.0 = vec![1, 2, 3, 4, 5];
        c2.0 = vec![5, 4, 3, 2, 1];
        c1.add(&c2);
        assert_eq!(c1.0, vec![6, 6, 6, 6, 6]);
        c1.sub(&c2);
        assert_eq!(c1.0, vec![1, 2, 3, 4, 5]);
    }

    fn test_lsr_fit() {
        let counts = Counts(vec![100, 50, 25, 12, 6]);
        let mut slr = SimpleSLR::from(&counts);
        let beta = slr.fit();
        println!("Fitted beta: {:?}", beta);
        assert!(beta.len() == 2);
    }
    fn test_slr_ak_fit() {
        let (A,k) = (1000.0, 0.5);
        let counts = Counts((0..5).map(|x| ((A * (-k * x as f64).exp()).round() as u64 )).collect());
        let mut slr = SimpleSLR::from(&counts);
        slr.fit();
        let (A_est, k_est) = slr.to_exp_params().unwrap();

        println!("True A: {}, k: {}", A, k);
        println!("Estimated A: {}, k: {}", A_est, k_est);
        assert!((A - A_est).abs() / A < 0.1);
        assert!((k - k_est).abs() / k < 0.1);
    }
}
