use std::collections::VecDeque;
use std::sync::{Arc};
use tokio::sync::{RwLock,broadcast::{Sender, Receiver}};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::book::OrderBook;
use nalgebra::{DMatrix, DVector};
use crate::regression::{SimpleSLR, RegressionEngine};
use common::{AnyUpdate,TradeUpdate, PenetrationUpdate, AnyWsUpdate};

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
    pub A: Option<f64>,
    pub k: Option<f64>,
}
impl PenetrationAggregator {
    pub fn new(window_len: usize, number_bins: usize) -> Self {
        Self {
            current: TradePricedeltaHist::new(number_bins),
            window_len,
            count: 0,
            data_window: VecDeque::with_capacity(window_len+1),
            aggregated_counts: Counts::new(number_bins),
            A: None,
            k: None,
        }
    }
    pub fn rotate_aggregate(&mut self, new: TradePricedeltaHist ) -> Option<TradePricedeltaHist>{
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
    pub fn fit_exponential(&mut self) -> Result<(f64,f64), String> {
        let mut slr = SimpleSLR::from(&self.aggregated_counts);
        slr.fit()?;
        let (A,k) = slr.to_exp_params()?;
        self.A = Some(A);
        self.k = Some(k);
        Ok((A,k))
    }

}
use tokio::sync::mpsc;
pub async fn engine(
    mut rx: mpsc::Receiver<AnyUpdate>, 
    tx_ws: mpsc::Sender<PenetrationUpdate>,
    // mut aggregator: PenetrationAggregator,
    // book_state: Arc<RwLock<OrderBook>>, 
    window_len: usize,
    num_bins: usize,
    interval_ms: i64,
    symbol: String,
) {
    let mut aggregator = PenetrationAggregator::new(window_len, num_bins); // window size 100, 50 bins
    let mut next_interval_ts: Option<i64> = None;
    let mut last_midprice_tick: Option<u64> = None;

    let us_per_ms = 1000;
    let interval_us = interval_ms * us_per_ms;


    while let Some(update) = rx.recv().await {
        match update {
            AnyUpdate::TradeUpdate(trade) => {
                let mid_price_tick = match last_midprice_tick {
                    Some(tick) => tick,
                    None => {
                        // Skip trade if no midprice available yet
                        continue;
                    }
                };
                // record trade penetration
                aggregator.current.midprice_tick = mid_price_tick;
                aggregator.current.record_trade(trade.clone()).await;

                // Initialize clock if first event was trade
                if next_interval_ts.is_none() {
                    let interval_start = 
                        ( trade.ts_received / interval_us ) * interval_us;
                    next_interval_ts = Some(interval_start + interval_us);
                }

                // Catch up across all crossed intervals
                while trade.ts_received >= next_interval_ts.unwrap() {
                    let mut last = aggregator.current.collect_and_reset(mid_price_tick);
                    last.timestamp = next_interval_ts.unwrap() as u64;
                    next_interval_ts = Some( next_interval_ts.unwrap() + interval_us);

                    _ = aggregator.rotate_aggregate(last.clone());

                    if let Ok((A,k)) = aggregator.fit_exponential(){
                        let pen_update = PenetrationUpdate {
                            timestamp: last.timestamp.clone(),
                            symbol: symbol.clone(),
                            counts: aggregator.aggregated_counts.clone().as_vec(), // Keep Counts implementation local.
                            fit_A: Some(A),
                            fit_k: Some(k),
                        };
                        let _ = tx_ws.send(pen_update).await;
                    }
                }
            }
            AnyUpdate::QuoteUpdate(quote) => {
                if let (Some(bid), Some(ask)) = (quote.best_bid, quote.best_ask) {
                    let midprice = (bid + ask) / 2.0;
                    let midprice_tick = (midprice / quote.tick_size).round() as u64;
                    last_midprice_tick = Some(midprice_tick);

                    // Initialize next_interval_ts on first quote or first trade
                    if next_interval_ts.is_none() {
                        let interval_start = 
                            ( quote.ts_received / interval_us ) * interval_us;
                        next_interval_ts = Some(interval_start + interval_us);
                    }

                }
            }
            _ => {
                continue;   
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
            if count > 0 { //skip zero counts to avoid ln(0)
                let x = i as f64;
                let c = (count as f64).ln(); //linearize like c -> ln(c) 

                y_data.push(c);
                x_data.push(1.0); //intercept
                x_data.push(x);
            }

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
            Err("SimpleSLR: Fit model first".to_string())
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
    #[test]
    fn test_lsr_beta() {
        let counts = Counts(vec![100, 50, 25, 12, 6]);
        let mut slr = SimpleSLR::from(&counts);
        match slr.fit() {
            Ok(beta) => {
                println!("Fitted beta: {:?}", beta);
                assert!(beta.len() == 2);
                assert!(!beta[0].is_nan());
                assert!(!beta[1].is_nan());
            }
            Err(e) => {
                println!("SLR fit failed: {}", e);
                assert!(false);
            }
        }
    }
    #[test]
    fn test_slr_exp() {
        let (A,k) = (1000.0, 0.5);
        let counts = Counts((0..5).map(|x| ((A * (-k * x as f64).exp()).round() as u64 )).collect());
        let mut slr = SimpleSLR::from(&counts);
        match slr.fit() {
            Ok(beta) => {
                println!("Fitted beta: {:?}", beta);
            }
            Err(e) => {
                println!("SLR fit failed: {}", e);
                assert!(false);
            }
        }
        let (A_est, k_est) = slr.to_exp_params().unwrap();

        println!("True A: {}, k: {}", A, k);
        println!("Estimated A: {}, k: {}", A_est, k_est);
        assert!((A - A_est).abs() / A < 0.1);
        assert!((k - k_est).abs() / k < 0.1);
    }
}
