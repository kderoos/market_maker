//! Volatility estimation based on rolling midprice log-returns.
//!
//! This module provides:
//! - A windowed volatility estimator (`VolatilityEstimator`)
//! - A midprice-driven sampler (`MidpriceVolatilitySampler`)
//! - Async engines for producing midprice and volatility updates
//!
//! Volatility is computed as the square root of the time-normalized
//! variance of log-returns:
//!
//! ```text
//!     σ = sqrt( Var( ln(P_t / P_{t-1}) ) / Δt )
//! ```
//! All timestamps are assumed to be in microseconds.

use common::{AnyUpdate, AnyWsUpdate, VolatilityUpdate,MidPriceUpdate};
use std::collections::VecDeque;
use std::sync::{Arc};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock,broadcast::{Sender, Receiver}};
use crate::book::OrderBook;

/// Number of microseconds per second, used for timestamp normalization.
const MICROS_PER_SECOND: f64 = 1_000_000.0;

/// Rolling-window volatility estimator based on log-returns.
///
/// This estimator maintains a fixed-size window of recent log-returns
/// and computes the annualized (or time-normalized) volatility as:
///
/// ```text
///     σ = sqrt( Var(returns) / dt )
/// ```
///
/// where `dt` is the elapsed time between samples in seconds.
///
/// # Notes
/// - Returns `None` until at least two observations are available.
/// - Does not perform any price sampling; it only consumes prices.
pub struct VolatilityEstimator {
    // Rolling window of log-returns
    window: VecDeque<f64>,
    // Maximum number of returns retained
    window_len: usize,
    // Current volatility estimate
    sigma: Option<f64>,
}
impl VolatilityEstimator {
    /// Create a new volatility estimator with a fixed rolling window size.
    ///
    /// # Arguments
    /// - `window_len`: Number of log-returns retained for variance estimation.
    pub fn new(window_len: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(window_len + 1),
            window_len,
            sigma: None,
        }
    }
    /// Update the estimator with a new midprice observation.
    ///
    /// # Arguments
    /// - `prev_mid`: Previous midprice
    /// - `mid`: Current midprice
    /// - `dt`: Time difference between prices in seconds
    ///
    /// # Returns
    /// - `Some(sigma)` once sufficient data is available
    /// - `None` during warmup or if insufficient observations exist
    ///
    /// # Panics
    /// This function assumes `dt > 0`. Callers must validate timestamps.
    pub fn update(&mut self, prev_mid: f64, mid: f64, dt: f64) -> Option<f64> {
        let r = (mid / prev_mid).ln();
        self.window.push_back(r);

        if self.window.len() > self.window_len {
            self.window.pop_front();
        }

        if self.window.len() >= 2 {
            let mean = self.window.iter().sum::<f64>() / self.window.len() as f64;
            let var = self.window
                .iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>() / self.window.len() as f64;

            self.sigma = Some((var / dt).sqrt());
        }

        self.sigma
    }
}

/// Periodically samples the midprice from the order book and publishes it.
///
/// This task is intentionally simple and stateless. It:
/// - Reads the best bid and ask
/// - Computes the midprice
/// - Emits an `AnyWsUpdate::MidPriceUpdate` message
///
/// Volatility estimation is handled downstream.
pub async fn midprice_sampler(
    tx: Sender<AnyWsUpdate>, 
    book_state: Arc<RwLock<OrderBook>>, 
    interval_ms: u64,
    symbol: String
) {
    let interval = tokio::time::Duration::from_millis(interval_ms);
    loop {
        {
            let state = book_state.read().await;

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
                let mp = MidPriceUpdate {
                    timestamp: now,
                    ts_exchange: state.timestamp,
                    symbol: symbol.clone(),
                    mid_price: mid,
                };
                let _ = tx.send(AnyWsUpdate::MidPrice(mp));
            }
        }

        tokio::time::sleep(interval).await;
    }
}
/// Consumes midprice updates and publishes rolling volatility estimates.
///
/// This engine:
/// - Listens for `AnyWsUpdate::MidPriceUpdate`
/// - Converts midprice streams into volatility estimates
/// - Emits `AnyWsUpdate::Volatility` events
///
/// It encapsulates both sampling state and volatility estimation.
pub async fn volatility_engine(
    mut rx: Receiver<AnyWsUpdate>,
    tx_ws: Sender<AnyWsUpdate>,
    window_len: usize,
    interval_sec: f64,
    symbol: String,
) {
    let mut sampler = MidpriceVolatilitySampler::new(100);

    while let Ok(update) = rx.recv().await {
        if let AnyWsUpdate::MidPrice(mp) = update {
            if let Some(sigma) = sampler.sample(mp.mid_price, mp.timestamp) {
                tx_ws.send(AnyWsUpdate::Volatility(VolatilityUpdate{
                    symbol: mp.symbol.clone(),
                    sigma,
                    timestamp: mp.timestamp,
                })).ok();
            }
        }
    }
}
/// Stateful midprice-driven volatility sampler.
///
/// Makes sure volatility scales correctly with time.
///
/// It tracks the previous midprice and timestamp in order to compute
/// log-returns and time deltas.
pub struct MidpriceVolatilitySampler {
    estimator: VolatilityEstimator,
    last_mid: Option<f64>,
    last_ts: Option<i64>, // microseconds or milliseconds
}
impl MidpriceVolatilitySampler {
    // Create a new sampler with a given volatility window length.
    pub fn new(window_len: usize) -> Self {
        Self {
            estimator: VolatilityEstimator::new(window_len),
            last_mid: None,
            last_ts: None,
        }
    }
    /// Sample a new midprice and update volatility.
    ///
    /// # Arguments
    /// - `mid`: Current midprice
    /// - `timestamp`: Timestamp in microseconds
    ///
    /// # Returns
    /// - `Some(sigma)` when volatility is available
    /// - `None` during warmup or invalid time deltas
    ///
    /// # Notes
    /// - Time deltas are converted to seconds internally
    /// - Non-positive `dt` values are ignored defensively
    pub fn sample(&mut self, mid: f64, timestamp: i64) -> Option<f64> {
        let (prev_mid, prev_ts) = match (self.last_mid, self.last_ts) {
            (Some(m), Some(t)) => (m, t),
            _ => {
                self.last_mid = Some(mid);
                self.last_ts = Some(timestamp);
                return None;
            }
        };

        let dt = (timestamp - prev_ts) as f64 / MICROS_PER_SECOND;
        if dt <= 0.0 {
            return None;
        }

        let sigma = self.estimator.update(prev_mid, mid, dt);

        self.last_mid = Some(mid);
        self.last_ts = Some(timestamp);

        sigma
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    const EPS: f64 = 1e-10;

    fn approx_eq(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    #[test]
    fn test_zero_volatility_constant_price() {
        let mut vol = VolatilityEstimator::new(10);
        let dt = 1.0;

        let mut prev = 100.0;
        for _ in 0..20 {
            let sigma = vol.update(prev, prev, dt);
            prev = prev;
            if let Some(s) = sigma {
                assert!(approx_eq(s, 0.0, EPS));
            }
        }
    }

    #[test]
    fn test_warmup_returns_none() {
        let mut vol = VolatilityEstimator::new(5);
        let dt = 1.0;

        let prices = [100.0, 101.0];

        let s1 = vol.update(prices[0], prices[1], dt);
        assert!(s1.is_none(), "sigma should be None during warmup");
    }

    #[test]
    fn test_known_returns_variance() {
        // Deterministic returns: [+1%, -1%, +1%, -1%]
        let mut vol = VolatilityEstimator::new(4);
        let dt = 1.0;

        let prices = [
            100.0,
            101.0,   // +1%
            99.99,   // -1%
            100.99,  // +1%
            99.98,   // -1%
        ];

        let mut sigma = None;
        for i in 1..prices.len() {
            sigma = vol.update(prices[i - 1], prices[i], dt);
        }

        let sigma = sigma.expect("sigma should be computed");

        // Expected variance of log-returns
        let log_returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] / w[0]).ln())
            .collect();
        let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let var = log_returns.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / log_returns.len() as f64;
        let expected_sigma = var.sqrt();

        assert!(
            approx_eq(sigma, expected_sigma, 1e-6),
            "expected {}, got {}",
            expected_sigma,
            sigma
        );
    }

    #[test]
    fn test_dt_scaling() {
        // Same returns, different dt → sigma should scale as 1/sqrt(dt)
        let mut vol1 = VolatilityEstimator::new(10);
        let mut vol2 = VolatilityEstimator::new(10);

        let prices = [100.0, 101.0, 100.0, 101.0, 100.0];

        let mut s1 = None;
        let mut s2 = None;

        for i in 1..prices.len() {
            s1 = vol1.update(prices[i - 1], prices[i], 1.0);
            s2 = vol2.update(prices[i - 1], prices[i], 0.25);
        }

        let sigma1 = s1.unwrap();
        let sigma2 = s2.unwrap();

        // sigma ∝ 1/sqrt(dt)
        let expected_ratio = (1.0_f64 / 0.25_f64 ).sqrt();
        let ratio = sigma2 / sigma1;

        assert!(
            approx_eq(ratio, expected_ratio, 1e-6),
            "expected ratio {}, got {}",
            expected_ratio,
            ratio
        );
    }

    #[test]
    fn test_no_nan_or_negative_sigma() {
        let mut vol = VolatilityEstimator::new(20);
        let dt = 1.0;

        let prices = [
            100.0, 100.5, 99.8, 100.2, 100.1, 99.9, 100.0,
        ];

        for i in 1..prices.len() {
            if let Some(sigma) = vol.update(prices[i - 1], prices[i], dt) {
                assert!(!sigma.is_nan());
                assert!(sigma >= 0.0);
            }
        }
    }
    #[test]
    fn test_sampler_constant_price() {
        let mut sampler = MidpriceVolatilitySampler::new(5);

        let t0 = 1_000_000;
        let mut sigma = None;

        for i in 0..10 {
            sigma = sampler.sample(100.0, t0 + i * 1_000_000);
        }

        assert!(sigma.unwrap() == 0.0);
    }

}
