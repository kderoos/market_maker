use crate::traits::Strategy;
use common::{AnyWsUpdate, Order, OrderSide, ExecutionEvent};
use ta::indicators::ExponentialMovingAverage;
use ta::Next;

const BID_ID: u64 = 1;
const ASK_ID: u64 = 2;

pub struct MomentumStrategy {
    ema_fast: ExponentialMovingAverage,
    ema_slow: ExponentialMovingAverage,

    // To detect crossovers
    prev_fast: Option<f64>,
    prev_slow: Option<f64>,

    // Internal position counter
    position: i64,

    // Config
    size: i64,

    // Track periods & warmup counts for EMAs
    fast_period: usize,
    slow_period: usize,
    fast_count: usize,
    slow_count: usize,
}

impl MomentumStrategy {
    pub fn new(fast_period: usize, slow_period: usize, size: i64) -> Self {
        assert!(fast_period < slow_period, "fast_period must be < slow_period");

        Self {
            ema_fast: ExponentialMovingAverage::new(fast_period).unwrap(),
            ema_slow: ExponentialMovingAverage::new(slow_period).unwrap(),
            prev_fast: None,
            prev_slow: None,
            position: 0,
            size,
            fast_period,
            slow_period,
            fast_count: 0,
            slow_count: 0,
        }
    }

    fn is_initialized(&self) -> bool {
        self.fast_count >= self.fast_period && self.slow_count >= self.slow_period
    }
}

impl Strategy for MomentumStrategy {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order> {
        let mut orders = Vec::new();

        if let AnyWsUpdate::Trade(t) = update {
            // Update EMA values
            let fast_now = self.ema_fast.next(t.price);
            let slow_now = self.ema_slow.next(t.price);

            // Track warmup counts
            if self.fast_count < self.fast_period {
                self.fast_count += 1;
            }
            if self.slow_count < self.slow_period {
                self.slow_count += 1;
            }
            
            // We cannot generate signals until both EMAs have filled their warmup period
            if !self.is_initialized() {
                self.prev_fast = Some(fast_now);
                self.prev_slow = Some(slow_now);
                return orders;
            }

            let pf = self.prev_fast.unwrap();
            let ps = self.prev_slow.unwrap();

            let fast_crosses_above = pf <= ps && fast_now > slow_now;
            let fast_crosses_below = pf >= ps && fast_now < slow_now;

            let symbol = t.base.clone() + &t.quote;
            // Trading logic
            if fast_crosses_above && self.position <= 0 {
                orders.push(Order::Limit {
                    symbol: symbol.clone(),
                    side: OrderSide::Buy,
                    price: t.price,
                    size: self.size,
                    client_id: Some(BID_ID),
                });
            }

            if fast_crosses_below && self.position >= 0 {
                orders.push(Order::Limit {
                    symbol: symbol,
                    side: OrderSide::Sell,
                    price: t.price,
                    size: self.size,
                    client_id: Some(ASK_ID),
                });
            }

            // Persist previous values for next crossover detection
            self.prev_fast = Some(fast_now);
            self.prev_slow = Some(slow_now);
        }

        orders
    }

    fn on_fill(&mut self, fill: &ExecutionEvent) {
        if fill.action == "Fill" {
            match fill.side {
                OrderSide::Buy => self.position += fill.size,
                OrderSide::Sell => self.position -= fill.size,
            }
        }
    }
}
