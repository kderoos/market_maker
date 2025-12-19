use crate::traits::Strategy;
use common::{AnyWsUpdate, Order, OrderSide, ExecutionEvent};
use std::time::{SystemTime, UNIX_EPOCH};
/// An implementation of the Avellaneda-Stoikov market making strategy [1] using the approximations introduced
/// by  Gueant et al. [2]. This strategy dynamically sets bid and ask quotes based on inventory risk
/// and market conditions.
///
/// It models the arrival of market orders as a Poisson process, with parameters A (base intensity)
/// and k (decay rate with distance from mid-price). The strategy adjusts quotes to manage inventory
/// and control risk exposure. 
///
/// Parameters A and k are obtained by counting the number of trades that a delta ticks from mid_price
/// over a recent time window, and fitting an exponential decay curve.
///
///[1] https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf
// [2] https://arxiv.org/pdf/1105.3115

// Maintain one order per side
const BID_ID: u64 = 1;
const ASK_ID: u64 = 2;

pub struct AvellanedaStrategy {
    // Inventory
    position: i64,
    max_position: i64,
    quote_size: i64,

    // Model params
    sigma: f64,      // volatility estimate
    gamma: f64,      // price risk aversion
    xi: f64,         // execution risk aversion

    // Liquidity params (from market)
    a: Option<f64>,
    k: Option<f64>,

    // Last mid price
    mid: Option<f64>,

    symbol: String,
}

impl AvellanedaStrategy {
    pub fn new(
        symbol: String,
        quote_size: i64,
        max_position: i64,
        gamma: f64,
        sigma: f64,
        xi: f64,
    ) -> Self {
        Self {
            position: 0,
            quote_size,
            max_position,
            gamma,
            sigma,
            xi,
            a: None,
            k: None,
            mid: None,
            symbol,
        }
    }

    fn now() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    }
    fn half_spread_skew(&self) -> Option<(f64,f64)> {
        let A = self.a?;
        let k = self.k?;        
        let q = self.position as f64;

        let gamma = self.gamma;
        let sigma = self.sigma;

        let c1 = 1.0/gamma * (1.0+gamma/k).ln();
        let c2 = gamma/(2.0*A*k) * (1.0+gamma/k).powf(gamma/k +1.0);
        let half_spread = c1 + (0.5*sigma*sigma +q.abs())*c2;
        let skew = sigma*c2;

        Some((half_spread, skew))

    }
    fn quotes(&self) -> Option<(f64, f64)> {
        let mid = self.mid?;
        let k = self.k?;

        let q = self.position as f64;
        let xi = self.xi as f64;

        let (half_spread, skew) = self.half_spread_skew()?;

        // Optimal half spread
        let bid = mid - (half_spread - skew * q);
        let ask = mid + (half_spread + skew * q);

        Some((bid, ask))
    }
}

impl Strategy for AvellanedaStrategy {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order> {
        let mut orders = Vec::new();

        match update {
            AnyWsUpdate::Quote(q) => {
                // Update mid price
                match (q.best_bid, q.best_ask) {
                    (Some(bid), Some(ask)) => self.mid = Some((bid + ask)/2.0),
                    //Only update if both sides are present
                    _ => {}
                }
            }
            AnyWsUpdate::Penetration(p) => {
                // Pull A and k from fit
                if let (Some(a), Some(k)) = (p.fit_A, p.fit_k) {
                    self.a = Some(a);
                    self.k = Some(k);
                }
            }

            _ => {}
        }
        
        // Make sure we have A,k and mid price
        let (bid, ask) = match self.quotes() {
            Some(q) => q,
            None => return orders,
        };

        // Inventory-aware quoting
        if self.position < self.max_position {
            orders.push(Order::Limit {
                symbol: self.symbol.clone(),
                side: OrderSide::Buy,
                price: bid,
                size: self.quote_size,
                client_id: Some(BID_ID),
            });
        }
        if self.position > -self.max_position {
            orders.push(Order::Limit {
                symbol: self.symbol.clone(),
                side: OrderSide::Sell,
                price: ask,
                size: self.quote_size,
                client_id: Some(ASK_ID),
            });            
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
use common::{QuoteUpdate,PenetrationUpdate};
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_quotes_without_mid_or_liquidity() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10,
            100,
            0.1,
            0.2,
            0.1,
        );

        let orders = strat.on_market(&AnyWsUpdate::Penetration(PenetrationUpdate {
            timestamp: 0,
            symbol: "XBTUSDT".into(),
            counts: vec![],
            fit_A: None,
            fit_k: None,
        }));

        assert!(orders.is_empty());
    }
    #[test]
    fn symmetric_quotes_at_zero_inventory() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10,
            100,
            0.1,
            0.2,
            0.1,
        );
        // Some quotes to set mid price
        strat.on_market(&AnyWsUpdate::Quote(QuoteUpdate {
            base: "XBT".into(),
            quote: "USDT".into(),
            exchange: "TestEx".into(),
            fee: 0.001.to_string(),
            tick_size: 0.01,
            ts_exchange: None,
            ts_received: 0,
            best_bid: Some(100.0),
            best_ask: Some(102.0),
        }));

        let orders = strat.on_market(&AnyWsUpdate::Penetration(PenetrationUpdate {
            timestamp: 0,
            symbol: "XBTUSDT".into(),
            counts: vec![],
            fit_A: Some(10.0),
            fit_k: Some(1.5),
        }));

        assert_eq!(orders.len(), 2);

        let bid = orders.iter().find(|o| matches!(o, Order::Limit { side: OrderSide::Buy, .. })).unwrap();
        let ask = orders.iter().find(|o| matches!(o, Order::Limit { side: OrderSide::Sell, .. })).unwrap();

        if let (Order::Limit { price: bid_p, .. }, Order::Limit { price: ask_p, .. }) = (bid, ask) {
            let mid = 101.0;
            assert!(*bid_p < mid);
            assert!(*ask_p > mid);
        }
    }
    #[test]
    fn inventory_skews_quotes() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10,
            100,
            0.1,
            0.2,
            0.1,
        );

        strat.a = Some(10.0);
        strat.k = Some(1.5);
        strat.mid = Some(100.0);

        strat.position = 50; // long

        let (bid, ask) = strat.quotes().unwrap();

        assert!(bid < 100.0, "long inventory should push bid down");
        assert!(ask > 100.0, "long inventory should push ask up");
    }
    #[test]
    fn max_position_blocks_side() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10,
            50,
            0.1,
            0.2,
            0.1,
        );

        strat.a = Some(10.0);
        strat.k = Some(1.5);
        strat.mid = Some(100.0);
        strat.position = 50; // max long

        let orders = strat.on_market(&AnyWsUpdate::Quote(QuoteUpdate {
            base: "XBT".into(),
            quote: "USDT".into(),
            exchange: "TestEx".into(),
            fee: 0.001.to_string(),
            tick_size: 0.01,
            ts_exchange: None,
            ts_received: 0,
            best_bid: Some(100.0),
            best_ask: Some(102.0),
        }));

        assert!(
            !orders.iter().any(|o| matches!(o, Order::Limit { side: OrderSide::Buy, .. })),
            "should not place bid at max long"
        );
    }

}
