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
    size: i64,

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
        size: i64,
        gamma: f64,
        sigma: f64,
        xi: f64,
    ) -> Self {
        Self {
            position: 0,
            size,
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
        let A = self.a?
        let k = self.k?        

        let gamma = self.gamma;
        let sigma = self.sigma;

        let c1 = 1/gamma * (1+gamma/k).ln();
        let c2 = gamma/(2*A*k) * (1+gamma/k).powf(gamma/k +1);
        let half_spread = c1 + (1/2*sigma +q)*c2;
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
                // self.mid = Some((q.best_bid + q.best_ask)/2.0);
                match (q.best_bid, q.best_ask) {
                    (Some(bid), Some(ask)) => self.mid = Some((bid + ask)/2.0),
                    (Some(bid), None) => self.mid = None,
                    (None, Some(ask)) => self.mid = None,
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
        
        // We need mid + k before quoting
        let (bid, ask) = match self.quotes() {
            Some(q) => q,
            None => return orders,
        };

        // Inventory-aware quoting
        orders.push(Order::Limit {
            symbol: self.symbol.clone(),
            side: OrderSide::Sell,
            price: bid,
            size: self.size,
            client_id: Some(BID_ID),
        });

        orders.push(Order::Limit {
            symbol: self.symbol.clone(),
            side: OrderSide::Buy,
            price: ask,
            size: self.size,
            client_id: Some(ASK_ID),
        });

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
