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
/// Stream input:
/// A, k    Exponential fit to trade price deviations from midprice (penetration.rs).
/// mid     Mid price, calculated from exchange quotes directly.
/// sigma   Mid price Volatility (volatility.rs).
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
    tick_size: Option<f64>,

    // Model params
    sigma: Option<f64>, // volatility 
    gamma: f64,      // price risk aversion
    delta: f64,      // price risk aversion
    xi: f64,         // execution risk aversion

    // Liquidity params (from market)
    a: Option<f64>,
    k: Option<f64>,

    // Last mid price
    mid: Option<f64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,

    symbol: String,
}

impl AvellanedaStrategy {
    pub fn new(
        symbol: String,
        quote_size: i64,
        max_position: i64,
        gamma: f64,
        delta: f64,
        xi: f64,
    ) -> Self {
        Self {
            position: 0,
            quote_size,
            max_position,
            tick_size: None,
            sigma: None,
            gamma,
            delta,
            xi,
            a: None,
            k: None,
            mid: None,
            best_bid: None,
            best_ask: None,
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
        let sigma = self.sigma?;

        let q = self.position as f64;
        let gamma = self.gamma;
        let delta = self.delta;
        let xi = self.xi;
        
        // c1 = 1 / (xi * delta) * np.log(1 + xi * delta * inv_k)
        // c2 = np.sqrt(np.divide(gamma, 2 * A * delta * k) * ((1 + xi * delta * inv_k) ** (k / (xi * delta) + 1)))
        let c1 = 1.0/(xi* delta)  * (1.0+xi*delta/k).ln();
        let c2 = ( gamma/(2.0*A*delta*k) * (1.0+xi*delta/k).powf(k/(xi*delta) +1.0)).sqrt();
        let half_spread = c1 + 0.5*delta * sigma * c2;
        let skew = sigma*c2;

        Some((half_spread, skew))

    }
    fn quotes(&self) -> Option<(f64, f64)> {
        let mid = self.mid?;

        let q = self.position as f64;

        let (half_spread, skew) = self.half_spread_skew()?;
        println!("Half spread: {}, Skew: {}, mid: {}", half_spread, skew, mid);
        // Optimal half spread
        let bid = mid - (half_spread + skew * q);
        let ask = mid + (half_spread - skew * q);

        Some((bid, ask))
    }
}

impl Strategy for AvellanedaStrategy {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order> {
        let mut orders = Vec::new();

        match update {
            AnyWsUpdate::Quote(q) => {
                // Set tick size
                if self.tick_size.is_none() {
                    self.tick_size = Some(q.tick_size);
                }
                // Update mid price
                match (q.best_bid, q.best_ask) {
                    (Some(bid), Some(ask)) => {
                        self.mid = Some((bid + ask)/2.0);
                        self.best_bid = Some(bid);
                        self.best_ask = Some(ask);
                    },
                    //Only update if both sides are present
                    _ => {}
                }
                // Only post orders on quote update
                return orders;
            }
            AnyWsUpdate::Penetration(p) => {
                // Pull A and k from fit
                if let (Some(a), Some(k)) = (p.fit_A, p.fit_k) {
                    self.a = Some(a);
                    self.k = Some(k);
                }
            }
            AnyWsUpdate::Volatility(v) => {
                // Update sigma
                self.sigma = Some(v.sigma);
                // Only post orders on penetration update
                // return orders;
            }

            _ => {}
        }
        
        // Make sure we have A,k and mid price
        let (bid, ask) = match self.quotes() {
            Some(q) => q,
            None => return orders,
        };
        // Round to tick size
        let tick_size = self.tick_size.unwrap() else {return orders};
        let bid = (bid / tick_size).round() * tick_size;
        let ask = (ask / tick_size).round() * tick_size;

        // Quotes should not cross best bid/ask from exchange
        let bb = self.best_bid.unwrap() else {return orders};
        let ba = self.best_ask.unwrap() else {return orders};
        let bid = bid.max(bb);
        let ask = ask.min(ba);



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
            0.1,
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
        // Set sigma
        strat. on_market(&AnyWsUpdate::Volatility(common::VolatilityUpdate {
            symbol: "XBTUSDT".into(),
            sigma: 0.5,
            timestamp: 0,
        }));
        // Set liquidity params
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
            assert!((ask_p - mid) - (mid - bid_p) < 0.001, "spreads should be symmetric");
        }
    }

    #[test]
    fn inventory_skews_quotes() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10, //quote size
            100,//max position
            0.1,//gamma
            1.0,//delta
            0.1,//xi
        );

        strat.a = Some(100.0);
        strat.k = Some(1.5);
        strat.mid = Some(100.0);
        strat.sigma = Some(1.0);

        strat.position = 5; // long

        let (bid, ask) = strat.quotes().unwrap();
        println!("Bid: {}, Ask: {}", bid, ask);
        assert!(bid < 100.0, "test sanity check");
        assert!(ask > 100.0, "test sanity check");
        assert!(((ask - 100.0) < (100.0 - bid)) , "bid should be further from mid than ask for long inventory");
    
        strat.position = -5; // short 
        let (bid2, ask2) = strat.quotes().unwrap();
        println!("Bid: {}, Ask: {}", bid2, ask2);
        assert!(bid2 < 100.0, "test sanity check");
        assert!(ask2 > 100.0, "test sanity check");
        assert!(((ask2 - 100.0) > (100.0 - bid2)) , "ask should be further from mid than bid for short inventory");
    }
    #[test]
    fn inventory_moves_reservation_price() {
        let mut strat = AvellanedaStrategy::new(
            "XBTUSDT".into(),
            10, //quote size
            100,//max position
            0.1,//gamma
            1.0,//delta
            0.1,//xi
        );

        strat.a = Some(100.0);
        strat.k = Some(1.5);
        strat.mid = Some(100.0);
        strat.sigma = Some(1.0);

        strat.position = 10;
        let (bid1, ask1) = strat.quotes().unwrap();
        strat.position = 20;
        let (bid2, ask2) = strat.quotes().unwrap();
        assert!(bid2 < bid1);
        assert!(ask2 < ask1);
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
