use crate::traits::Strategy;
use common::{StrategyInput,AvellanedaInput,AnyWsUpdate, Order, OrderSide, ExecutionEvent};
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
    // Time
    ts: i64,
    // Inventory
    position: i64,
    max_position: i64,
    quote_size: i64,
    tick_size: f64,

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
        tick_size: f64,
        quote_size: i64,
        max_position: i64,
        gamma: f64,
        delta: f64,
        xi: f64,
    ) -> Self {
        Self {
            ts: 0,
            position: 0,
            quote_size,
            max_position,
            tick_size,
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
        // println!("Half spread: {}, Skew: {}, mid: {}", half_spread, skew, mid);
        // Optimal half spread
        let bid = mid - (half_spread + skew * q);
        let ask = mid + (half_spread - skew * q);

        Some((bid, ask))
    }
}

impl Strategy for AvellanedaStrategy {
    fn on_market(&mut self, update: &StrategyInput) -> Vec<Order> {
        let mut orders = Vec::new();
        println!("Avellaneda Strategy received market update: {:?}", update);
        match update {
            StrategyInput::Avellaneda(av) => {
                self.ts = av.ts_interval as i64;
                self.a = av.A;
                self.k = av.k;
                self.sigma = Some(av.sigma);
                self.best_bid = av.quote.best_bid;
                self.best_ask = av.quote.best_ask;
                // Update mid price
                match (av.quote.best_bid, av.quote.best_ask) {
                    (Some(bid), Some(ask)) => {
                        self.mid = Some((bid + ask)/2.0);
                        self.best_bid = Some(bid);
                        self.best_ask = Some(ask);
                    },
                    //Only update if both sides are present
                    _ => {}
                }
            }
            _ => {}
        }
        
        // Make sure we have A,k and mid price
        let (bid, ask) = match self.quotes() {
            Some(q) => q,
            None => return orders,
        };
        // Round to tick size
        let tick_size = self.tick_size;
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
                ts_received: self.ts,
            });
        }
        if self.position > -self.max_position {
            orders.push(Order::Limit {
                symbol: self.symbol.clone(),
                side: OrderSide::Sell,
                price: ask,
                size: self.quote_size,
                client_id: Some(ASK_ID),
                ts_received: self.ts,
            });            
        }
        // Debug
        println!(
            "Avellaneda Strategy Quotes - Position: {}, Bid: {}, Ask: {}",
            self.position, bid, ask
        );

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
    fn make_quote(ts_received: i64, best_bid: Option<f64>, best_ask: Option<f64>) -> QuoteUpdate {
        QuoteUpdate {
            exchange: "TestExchange".to_string(),
            base: "TEST".to_string(),
            quote: "USD".to_string(),
            tick_size: 0.01,
            fee: "0.001".to_string(),
            best_bid: best_bid,
            best_ask: best_ask,
            ts_received,
            ts_exchange: Some(ts_received - 5),
        }
    }
    fn make_input(
        ts_interval: u64,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        A: Option<f64>,
        k: Option<f64>,
        sigma: f64,
    ) -> StrategyInput {
        StrategyInput::Avellaneda(AvellanedaInput {
            ts_interval,
            quote: make_quote(ts_interval as i64, best_bid, best_ask),
            A,
            k,
            sigma,
        })
    }
    fn new_strat() -> AvellanedaStrategy {
        AvellanedaStrategy::new(
            "XBTUSDT".into(),
            0.01,
            10,
            50,
            0.1,
            0.2,
            0.1,
        )
    }
    #[test]
    fn no_quotes_without_mid_or_liquidity() {
        let mut strat = new_strat();
        let input = make_input(0, None, None, None, None, 0.5);
        let orders = strat.on_market(&input);

        assert!(orders.is_empty());
    }
    #[test]
    fn symmetric_quotes_at_zero_inventory() {
        let mut strat = new_strat();
        let input = make_input(1000, Some(100.0), Some(102.0), Some(10.0), Some(1.5), 0.5);
        let orders = strat.on_market(&input);
        
        // At zero inventory, quotes should be symmetric around mid
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
        let mut strat = new_strat();

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
        let mut strat = new_strat();

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
        let mut strat = new_strat();

        strat.a = Some(10.0);
        strat.k = Some(1.5);
        strat.mid = Some(100.0);
        strat.position = 50; // max long

        let orders = strat.on_market(&make_input(0, Some(100.0), Some(102.0), Some(10.0), Some(1.5), 0.5));

        assert!(
            !orders.iter().any(|o| matches!(o, Order::Limit { side: OrderSide::Buy, .. })),
            "should not place bid at max long"
        );
    }

}
