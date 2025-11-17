use std::collections::HashMap;
use tokio::time::{interval, Duration};
use std::sync::{Arc, RwLock};
// use tokio::sync::mpsc;
use tokio::sync::broadcast;
use common::{AnyUpdate};


// Table of exchange rates: (exchange, baseCurrency, quoteCurrency) -> rate
pub type ExchangeRateTable = Arc<RwLock<HashMap<(String, String, String), RateInstance>>>;

#[derive(Clone)]
pub struct RateInstance {
    rate: f64,
    fee: f64,
}
pub async fn store_rates(mut rx: broadcast::Receiver<AnyUpdate>, rates: ExchangeRateTable) {

    println!("Arbitrage engine starting...");

    while let Ok(update) = rx.recv().await {
        // println!("Received update: {:?}", update);
        match update {
            AnyUpdate::TradeUpdate(trade) => {
                // println!("Trade update received: {:?}", trade);
                if trade.exchange == "bitmex" {
                    println!("Bitmex trade at engine: {}", trade.price);
                }
                continue;
            }
            AnyUpdate::QuoteUpdate(update) => {
                // Process rateTable update
                {
                    let mut table = rates.write().unwrap();
                    let (ex, base, quote) = (update.exchange.clone(), update.base.clone(), update.quote.clone());
                    match (update.best_bid, update.best_ask) {
                        (Some(bid), Some(ask)) => {
                            let rate_bid = RateInstance { rate: bid, fee: update.fee.parse().unwrap_or(0.0) };
                            let rate_ask = RateInstance { rate: 1.0 / ask, fee: update.fee.parse().unwrap_or(0.0) };
                            table.insert((ex.clone(), base.clone(), quote.clone()), rate_bid);
                            table.insert((ex.clone(), quote.clone(), base.clone()), rate_ask);
                        }
                        (Some(bid), None) => {
                            let rate_bid = RateInstance { rate: bid, fee: update.fee.parse().unwrap_or(0.0) };
                            table.insert((ex.clone(), base.clone(), quote.clone()), rate_bid);
                        }
                        (None, Some(ask)) => {
                            let rate_ask = RateInstance { rate: 1.0 / ask, fee: update.fee.parse().unwrap_or(0.0) };
                            table.insert((ex.clone(), quote.clone(), base.clone()), rate_ask);
                        }
                        (None, None) => {
                            continue; // skip invalid update
                        }
                    }
                }
                // else continue to process
            }
            AnyUpdate::BookUpdate(_book) => {
                // Do not process order book updates in this engine.
                continue;
            }
        }
    }
} 

pub async fn check_arb_opp(rates_clone: ExchangeRateTable, interval_ms: u64) {
    let mut ticker = interval(Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        // println!("Cycle detection tick...");
        let snapshot = {
            let table = rates_clone.read().unwrap();
            table.clone()
        };
        // Run arbitrage detection on snapshot
        let start_time = std::time::Instant::now();
        let mut count_cycles = 0;
        for ((ex, base, quote), _rate_first_leg) in snapshot.iter() {
            let mut visited = Vec::new();
            find_cycle_product(&(ex.clone(), base.clone(), quote.clone()), &(ex.clone(), base.clone(), quote.clone()), &mut visited, &snapshot, 1.0, 0,&mut count_cycles);
        }
        let _duration = start_time.elapsed();
        // println!("Checked {} arbitrage cycles in {:?}", count_cycles, duration);
   }
}
fn find_cycle_product(start: &(String, String, String),
                         current: &(String, String, String), 
                         visited: &mut Vec<(String, String, String)>,
                         rates: &HashMap<(String, String, String),
                         RateInstance>, product: f64, depth: usize,
                         count_cycles: &mut usize) {
    if depth > 10 {
        return; // prevent infinite recursion
    }
    if visited.contains(current) {
        *count_cycles += 1;
        if current == start && product > 1.1 && visited.len() > 1 {
            println!("Found cycle: {:?} with product {:.5}", visited, product);
            // for v in visited.iter() {
            //     print!("x{}", rates.get(v).unwrap().rate);
            // }
        }
        return;
    }
    visited.push(current.clone());
    if let Some(_rate_instance) = rates.get(current) {
        for ((ex, base, quote), rate_inst) in rates.iter() {
            if &current.2 == base { // current quote matches next base
                let mut new_visited = visited.clone();
                find_cycle_product(start, &(ex.clone(), base.clone(), quote.clone()), &mut new_visited, rates, product * rate_inst.rate * (1.0 - rate_inst.fee), depth + 1, count_cycles);
            }
        }
    }
    visited.pop();
}
