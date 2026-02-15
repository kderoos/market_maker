use common::{VolatilityUpdate, PenetrationUpdate, QuoteUpdate, StrategyInput,AvellanedaInput};
use tokio::sync::{mpsc,mpsc::error};
use std::collections::BTreeMap;
use tracing::{info};
// use anyhow::Result;

#[derive(Debug, Clone)]
struct SequencerState {
    interval_ms: i64,
    vol: BTreeMap<i64, f64>,                        // (interval, sigma)
    pen: BTreeMap<i64, (Option<f64>, Option<f64>)>, // (interval, (A, k))
    latest_quote: BTreeMap<i64, (i64, QuoteUpdate)>,// (interval, (ts_received, Quote))
    last_emitted: Option<i64>,
}
fn prune_below_frontier(state: &mut SequencerState) {
    let frontier = match state.last_emitted {
        Some(t) => t + state.interval_ms,
        None => return,
    };

    state.vol.retain(|&k, _| k >= frontier);
    state.pen.retain(|&k, _| k >= frontier);
    state.latest_quote.retain(|&k, _| k >= frontier);
}
pub fn try_emit(state: &mut SequencerState) -> Option<StrategyInput> {
    for (&interval, &sigma) in state.vol.iter() {
        let (A, k) = match state.pen.get(&interval) {
            Some(&(A, k)) => (A, k),
            None => continue,
        };

        let (_, q) = match state.latest_quote.get(&interval) {
            Some(v) => v,
            None => continue,
        };

        let output = StrategyInput::Avellaneda(AvellanedaInput {
            ts_interval: interval as u64,
            quote: q.clone(),
            sigma,
            A,
            k,
        });

        state.vol.remove(&interval);
        state.pen.remove(&interval);
        state.latest_quote.remove(&interval);
        state.last_emitted = Some(interval);

        // Keep state small and try_emit efficient.
        prune_below_frontier(state);

        return Some(output);
    }

    None
}

pub async fn sequencer_run(
    interval_ms: i64,
    mut vol_rx: mpsc::Receiver<VolatilityUpdate>,
    mut pen_rx: mpsc::Receiver<PenetrationUpdate>,
    mut quote_rx: mpsc::Receiver<QuoteUpdate>,
    strategy_tx: mpsc::Sender<StrategyInput>,
) -> Result<(), error::SendError<StrategyInput>> {
    let mut state = SequencerState {
        interval_ms,
        vol: BTreeMap::new(),
        pen: BTreeMap::new(),
        latest_quote: BTreeMap::new(),
        last_emitted: None,
    };
    let mut vol_interval_checked = false;
    let mut pen_interval_checked = false;
    let us_per_ms = 1000;

    loop {
        tokio::select! {
            msg = vol_rx.recv() => {
                match msg {
                    Some(v) => {
                        state.vol.insert(v.timestamp, v.sigma);
                        // Check interval alignment only once
                        if !vol_interval_checked {
                            let expected_interval = v.timestamp % interval_ms;
                            if expected_interval != 0 {
                                panic!("Volatility update timestamp {} is not aligned with interval_ms {}", v.timestamp, interval_ms);
                            }
                            vol_interval_checked = true;
                        }
                    }
                    None => {
                        info!("Volatility feed ended..");
                        break;
                    }
                }
            }

            Some(p) = pen_rx.recv() => {
                state.pen.insert(p.timestamp as i64, (p.fit_A, p.fit_k));
                // Check interval alignment only once
                if !pen_interval_checked {
                    let expected_interval = p.timestamp as i64 % interval_ms;
                    if expected_interval != 0 {
                        panic!("Penetration update timestamp {} is not aligned with interval_ms {}", p.timestamp, interval_ms);
                    }
                    pen_interval_checked = true;
                }
            }
            Some(q) = quote_rx.recv() => {
                // Ceiling to nearest interval
                let interval_id = q.ts_received + (state.interval_ms * us_per_ms) - (q.ts_received % (state.interval_ms * us_per_ms));
                if let Some((existing_ts, _)) = state.latest_quote.get(&interval_id) {
                    // Update only if newer quote
                    if q.ts_received > *existing_ts {
                        state.latest_quote.insert(interval_id, (q.ts_received, q));
                    }
                } else {
                    // No existing quote, insert new
                    state.latest_quote.insert(interval_id, (q.ts_received, q));
                }
                
            }
        }
        while let Some(output) = try_emit(&mut state) {
            strategy_tx.send(output.clone()).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state() -> SequencerState {
        SequencerState {
            interval_ms: 10,
            vol: BTreeMap::new(),
            pen: BTreeMap::new(),
            latest_quote: BTreeMap::new(),
            last_emitted: None,
        }
    }
    fn make_quote(ts_received: i64, best_bid: f64, best_ask: f64) -> QuoteUpdate {
        QuoteUpdate {
            exchange: "TestExchange".to_string(),
            base: "TEST".to_string(),
            quote: "USD".to_string(),
            best_bid: Some(best_bid),
            best_ask: Some(best_ask),
            ts_received,
            ts_exchange: Some(ts_received - 5),
            tick_size: 0.01,
            fee: "0.001".to_string(),
        }
    }

    #[test]
    fn emits_latest_quote_not_from_future() {
        let mut state = make_state();

        // vol-90 arrives
        state.vol.insert(90, 0.5);

        // pen-90 arrives
        state.pen.insert(90, (Some(1.2), Some(0.8)));

        // quotes arrive out of order (future first)
        state.latest_quote.insert(90, (91, make_quote(91, 80.0, 80.0)));
        state.latest_quote.insert(100, (105, make_quote(105, 105.0, 105.0)));
        state.latest_quote.insert(90, (99, make_quote(99, 99.0, 99.0)));
        state.latest_quote.insert(150, (150, make_quote(150, 150.0, 150.0)));

        if let StrategyInput::Avellaneda(out) = try_emit(&mut state).expect("should emit") {

            // The key assertion:
            // quote-99 must be selected, not 105 or 150
            assert_eq!(out.ts_interval, 90);
            assert_eq!(out.quote.ts_received, 99);

            // And the strategy parameters must match
            assert_eq!(out.sigma, 0.5);
            assert_eq!(out.A, Some(1.2));
            assert_eq!(out.k, Some(0.8));

            // State must be advanced correctly
            assert!(state.vol.is_empty());
            assert!(state.pen.is_empty());

            // Quote >= frontier (100) must remain
            assert_eq!(state.latest_quote.get(&150).unwrap().1.ts_received, 150);
            assert_eq!(state.latest_quote.get(&100).unwrap().1.ts_received, 105);
            assert!(state.latest_quote.get(&90).is_none());

        } else {
            panic!("expected Avellaneda output");
        }
    }
    #[test]
    fn does_not_emit_until_quote_for_interval_arrives() {
        let mut state = make_state();

        // interval = 90
        state.vol.insert(90, 0.7);
        state.pen.insert(90, (Some(2.0), Some(1.1)));

        // Only future-interval quotes arrive (no quote for interval 90 yet)
        state.latest_quote.insert(100, (105, make_quote(105, 105.0, 105.0)));
        state.latest_quote.insert(150, (150, make_quote(150, 150.0, 150.0)));

        // Must NOT emit yet
        assert!(
            try_emit(&mut state).is_none(),
            "sequencer must not emit without a quote for the interval"
        );

        // Now a valid quote for interval 90 arrives
        state.latest_quote.insert(90, (99, make_quote(99, 99.0, 99.0)));

        let out = try_emit(&mut state).expect("should emit now");

        if let StrategyInput::Avellaneda(av) = out {
            assert_eq!(av.ts_interval, 90);
            assert_eq!(av.quote.ts_received, 99);
            assert_eq!(av.sigma, 0.7);
            assert_eq!(av.A, Some(2.0));
            assert_eq!(av.k, Some(1.1));
        } else {
            panic!("expected Avellaneda output");
        }

        // State must be consumed correctly
        assert!(state.vol.is_empty());
        assert!(state.pen.is_empty());

        // Future quotes must remain untouched
        assert!(state.latest_quote.contains_key(&100));
        assert!(state.latest_quote.contains_key(&150));
    }
    #[test]
    fn emits_multiple_intervals_in_order_without_cross_contamination() {
        let mut state = make_state();

        // Insert vol + pen for three intervals (out of order)
        state.vol.insert(100, 1.0);
        state.vol.insert(90, 0.5);
        state.vol.insert(110, 1.5);

        state.pen.insert(100, (Some(2.0), Some(1.0)));
        state.pen.insert(90, (Some(1.2), Some(0.8)));
        state.pen.insert(110, (Some(2.5), Some(1.2)));

        // Insert quotes in adversarial order
        state.latest_quote.insert(110, (115, make_quote(115, 110.0, 110.0)));
        state.latest_quote.insert(100, (105, make_quote(105, 100.0, 100.0)));
        state.latest_quote.insert(90, (99, make_quote(99, 90.0, 90.0)));

        // ---- Emit interval 90 ----
        let out1 = try_emit(&mut state).expect("should emit interval 90");
        match out1 {
            StrategyInput::Avellaneda(av) => {
                assert_eq!(av.ts_interval, 90);
                assert_eq!(av.quote.ts_received, 99);
                assert_eq!(av.sigma, 0.5);
            }
            _ => panic!("expected Avellaneda"),
        }

        // ---- Emit interval 100 ----
        let out2 = try_emit(&mut state).expect("should emit interval 100");
        match out2 {
            StrategyInput::Avellaneda(av) => {
                assert_eq!(av.ts_interval, 100);
                assert_eq!(av.quote.ts_received, 105);
                assert_eq!(av.sigma, 1.0);
            }
            _ => panic!("expected Avellaneda"),
        }

        // ---- Emit interval 110 ----
        let out3 = try_emit(&mut state).expect("should emit interval 110");
        match out3 {
            StrategyInput::Avellaneda(av) => {
                assert_eq!(av.ts_interval, 110);
                assert_eq!(av.quote.ts_received, 115);
                assert_eq!(av.sigma, 1.5);
            }
            _ => panic!("expected Avellaneda"),
        }

        // ---- No more emissions ----
        assert!(try_emit(&mut state).is_none());

        // ---- Final state must be clean ----
        assert!(state.vol.is_empty());
        assert!(state.pen.is_empty());
        assert!(state.latest_quote.is_empty());
    }
    #[test]
    fn emits_using_latest_quote_at_or_before_interval() {
        let mut state = make_state();

        let interval = 100;

        // Quote arrives BEFORE the interval boundary
        state.latest_quote.insert(
            100,
            (95, make_quote(95, 95.0, 96.0)),
        );

        // Future quote (must NOT be used)
        state.latest_quote.insert(
            110,
            (110, make_quote(110, 110.0, 111.0)),
        );

        // Vol + Pen arrive exactly on interval
        state.vol.insert(interval, 0.42);
        state.pen.insert(interval, (Some(1.0), Some(0.5)));

        // This SHOULD emit.
        let out = try_emit(&mut state).expect("sequencer should emit");

        match out {
            StrategyInput::Avellaneda(av) => {
                assert_eq!(av.ts_interval, interval as u64);
                assert_eq!(av.sigma, 0.42);
                assert_eq!(av.A, Some(1.0));
                assert_eq!(av.k, Some(0.5));

                assert_eq!(av.quote.ts_received, 95);
            }
            _ => panic!("expected Avellaneda output"),
        }

        // State must advance
        assert!(state.vol.is_empty());
        assert!(state.pen.is_empty());

        // Future quote must remain
        assert!(state.latest_quote.contains_key(&110));
    }

}