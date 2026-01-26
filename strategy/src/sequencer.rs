use common::{VolatilityUpdate, PenetrationUpdate, QuoteUpdate, AvellanedaInput};
use tokio::sync::{mpsc,mpsc::error};
use std::collections::BTreeMap;
// use anyhow::Result;

#[derive(Debug, Clone)]
struct SequencerState {
    interval_ms: i64,
    vol: BTreeMap<i64, f64>,                        // (interval, sigma)
    pen: BTreeMap<i64, (Option<f64>, Option<f64>)>, // (interval, (A, k))
    latest_mid: BTreeMap<i64, (i64, f64)>,             // (interval, (ts_received, mid))
    last_emitted: Option<i64>,
}
// TODO:Storing quotes in BTreeMap seems overkill here. Maybe derive interval size
// from the volatility/penetration updates and store only the latest quote per interval?
pub fn try_emit(state: &mut SequencerState) -> Option<AvellanedaInput> {
    if let Some((&interval, &sigma)) = state.vol.iter().next() {
        if let Some(&(A, k)) = state.pen.get(&interval) {
            let frontier_ts = interval;
            // Get latest mid in interval
            if let Some((ts,mid)) = state.latest_mid.get(&frontier_ts) {
                // Emit output
                let sequenced_output = AvellanedaInput{
                    ts_interval: interval as u64,
                    quote_ts: *ts as u64,
                    sigma,
                    A,
                    k,
                    mid: *mid,
                };
                // remove used entries
                state.vol.remove(&interval);
                state.pen.remove(&interval);
                state.latest_mid.remove(&frontier_ts);                

                return Some(sequenced_output);
            }
        }
    }
    None
}

pub async fn sequencer_run(
    interval_ms: i64,
    mut vol_rx: mpsc::Receiver<VolatilityUpdate>,
    mut pen_rx: mpsc::Receiver<PenetrationUpdate>,
    mut quote_rx: mpsc::Receiver<QuoteUpdate>,
    strategy_tx: mpsc::Sender<AvellanedaInput>,
) -> Result<(), error::SendError<AvellanedaInput>> {
    let mut state = SequencerState {
        interval_ms,
        vol: BTreeMap::new(),
        pen: BTreeMap::new(),
        latest_mid: BTreeMap::new(),
        last_emitted: None,
    };
    let mut vol_interval_checked = false;
    let mut pen_interval_checked = false;

    loop {
        tokio::select! {
            Some(v) = vol_rx.recv() => {
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
                if let (Some(bid), Some(ask)) = (q.best_bid, q.best_ask) {
                    let mid = (bid + ask ) / 2.0;
                    let interval_id = q.ts_received - (q.ts_received % state.interval_ms);
                    if let Some((existing_ts, _)) = state.latest_mid.get(&interval_id) {
                        // Update only if newer quote
                        if q.ts_received > *existing_ts {
                            state.latest_mid.insert(interval_id, (q.ts_received, mid));
                        }
                    } else {
                        // No existing quote, insert new
                        state.latest_mid.insert(interval_id, (q.ts_received, mid));
                    }
                }
            }
        }

        while let Some(output) = try_emit(&mut state) {
            strategy_tx.send(output).await?;
            state.last_emitted = Some(output.ts_interval as i64);
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
            latest_mid: BTreeMap::new(),
            last_emitted: None,
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
        state.latest_mid.insert(90, (91, 80.0));
        state.latest_mid.insert(100, (105, 105.0));
        state.latest_mid.insert(90, (99, 99.0));
        state.latest_mid.insert(150, (150, 150.0));

        let out = try_emit(&mut state).expect("should emit");

        // The key assertion:
        // quote-99 must be selected, not 105 or 150
        assert_eq!(out.ts_interval, 90);
        assert_eq!(out.quote_ts, 99);
        assert_eq!(out.mid, 99.0);

        // And the strategy parameters must match
        assert_eq!(out.sigma, 0.5);
        assert_eq!(out.A, Some(1.2));
        assert_eq!(out.k, Some(0.8));

        // State must be advanced correctly
        assert!(state.vol.is_empty());
        assert!(state.pen.is_empty());

        // Mid >= frontier (100) must remain
        assert_eq!(state.latest_mid.get(&150), Some(&(150, 150.0)));
        assert_eq!(state.latest_mid.get(&100), Some(&(105, 105.0)));
        assert!(state.latest_mid.get(&90).is_none());
    }
}

