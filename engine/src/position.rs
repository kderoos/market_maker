use common::{OrderSide, ExecutionEvent};
use tokio::sync::mpsc;
use common::PositionState;


pub async fn run_position_engine(
    mut rx_exec: mpsc::Receiver<ExecutionEvent>,
    tx_position: mpsc::Sender<PositionState>,
) {
    let mut state = PositionState {
        timestamp: 0,
        position: 0,
        avg_price: 0.0,
        realized_pnl: 0.0,
        cash: 0.0,
    };
    
    while let Some(exec) = rx_exec.recv().await {

        apply_execution(&mut state, &exec);
        let _ = tx_position.send(state.clone()).await;
    }
}
fn apply_execution(state: &mut PositionState, exec: &ExecutionEvent) {
    match exec.side {
        OrderSide::Buy => {
            state.timestamp = exec.ts_received;
            let signed = exec.size;
            let cost = exec.price * signed as f64;

            let new_pos = state.position + signed;

            if state.position == 0 || state.position.signum() == signed.signum() {
                // increase or open
                state.avg_price =
                    (state.avg_price * state.position as f64 + cost)
                    / new_pos as f64;
            } else {
                // closing short
                let closed = signed.min(-state.position);
                state.realized_pnl +=
                    (state.avg_price - exec.price) * closed as f64;
            }

            state.position = new_pos;
            state.cash -= cost;

            if state.position == 0 {
                state.avg_price = 0.0;
            } else if state.position.signum() != (state.position - signed).signum() {
                // position flipped: reset cost basis for the remaining position
                state.avg_price = exec.price;
            }        
        }

        OrderSide::Sell => {
            state.timestamp = exec.ts_received;
            let signed = -exec.size;
            let proceeds = exec.price * exec.size as f64;

            let new_pos = state.position + signed;

            if state.position == 0 || state.position.signum() == signed.signum() {
                // increase short
                state.avg_price =
                    (state.avg_price * state.position as f64 - proceeds)
                    / new_pos as f64;
            } else {
                // closing long
                let closed = (-signed).min(state.position);
                state.realized_pnl +=
                    (exec.price - state.avg_price) * closed as f64;
            }

            state.position = new_pos;
            state.cash += proceeds;

            if state.position == 0 {
                state.avg_price = 0.0;
            } else if state.position.signum() != (state.position - signed).signum() {
                // position flipped: reset cost basis for the remaining position
                state.avg_price = exec.price;
            }

        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use common::OrderSide;

    fn exec(side: OrderSide, price: f64, size: i64) -> ExecutionEvent {
        ExecutionEvent {
            action: "Fill".into(),
            symbol: "BTCUSDT".into(),
            order_id: 1,
            side,
            price,
            size,
            ts_exchange: None,
            ts_received: 0,
        }
    }

    #[test]
    fn buy_updates_position_and_avg_price() {
        let mut s = PositionState {
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Buy, 100.0, 10));

        assert_eq!(s.position, 10);
        assert_eq!(s.avg_price, 100.0);
        assert_eq!(s.cash, -1000.0);
        assert_eq!(s.realized_pnl, 0.0);
    }

    #[test]
    fn multiple_buys_compute_vwap() {
        let mut s = PositionState {
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Buy, 100.0, 10));
        apply_execution(&mut s, &exec(OrderSide::Buy, 110.0, 10));

        assert_eq!(s.position, 20);
        assert_eq!(s.avg_price, 105.0);
        assert_eq!(s.cash, -2100.0);
    }

    #[test]
    fn partial_sell_realizes_pnl() {
        let mut s = PositionState {
            position: 10,
            avg_price: 100.0,
            realized_pnl: 0.0,
            cash: -1000.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Sell, 110.0, 4));

        assert_eq!(s.position, 6);
        assert_eq!(s.avg_price, 100.0);
        assert_eq!(s.realized_pnl, 40.0);
        assert_eq!(s.cash, -560.0);
    }

    #[test]
    fn full_exit_resets_avg_price() {
        let mut s = PositionState {
            position: 5,
            avg_price: 200.0,
            realized_pnl: 0.0,
            cash: -1000.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Sell, 210.0, 5));

        assert_eq!(s.position, 0);
        assert_eq!(s.avg_price, 0.0);
        assert_eq!(s.realized_pnl, 50.0);
        assert_eq!(s.cash, 50.0);
    }
    #[test]
    fn compare_with_arxiv_sol_usdt_example() {
        // Trades from the paper example (base = SOL, quote = USDT)
        // https://arxiv.org/html/2411.14068v1
        let trades = vec![
            (OrderSide::Buy, 170.0, 5),
            (OrderSide::Buy, 175.0, 10),
            (OrderSide::Sell, 180.0, 20),
            (OrderSide::Buy, 160.0, 5),
            (OrderSide::Buy, 165.0, 12),
            (OrderSide::Sell, 170.0, 12),
        ];

        let mut s = PositionState {
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
        };

        // Apply each execution
        for (side, price, size) in trades {
            apply_execution(&mut s, &exec(side, price, size));
        }

        // After all trades from the example:
        // The final realized PnL in base units (SOL) should be approx 1.527165…
        // And since the last quote price is 170.25 (ask), 1.527165 * 170.25 ≈ 260 USDT.
        let expected_realized_pnl_sol = 1.527165_f64;
        let expected_realized_pnl_usdt = 260.0_f64;

        assert!((s.realized_pnl - expected_realized_pnl_usdt).abs() < 1e-6, 
                "realized_pnl {} vs expected {}", s.realized_pnl, expected_realized_pnl_usdt);

        assert_eq!(s.position, 0, "final position should be flat");

        assert_eq!(s.avg_price, 0.0, "avg price should reset when flat");

        assert!((s.cash - expected_realized_pnl_usdt).abs() < 1e-6,
                "cash {} vs expected {}", s.cash, expected_realized_pnl_usdt);
    }
}
