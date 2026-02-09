use common::{OrderSide, ExecutionEvent};
use tokio::sync::mpsc;
use common::{PositionState,ContractType};
use crate::config::PositionEngineConfig;

pub async fn run_position_engine(
    cfg: PositionEngineConfig,
    mut rx_exec: mpsc::Receiver<ExecutionEvent>,
    tx_position: mpsc::Sender<PositionState>,
) {
    let mut state = PositionState {
        inv_cost: 0.0, // sum of contracts * (1/price) for inverse positions
        timestamp: 0,
        position: 0,
        avg_price: 0.0,
        realized_pnl: 0.0,
        cash: 0.0,
        fees_paid: 0.0,
    };
    let ct = cfg.contract_type.clone();
    let fee_rate = cfg.fee_rate.clone();
    while let Some(exec) = rx_exec.recv().await {

        apply_execution(&mut state, &exec, &ct, fee_rate);
        let _ = tx_position.send(state.clone()).await;
    }
}

fn apply_execution(state: &mut PositionState, exec: &ExecutionEvent, ct: &ContractType, fee_rate: f64) {
    state.timestamp = exec.ts_received;

    let size = exec.size as i64;
    let price = exec.price;

    let signed = match exec.side {
        OrderSide::Buy => size,      // + for buy
        OrderSide::Sell => -size,     // - for sell
    };

    let old_pos = state.position;
    let new_pos = old_pos + signed;

    // ---------- 1) If this trade closes some existing position, realize PnL ----------
    if old_pos != 0 && old_pos.signum() != signed.signum() {
        let closed = old_pos.abs().min(signed.abs());

        match ct {
            ContractType::Linear => {
                // closing long or short, sign handled by formula choice
                if old_pos > 0 {
                    // closing long
                    state.realized_pnl += (price - state.avg_price) * closed as f64;
                } else {
                    // closing short
                    state.realized_pnl += (state.avg_price - price) * closed as f64;
                }
            }
            ContractType::Inverse => {
                // Portion of inverse cost being closed
                let pos_abs = old_pos.abs() as f64;
                let closed_f = closed as f64;

                // entry inverse value for the closed portion
                let entry_inv = state.inv_cost * (closed_f / pos_abs);
                let exit_inv = closed_f * (1.0 / price);

                // Direction of the closed portion
                let direction = signed.signum() as f64; // +1 for long close, -1 for short close

                // PnL in quote currency
                let pnl = direction * (exit_inv - entry_inv);
                state.realized_pnl += pnl;

                // Remove closed portion from inverse cost basis
                state.inv_cost -= entry_inv;
            }
        }
    }

    // ---------- 2) Update position ----------
    state.position = new_pos;
    // ---------- 3) Update cost basis / avg price ----------
    if new_pos == 0 {
        // Flat
        state.avg_price = 0.0;
        state.inv_cost = 0.0;
    } else if old_pos == 0 || old_pos.signum() == signed.signum() {
        // Case 1: opening or increasing in same direction

        match ct {
            ContractType::Linear => {
                let old_abs = old_pos.abs() as f64;
                let add_abs = signed.abs() as f64;
                let new_abs = new_pos.abs() as f64;

                let old_notional = state.avg_price * old_abs;
                let add_notional = price * add_abs;

                state.avg_price = (old_notional + add_notional) / new_abs;
            }
            ContractType::Inverse => {
                state.inv_cost += (signed.abs() as f64) * (1.0 / price);
                let new_abs = new_pos.abs() as f64;
                state.avg_price = new_abs / state.inv_cost;
            }
        }
    } else if new_pos.signum() == old_pos.signum() {
        // Case 2: partial close, same direction remains
        // Do NOTHING to avg_price (and inv_cost already reduced in the close step)
        match ct {
            ContractType::Linear => {
                // avg_price unchanged
            }
            ContractType::Inverse => {
                // inv_cost already adjusted, recompute display avg_price
                let new_abs = new_pos.abs() as f64;
                state.avg_price = new_abs / state.inv_cost;
            }
        }
    } else {
        // Case 3: flipped position
        match ct {
            ContractType::Linear => {
                state.avg_price = price;
            }
            ContractType::Inverse => {
                let new_abs = new_pos.abs() as f64;
                state.inv_cost = new_abs * (1.0 / price);
                state.avg_price = price;
            }
        }
    }

    // ---------- 4) Cash movement ----------
    let cash_delta = match ct {
        ContractType::Linear => {
            // cash -= price * signed
            -(signed as f64) * price
        }
        ContractType::Inverse => {
            // cash -= signed / price
            (signed as f64) / price
        }
    };

    state.cash += cash_delta;
    //-----------5) Fees-------------------
    let fee = fee_rate * match ct {
        ContractType::Linear => {
            // Fee in base currency
            (signed as f64) * price
        }
        ContractType::Inverse => {
            // Fee in quote currency
            (signed as f64) / price
        }
    }.abs();
    state.cash -= fee;
    state.fees_paid += fee;
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
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Buy, 100.0, 10), &ContractType::Linear, 0.0);

        assert_eq!(s.position, 10);
        assert_eq!(s.avg_price, 100.0);
        assert_eq!(s.cash, -1000.0);
        assert_eq!(s.realized_pnl, 0.0);
        assert_eq!(s.fees_paid, 0.0);
    }

    #[test]
    fn multiple_buys_compute_vwap() {
            let mut s = PositionState {
            inv_cost: 0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Buy, 100.0, 10), &ContractType::Linear, 0.0);
        apply_execution(&mut s, &exec(OrderSide::Buy, 110.0, 10), &ContractType::Linear, 0.0);

        assert_eq!(s.position, 20);
        assert_eq!(s.avg_price, 105.0);
        assert_eq!(s.cash, -2100.0);
        assert_eq!(s.fees_paid, 0.0);
    }

    #[test]
    fn partial_sell_realizes_pnl() {
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 10,
            avg_price: 100.0,
            realized_pnl: 0.0,
            cash: -1000.0,
            fees_paid: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Sell, 110.0, 4), &ContractType::Linear, 0.0);

        assert_eq!(s.position, 6);
        assert_eq!(s.avg_price, 100.0);
        assert_eq!(s.realized_pnl, 40.0);
        assert_eq!(s.cash, -560.0);
        assert_eq!(s.fees_paid, 0.0);
    }

    #[test]
    fn full_exit_resets_avg_price() {
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 5,
            avg_price: 200.0,
            realized_pnl: 0.0,
            cash: -1000.0,
            fees_paid: 0.0,
        };

        apply_execution(&mut s, &exec(OrderSide::Sell, 210.0, 5), &ContractType::Linear, 0.0);

        assert_eq!(s.position, 0);
        assert_eq!(s.avg_price, 0.0);
        assert_eq!(s.realized_pnl, 50.0);
        assert_eq!(s.cash, 50.0);
        assert_eq!(s.fees_paid, 0.0);
    }
    #[test]
    fn compare_with_arxiv_linear_sol_usdt_example() {
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
            inv_cost: 0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid:0.0,
        };

        // Apply each execution
        for (side, price, size) in trades {
            apply_execution(&mut s, &exec(side, price, size), &ContractType::Linear, 0.0);
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
    #[test]
    fn compare_with_bitmex_web_inverse_example() {
        // Example from BitMEX documentation for inverse contracts:
        // https://www.bitmex.com/app/instrument/XBTUSD
        //A trader goes long 50,000 contracts of XBTUSD at a price of 10,000. A few days later the price of the contract increases to 11,000.
        //the trader’s profit will be: 50,000 * 1 * (1/10,000 - 1/11,000) = 0.4545 XBT
        let trades = vec![
            (OrderSide::Buy, 10000.0, 50000),
            (OrderSide::Sell, 11000.0, 50000),
        ];
        let mut s = PositionState {
            inv_cost: 0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };
        let (side, price, size) = trades[0];
        apply_execution(&mut s, &exec(side, price, size), &ContractType::Inverse, 0.0  );
        let (side, price, size) = trades[1];
        apply_execution(&mut s, &exec(side, price, size), &ContractType::Inverse, 0.0);
        let expected_realized_pnl_xbt = 0.4545_f64;
        assert!((s.realized_pnl - expected_realized_pnl_xbt).abs() < 1e-4,
                "realized_pnl {} vs expected {}", s.realized_pnl, expected_realized_pnl_xbt);
        assert!((s.cash - expected_realized_pnl_xbt).abs() < 1e-4,
                "cash {} vs expected {}", s.cash, expected_realized_pnl_xbt);        
        // If the price had in fact dropped to 9,000, the trader’s loss would have been: 50,000 * 1 * (1/10,000 - 1/9,000) = -0.5556 XBT.
        let trades = vec![
            (OrderSide::Buy, 10000.0, 50000),
            (OrderSide::Sell, 9000.0, 50000),
        ];
        let mut s = PositionState {
            inv_cost: 0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };
        let (side, price, size) = trades[0];
        apply_execution(&mut s, &exec(side, price, size), &ContractType::Inverse, 0.0);
        let (side, price, size) = trades[1];
        apply_execution(&mut s, &exec(side, price, size), &ContractType::Inverse, 0.0);
        let expected_realized_pnl_xbt = -0.5556_f64;
        assert!((s.realized_pnl - expected_realized_pnl_xbt).abs() < 1e-4,
                "realized_pnl {} vs expected {}", s.realized_pnl, expected_realized_pnl_xbt);    
        assert!((s.cash - expected_realized_pnl_xbt).abs() < 1e-4,
                "cash {} vs expected {}", s.cash, expected_realized_pnl_xbt);      }
    #[test]
    fn test_linear_pnl_with_fees_long() {
        // Long with net profit and fees
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        let fee_rate = 0.00055; // 0.055% fee

        apply_execution(&mut s, &exec(OrderSide::Buy, 5000.0, 1000), &ContractType::Linear, fee_rate);
        apply_execution(&mut s, &exec(OrderSide::Sell, 5500.0, 1000), &ContractType::Linear, fee_rate);

        // Gross PnL = (5500 - 5000) * 1000 = 500_000
        // Fees = (1000 * 5500) * 0.00055 + (1000 * 5000) * 0.00055 = 3025 + 2750 = 5775
        // Net PnL = Gross PnL - Fees = 500_000 - 5775 = 494_225
        assert!((s.realized_pnl - (500_000.0)).abs() < 1e-3 );
        assert!((s.fees_paid - 5775.0).abs() < 1e-4);
        assert_eq!(s.cash, 494_225.0);        
        
        // Long with net loss and fees
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        let fee_rate = 0.00055; // 0.055% fee

        apply_execution(&mut s, &exec(OrderSide::Buy, 5000.0, 1000), &ContractType::Linear, fee_rate);
        apply_execution(&mut s, &exec(OrderSide::Sell, 4500.0, 1000), &ContractType::Linear, fee_rate);

        // Gross PnL = (4500 - 5000) * 1000 = -500_000
        // Fees = (1000 * 5000) * 0.00055 + (1000 * 4500) * 0.00055 = 2750 + 2475 = 5225
        // Net PnL = Gross PnL - Fees = -500_000 - 5225 = -505_225

        assert!((s.realized_pnl - (-500_000.0)).abs() < 1e-3 );
        assert!((s.fees_paid - 5225.0).abs() < 1e-4);
        assert_eq!(s.cash, -505_225.0);
    }
    fn test_linear_pnl_with_fees_short() {
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        let fee_rate = 0.00055; // 0.055% fee

        apply_execution(&mut s, &exec(OrderSide::Sell, 5000.0, 1000), &ContractType::Linear, fee_rate);
        apply_execution(&mut s, &exec(OrderSide::Buy, 4500.0, 1000), &ContractType::Linear, fee_rate);

        // Gross PnL = (5000 - 4500) * 1000 = 500_000
        // Fees = (1000 * 5000) * 0.00055 + (1000 * 4500) * 0.00055 = 2750 + 2475 = 5225
        // Net PnL = Gross PnL - Fees = 500_000 - 5225 = 494_775

        assert!((s.realized_pnl - 500_000.0).abs() < 1e-3 );
        assert!((s.fees_paid - 5225.0).abs() < 1e-4);
        assert_eq!(s.cash, 494_775.0);
    }
    #[test]
    fn test_inv_pnl_with_fees_long() {
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        let fee_rate = 0.00055; // 0.055% fee

        apply_execution(&mut s, &exec(OrderSide::Buy, 5000.0, 1000), &ContractType::Inverse, fee_rate);
        apply_execution(&mut s, &exec(OrderSide::Sell, 4500.0, 1000), &ContractType::Inverse, fee_rate);

        // Gross PnL = (1/4500 - 1/5000) * 1000 = -0.02223
        // Fees = (1000 * 1/5000) * 0.00055 + (1000 * 1/4500) * 0.00055 = 0.0002322 
        // Net PnL = Gross PnL - Fees = -0.02223 - 0.0002322 = -0.0224622

        assert!((s.realized_pnl - (-0.02223)).abs() < 1e-4 );
        assert!((s.fees_paid - 0.0002322).abs() < 1e-6);
        assert!((s.cash - (-0.0224622)).abs() < 1e-4);
    }
    #[test]
    fn test_inv_pnl_with_fees_short() {
        let mut s = PositionState {
            inv_cost:0.0,
            timestamp: 0,
            position: 0,
            avg_price: 0.0,
            realized_pnl: 0.0,
            cash: 0.0,
            fees_paid: 0.0,
        };

        let fee_rate = 0.00055; // 0.055% fee

        apply_execution(&mut s, &exec(OrderSide::Sell, 5000.0, 1000), &ContractType::Inverse, fee_rate);
        apply_execution(&mut s, &exec(OrderSide::Buy, 4500.0, 1000), &ContractType::Inverse, fee_rate);

        // Gross PnL = (1/4500 - 1/5000) * 1000 = 0.02223
        // Fees = (1000 * 1/5000) * 0.00055 + (1000 * 1/4500) * 0.00055 = 0.0002322 
        // Net PnL = Gross PnL - Fees = 0.02223 - 0.0002322 = 0.0219978

        assert!((s.realized_pnl - 0.02223).abs() < 1e-4 );
        // assert_eq!(s.realized_pnl,0.02223);
        assert!((s.fees_paid - 0.0002322).abs() < 1e-6);
        // assert_eq!(s.fees_paid,0.0002322);
        assert!((s.cash - 0.0219978).abs() < 1e-4);
    }


}
