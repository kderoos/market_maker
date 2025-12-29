use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::sleep;
use tokio;
use common::{AnyWsUpdate, TradeUpdate, Order, OrderSide, ExecutionEvent};
use common::OrderSide::{Buy, Sell};

use strategy::momentum::MomentumStrategy;
use strategy::runner::run_strategy;
use utils::candle_service::CandleService;
use engine::execution::run as run_execution; 
use engine::book::OrderBook; 
use tokio::sync::RwLock;

#[tokio::test]
async fn test_momentum_with_sinusoidal_trades() {
    assert!(false);
    // Channels: market updates, engine trades, orders, exec events
    let (tx_market, _) = broadcast::channel::<AnyWsUpdate>(256);
    let (tx_trade, _) = broadcast::channel::<TradeUpdate>(256);
    let (tx_order, _) = broadcast::channel::<Order>(256);//Strategy -> execution
    let (tx_exec, _)  = broadcast::channel::<ExecutionEvent>(256);

    // subscribe handles for spawned tasks
    let mut rx_order = tx_order.subscribe();
    
    // debug: also spawn a logger that prints everything the strategy sends
    {
        let mut rx_order_log = tx_order.subscribe();
        tokio::spawn(async move {
            while let Ok(order) = rx_order_log.recv().await {
                eprintln!("[DEBUG] strategy -> order: {:?}", order);
            }
        });
    }
    // debug: log execution events from engine
    {
        let mut rx_exec_log = tx_exec.subscribe();
        tokio::spawn(async move {
            while let Ok(exec) = rx_exec_log.recv().await {
                eprintln!("[DEBUG] engine -> exec: {:?}", exec);
            }
        });
    }

    // Spawn candle service (optional — demonstrates wiring)
    // CandleService expects mpsc channels: input AnyWsUpdate and output AnyWsUpdate
    let (cs_tx_in, cs_rx_in) = mpsc::channel(256);
    let (cs_tx_out, mut cs_rx_out) = mpsc::channel(256);
    // symbol must match trades' symbol format used by CandleService/trades
    let candle_service = CandleService::new("BTCUSDT".to_string(), 100, cs_rx_in, cs_tx_out);
    candle_service.spawn();

    // forward AnyWsUpdate market broadcast into candle service input (non-blocking)
    {
        let mut rx = tx_market.subscribe();
        let cs_tx_in = cs_tx_in.clone();
        task::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                // forward to candle service
                let _ = cs_tx_in.send(msg).await;
            }
        });
    }

    // Spawn engine run() (execution engine) — feed it TradeUpdate receiver and Order receiver
    let orderbook = Arc::new(RwLock::new(engine::book::OrderBook::default())); 
    let mut engine_trade_rx = tx_trade.subscribe();
    let mut engine_order_rx = tx_order.subscribe();
    let tx_exec_clone = tx_exec.clone();
    let orderbook_clone = orderbook.clone();
    let mut execution_events = tx_exec.subscribe();
    task::spawn(async move {
        run_execution(orderbook_clone, engine_trade_rx, engine_order_rx, tx_exec_clone).await;
    });

    // Spawn momentum strategy runner
    let rx_market_for_strategy = tx_market.subscribe();
    let rx_exec_for_strategy = tx_exec.subscribe();
    let tx_order_for_strategy = tx_order.clone();
    let strategy = MomentumStrategy::new(3, 6, 1); // fast=3, slow=6, size=1
    task::spawn(async move {
        run_strategy(strategy, rx_market_for_strategy, rx_exec_for_strategy, tx_order_for_strategy).await;
    });

    // send sinusoidal trades (also send to engine trade channel)
    let samples = 300usize;
    let base_price = 10000.0_f64;
    let amplitude = 50.0_f64;
    let freq = 40.0_f64; // affects how often crosses happen
    for i in 0..samples {
        let t = i as f64;
        let price = base_price + amplitude * ((2.0 * std::f64::consts::PI * t / freq).sin());
        let trade = TradeUpdate {
            exchange: "TestEx".to_string(),
            tick_size: 0.01,
            base: "BTC".to_string(),
            quote: "USDT".to_string(),
            // symbol: "BTCUSDT".to_string(),
            side: if i % 2 == 0 { Buy } else { Sell },
            price,
            size: 1,
            ts_exchange: Some((i as i64) * 10),
            ts_received: (i as i64) * 10,
        };

        // send to engine
        let _ = tx_trade.send(trade.clone());
        // send to strategy as AnyWsUpdate::Trade
        let _ = tx_market.send(AnyWsUpdate::Trade(trade));

        // short pause to let components process (tune as needed)
        sleep(Duration::from_millis(5)).await;
    }

    // close candle service input so it flushes any final candles
    drop(cs_tx_in);


    // Expect at least one order emitted by the strategy due to EMA crossovers
    let got_order = tokio::time::timeout(Duration::from_secs(5), rx_order.recv()).await;
    match got_order {
        Ok(Ok(order)) => {
            eprintln!("Received order from strategy: {:?}", order);
            // pass — test succeeded
        }
        other => {
            panic!("expected at least one order from momentum strategy, got: {:?}", other);
        }
    }
    // Expect execution event
    // let got_exec = tokio::time::timeout(Duration::from_secs(5), tx_exec.subscribe().recv()).await;
    let got_exec = tokio::time::timeout(Duration::from_secs(5), execution_events.recv()).await;
    match got_exec {
        Ok(Ok(exec)) => {
            eprintln!("Received execution event: {:?}", exec);
            // pass — test succeeded
        }
        other => {
            panic!("expected at least one execution event, got: {:?}", other);
        }
    }
}
