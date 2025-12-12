use strategy::{traits::Strategy,runner::run_strategy};
use common::{AnyWsUpdate, TradeUpdate,Order, ExecutionEvent, OrderSide};
use tokio::sync::broadcast;

#[derive(Default, Clone, Debug)]
struct TestStrategy {
    pub seen_market: usize,
    pub seen_fills: usize,
}
impl Strategy for TestStrategy {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order> {
        self.seen_market += 1;

        // For testing: produce a single order if trade price > 100
        match update {
            AnyWsUpdate::Trade(t) if t.price > 100.0 => vec![
                Order::Limit {
                    symbol: t.base.clone()+&t.quote,
                    side: OrderSide::Buy,
                    price: t.price,
                    size: 1,
                }
            ],
            _ => Vec::new(),
        }
    }

    fn on_fill(&mut self, _fill: &ExecutionEvent) {
        self.seen_fills += 1;
    }
}

#[tokio::test]
async fn test_strategy_produces_order_on_market_update() {
    // Channels used by strategy runner
    let (tx_market, rx_market) = broadcast::channel(16);
    let (tx_exec, rx_exec)     = broadcast::channel(16);
    let (tx_order, mut rx_order) = broadcast::channel(16);

    // Strategy under test
    let strat = TestStrategy::default();

    // Spawn strategy runner
    tokio::spawn(run_strategy(
        strat,
        rx_market,
        rx_exec,
        tx_order.clone(),
    ));

    // Send a market update
    let update = AnyWsUpdate::Trade(TradeUpdate {
        exchange: "TestEx".to_string(),
        tick_size: 0.01,
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        side: OrderSide::Buy,
        price: 101.0,
        size: 1,
        ts_exchange: Some(1234567890),
        ts_received: 1234567891,
    });

    tx_market.send(update).unwrap();

    // Expect an order output
    let received = rx_order.recv().await.unwrap();

    match received {
        Order::Limit { symbol, side, price, size } => {
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(side, OrderSide::Buy);
            assert_eq!(price, 101.0);
            assert_eq!(size, 1);
        }
        _ => panic!("Expected limit order"),
    }
}

#[tokio::test]
async fn test_strategy_receives_fills_2() {
    let (tx_market, rx_market) = broadcast::channel(16);
    let (tx_exec, rx_exec)     = broadcast::channel(16);
    let (tx_order, _rx_order)  = broadcast::channel(16);

    let strat = TestStrategy::default();
    // let strat_ref = std::sync::Arc::new(tokio::sync::Mutex::new(strat));
    let strat_runner_ref = strat.clone();

    tokio::spawn(async move {
        run_strategy(
            strat_runner_ref,
            // strat,
            rx_market,
            rx_exec,
            tx_order
        );
    });

    // Send a fill
    let fill = ExecutionEvent {
        action: "fill".into(),
        symbol: "BTCUSDT".into(),
        side: OrderSide::Buy,
        size: 1,
        price: 100.0,
        order_id: 42,
        ts_exchange: Some(1234567888),
        ts_received: 1234567890,
    };

    tx_exec.send(fill).unwrap();

    // Wait briefly for async processing
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify fill was counted
    assert_eq!(strat.seen_fills, 1);
}
#[tokio::test]
async fn test_strategy_receives_fills() {
    let mut strat = TestStrategy::default();

    let fill = ExecutionEvent {
        action: "fill".into(),
        symbol: "BTCUSDT".into(),
        side: OrderSide::Buy,
        size: 1,
        price: 100.0,
        order_id: 42,
        ts_exchange: Some(1234567888),
        ts_received: 1234567890,
    };

    // Call the handler directly instead of running the runner
    strat.on_fill(&fill);

    // Verify fill was counted
    assert_eq!(strat.seen_fills, 1);
}

