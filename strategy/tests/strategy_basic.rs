use strategy::{traits::Strategy,runner::run_strategy};
use common::{StrategyInput, TradeUpdate,Order, ExecutionEvent, OrderSide};
use tokio::sync::broadcast;

#[derive(Default, Clone, Debug)]
struct TestStrategy {
    pub seen_market: usize,
    pub seen_fills: usize,
}
impl Strategy for TestStrategy {
    fn on_market(&mut self, update: &StrategyInput) -> Vec<Order> {
        self.seen_market += 1;

        // For testing: produce a single order if trade price > 100
        match update {
            StrategyInput::Trade(t) if t.price > 100.0 => vec![
                Order::Limit {
                    symbol: t.base.clone()+&t.quote,
                    side: OrderSide::Buy,
                    price: t.price,
                    size: 1,
                    client_id: None,
                    ts_received: t.ts_received,
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
async fn test_strategy_fills() {
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
#[tokio::test]
async fn test_strategy_market() {
    let mut strat = TestStrategy::default();
    let trade_update = TradeUpdate {
        exchange: "TestExchange".into(),
        tick_size: 0.01,
        base: "BTC".into(),
        quote: "USDT".into(),
        side: OrderSide::Buy,
        price: 150.0,
        size: 1,
        ts_exchange: Some(1234567888),
        ts_received: 1234567890,
    };
    let market_update = StrategyInput::Trade(trade_update);
    let orders = strat.on_market(&market_update);
    // Verify market update was counted
    assert_eq!(strat.seen_market, 1);
    // Verify an order was produced
    assert_eq!(orders.len(), 1);
    if let Order::Limit{ symbol, side, price, size, client_id, ts_received } = &orders[0] {
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(*side, OrderSide::Buy);
        assert_eq!(*price, 150.0);
        assert_eq!(*size, 1);
    } else {
        panic!("Expected Limit order");
    }
}
