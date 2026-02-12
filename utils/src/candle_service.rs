use tokio::sync::mpsc::{Receiver, Sender};
use crate::candles::{CandleAggregator};
use common::{Candle,AnyWsUpdate, TradeUpdate, OrderSide};
use tracing::info;

/// CandleServiceActor is responsible for converting incoming Trade updates
/// into completed OHLCV candles and publishing them downstream.
pub struct CandleService {
    symbol: String,
    candle_agg: CandleAggregator,

    rx_in: Receiver<AnyWsUpdate>,
    tx_out: Sender<AnyWsUpdate>,
}

impl CandleService {
    pub fn new(
        symbol: impl Into<String>,
        interval_ms: i64,
        rx_in: Receiver<AnyWsUpdate>,
        tx_out: Sender<AnyWsUpdate>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            candle_agg: CandleAggregator::new(interval_ms),
            rx_in,
            tx_out,
        }
    }

    /// Spawns the actor task.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx_in.recv().await {
            if let AnyWsUpdate::Trade(t) = msg {
                self.handle_trade(t).await;
            }
        }

        // flush at shutdown
        if let Some(c) = self.candle_agg.force_close() {
            let _ = self
                .tx_out
                .send(AnyWsUpdate::TradeCandle(c))
                .await;
        }
    }

    async fn handle_trade(&mut self, trade: TradeUpdate) {
        // if trade.base.clone() + &trade.quote != self.symbol {
        //     return;
        // }

        if let Some(candle) = self.candle_agg.on_trade(&trade) {
            let _ = self
                .tx_out
                .send(AnyWsUpdate::TradeCandle(candle))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use common::{AnyWsUpdate, TradeUpdate};
    use tokio::time::{timeout, Duration};
    #[tokio::test]
    async fn test_candle_service() {
        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);
        let symbol = "BTCUSDT".to_string();
        let interval_ms = 1000; // 1 second candles
        let candle_service = CandleService::new(symbol.clone(), interval_ms, rx_in, tx_out);
        candle_service.spawn();
        // Send some trade updates
        let trades = vec![
            TradeUpdate {
                exchange: "TestEx".to_string(),
                tick_size: 0.01,
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
                side: OrderSide::Buy,
                price: 100.0,
                size: 1,
                ts_exchange: Some(1000),
                ts_received: 1001,
            },
            TradeUpdate {
                exchange: "TestEx".to_string(),
                tick_size: 0.01,
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
                side: OrderSide::Buy,
                price: 101.0,
                size: 2,
                ts_exchange: Some(1500),
                ts_received: 1501,
            },
            TradeUpdate {
                exchange: "TestEx".to_string(),
                tick_size: 0.01,
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
                side: OrderSide::Buy,
                price: 99.0,
                size: 1,
                ts_exchange: Some(2000),
                ts_received: 2001,
            },

        ];
        for trade in trades {
            let _ = tx_in.send(AnyWsUpdate::Trade(trade)).await;
        }
        // Expect one completed candle
        let msg = timeout(Duration::from_secs(2), rx_out.recv())
            .await
            .expect("Timed out waiting for candle")
            .expect("No candle received");
        if let AnyWsUpdate::TradeCandle(candle) = msg {
            assert_eq!(candle.ts_open, 1000);
            assert_eq!(candle.open, 100.0);
            assert_eq!(candle.high, 101.0);
            assert_eq!(candle.low, 100.0);
            assert_eq!(candle.close, 101.0);
            assert_eq!(candle.volume, 3.0);
        } else {
            panic!("Expected TradeCandle update");
        }
    }
}