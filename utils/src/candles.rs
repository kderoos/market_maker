use common::{Candle, AnyWsUpdate, TradeUpdate, OrderSide};

pub struct CandleAggregator {
    pub interval_ms: i64,
    pub symbol: Option<String>,
    current: Option<Candle>,
}

impl CandleAggregator {
    pub fn new(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            symbol: None,
            current: None,
        }
    }

    /// Feed one Trade message. Returns:
    /// - Some(candle) when a candle is finished
    /// - None when still accumulating
    pub fn on_trade(&mut self, trade: &TradeUpdate) -> Option<Candle> {
        let bucket = trade.ts_received / (self.interval_ms * 1000);

        // Set symbol if not already set
        if self.symbol.is_none() {
            self.symbol = Some(trade.base.clone() + &trade.quote);
        }

        // Candle start timestamp
        let ts_open = bucket * (self.interval_ms * 1000);

        match &mut self.current {
            None => {
                // First trade of the candle
                self.current = Some(Candle {
                    symbol: self.symbol.clone().expect("symbol should be set"),
                    ts_open,
                    open: trade.price,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    volume: trade.size as f64,
                });
                None
            }

            Some(c) => {
                if c.ts_open != ts_open {
                    // Candle completed → replace with new candle
                    let finished = c.clone();
                    // Start a new candle
                    *c = Candle {
                        symbol: self.symbol.clone().expect("symbol should be set"),
                        ts_open,
                        open: trade.price,
                        high: trade.price,
                        low: trade.price,
                        close: trade.price,
                        volume: trade.size as f64,
                    };

                    Some(finished)
                } else {
                    // Update current candle
                    c.close = trade.price;
                    if trade.price > c.high { c.high = trade.price; }
                    if trade.price < c.low  { c.low  = trade.price; }
                    c.volume += trade.size as f64;

                    None
                }
            }
        }
    }

    /// Flush current candle (e.g., on shutdown)
    pub fn force_close(&mut self) -> Option<Candle> {
        self.current.take()
    }
}
