pub struct MomentumStrategy {
    last_price: Option<f64>,
    position: i64,
}

impl MomentumStrategy {
    pub fn new() -> Self {
        Self {
            last_price: None,
            position: 0,
        }
    }
}

impl Strategy for MomentumStrategy {
    fn on_market(&mut self, update: &AnyWsUpdate) -> Vec<Order> {
        let mut orders = Vec::new();

        if let AnyWsUpdate::Trade(t) = update {
            if let Some(prev) = self.last_price {
                if t.price > prev && self.position <= 0 {
                    // price rising: open long
                    orders.push(Order::Limit {
                        symbol: t.symbol.clone(),
                        side: OrderSide::Buy,
                        price: t.price,
                        size: 1000,
                    });
                } else if t.price < prev && self.position >= 0 {
                    // price falling: open short
                    orders.push(Order::Limit {
                        symbol: t.symbol.clone(),
                        side: OrderSide::Sell,
                        price: t.price,
                        size: 1000,
                    });
                }
            }

            self.last_price = Some(t.price);
        }

        orders
    }

    fn on_fill(&mut self, fill: &ExecutionEvent) {
        // Maintain a simple position counter
        match fill.side {
            OrderSide::Buy => self.position += fill.size,
            OrderSide::Sell => self.position -= fill.size,
        }
    }
}

