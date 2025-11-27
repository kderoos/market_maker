// Simulates order handling at exchange. It maintains order requests in it's state, the relative 
// position within the price level and uses the "trade" feed to determine what orders might be 
// filled. Since we only receive accumulated volume at a price level and not the individual 
// orders at a specific price we approximate "Fill" events using a statistical transaction 
// probability (Monte Carlo). 
pub async fn run(orderbook: Arc<RwLock<OrderBook>>,
                 mut trade_rx: Receiver<TradeUpdate>,
                 mut quote_rx: Receiver<Order>,
                 exec_tx: Sender<ExecutionEvent>) {
    let mut state = ExecutionState::default();

    loop {
        tokio::select! {
            Some(q) = order_rx.recv() => {
                state.place_or_cancel(q, &orderbook).await;
            }

            Some(trade) = trade_rx.recv() => {
                if let Some(fill) = state.on_trade(trade, &orderbook).await {
                    exec_tx.send(fill).await.unwrap();
                }
            }
        }
    }
}