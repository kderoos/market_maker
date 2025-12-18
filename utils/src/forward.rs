use tokio::sync::broadcast;
use common::{AnyUpdate, AnyWsUpdate};

pub async fn ws_forwarder(
    mut rx: broadcast::Receiver<AnyUpdate>,
    tx_ws: broadcast::Sender<AnyWsUpdate>,
) {
    while let Ok(update) = rx.recv().await {
        match update {
            AnyUpdate::TradeUpdate(t) => {
                let _ = tx_ws.send(AnyWsUpdate::Trade(t));
            }
            AnyUpdate::QuoteUpdate(q) => {
                let _ = tx_ws.send(AnyWsUpdate::Quote(q));
            }
            // Ignore BookUpdate, they are internal-only
            _ => {}
        }
    }
}
