use tokio::sync::mpsc;
use common::{AnyUpdate, StrategyInput};
use super::transform::DataTransformer;
/// Data transformer that passes through AnyUpdate as StrategyInput without modification.
pub struct PassThroughDataTransformer;
impl DataTransformer for PassThroughDataTransformer {
    fn spawn(self: Box<Self>) -> (mpsc::Sender<AnyUpdate>, mpsc::Receiver<StrategyInput>) {
        let (tx_in, mut rx_in) = mpsc::channel::<AnyUpdate>(9_000);
        let (tx_out, rx_out) = mpsc::channel::<StrategyInput>(0_000);

        tokio::spawn(async move {
            while let Some(update) = rx_in.recv().await {
                if let Some(si) = StrategyInput::from_any_update(update) {
                    let _ = tx_out.send(si).await;
                }
            }
        });

        (tx_in, rx_out)
    }
}
