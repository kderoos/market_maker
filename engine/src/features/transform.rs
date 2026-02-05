use tokio::sync::mpsc;
use common::{AnyUpdate, StrategyInput};

pub trait DataTransformer: Send {
    /// DataTransformer trait make it possible to transform raw engine updates, like Quote and Trade updates, into strategy-specific inputs. This allows strategies to receive the data they need in the format they expect, without having to handle the raw updates themselves.
    /// other derived features like moving average price or sliding window volatility estimate.

    /// Spawns the DataTransformer pipeline. Used to transform raw engine updates into strategy inputs.
    /// Returns:
    /// - tx_in: where engine sends raw AnyUpdate
    /// - rx_out: what strategy consumes as StrategyInput
    fn spawn(self: Box<Self>) -> (mpsc::Sender<AnyUpdate>, mpsc::Receiver<StrategyInput>);
}
