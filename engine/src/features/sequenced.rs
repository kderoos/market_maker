use tokio::sync::mpsc;
use common::{AnyUpdate, StrategyInput};
use super::transform::DataTransformer;
use strategy::{sequencer,runner::run_strategy};
use crate::penetration;
use crate::volatility::run_tick_volatility_sample_ema;
use crate::config::AvellanedaConfig;
use common::QuoteUpdate;

/// Data transformer that prepares data for Avellaneda-Stoikov strategy.
/// Strategy requires:
/// - Volatility estimates
/// - Liquidity estimates (A, k) obtained by fitting exponential decay to price impact data.
/// - Quote updates to get best bid/ask prices.
/// 
/// It fans out the AnyUpdate stream into Quote and Trade steams to compute the required aggregates. Then a sequencer
/// combines these streams into AvellanedaInput structs which are sent to the strategy.
///
/// AnyUpdate
///   |
///   v
/// Fan-out
///   |
///   +--> [Trade+Quote] --> Penetration Engine --> PenetrationUpdate --+
///   |                                                                 |
///   +--> [Quote]        --> Volatility Engine  --> VolatilityUpdate ---+--> Sequencer --> StrategyInput
///   |                                                                 |
///   +--> [Quote]        ----------------------------------------------+

pub struct AvellanedaDataTransformer {
    pub symbol: String,
    pub cfg: AvellanedaConfig,
}
impl DataTransformer for AvellanedaDataTransformer {
    fn spawn(self: Box<Self>) -> (mpsc::Sender<AnyUpdate>, mpsc::Receiver<StrategyInput>) {
        let (tx_in, mut rx_in) = mpsc::channel::<AnyUpdate>(9_000);
        let (tx_out, rx_out) = mpsc::channel::<StrategyInput>(1_000);

        // Spawn separate tasks for each feature stream (vol, pen, quote)
        // Each task maintains its own state and updates it based on incoming AnyUpdates
        // The sequencer task listens to all feature streams and emits AvellanedaInput when all required data is available

        // fan out to vol, pen, quote
        let (tx_trade_quote_pen, mut rx_trade_quote_pen) = mpsc::channel::<AnyUpdate>(4_000);
        let (tx_quote_vol, mut rx_quote_vol) = mpsc::channel::<AnyUpdate>(4_000);
        let (tx_quote_seq, mut rx_quote_seq) = mpsc::channel::<QuoteUpdate>(4_000);
        tokio::spawn(async move {
            loop {
                let update = rx_in.recv().await;
                match update {
                    Some(update) => {
                        match &update {
                            AnyUpdate::BookUpdate(_) => {
                                    continue;
                            }
                            AnyUpdate::TradeUpdate(_) => {
                                let _ = tx_trade_quote_pen.send(update.clone()).await;
                            }
                            AnyUpdate::QuoteUpdate(quote) => {
                                let _ = tx_trade_quote_pen.send(update.clone()).await;
                                let _ = tx_quote_vol.send(update.clone()).await;
                                let _ = tx_quote_seq.send(quote.clone()).await;
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        });

        // Create sequencer channels
        let (tx_vol, mut rx_vol) = mpsc::channel::<common::VolatilityUpdate>(1000);
        let (tx_pen, mut rx_pen) = mpsc::channel::<common::PenetrationUpdate>(1000);
        let (tx_out, mut rx_out) = mpsc::channel::<common::StrategyInput>(1000);
        
        // Spawn engine tasks
        tokio::spawn(run_tick_volatility_sample_ema(
            rx_quote_vol,
            tx_vol,
            self.cfg.vol.window_len,
            self.cfg.vol.interval_s,
            self.cfg.vol.ema_half_life,
            self.cfg.vol.sigma_min,
            self.cfg.vol.sigma_max,
            self.symbol.clone(),
        ));
        // Spawn penetration analyzer
        tokio::spawn(penetration::engine(
            rx_trade_quote_pen,
            tx_pen,
            self.cfg.penetration.window_len,
            self.cfg.penetration.num_bins,
            self.cfg.penetration.interval_ms,
            self.symbol.clone(),
        ));

        // Spawn sequencer
        tokio::spawn(sequencer::sequencer_run(
            self.cfg.sequencer.interval_ms,
            rx_vol,
            rx_pen,
            rx_quote_seq,
            tx_out.clone(),
        ));


        (tx_in, rx_out)
    }
}