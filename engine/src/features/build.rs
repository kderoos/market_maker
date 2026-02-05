use super::transform::DataTransformer;
use super::passthrough::PassThroughDataTransformer;
use super::sequenced::AvellanedaDataTransformer;
use crate::config::StrategyConfig;

pub enum TransformerKind {
    PassThrough,
    Sequencer,
}

pub fn build_transformer(cfg: &StrategyConfig) -> Box<dyn DataTransformer> {
    match cfg.kind.as_str() {
        "momentum" => Box::new(PassThroughDataTransformer),
        "avellaneda" => Box::new(AvellanedaDataTransformer { symbol: cfg.symbol.clone() }),
        other => panic!("Unknown strategy kind: {}", other),
    }
}
