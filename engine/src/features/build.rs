use super::transform::DataTransformer;
use super::passthrough::PassThroughDataTransformer;
use super::sequenced::AvellanedaDataTransformer;
use crate::config::{StrategyConfig, DataPreprocessingType::{Avellaneda, NoDataTransform}};

pub enum TransformerKind {
    PassThrough,
    Sequencer,
}

pub fn build_transformer(cfg: &StrategyConfig) -> Box<dyn DataTransformer> {
    match cfg.preprocessing {
        NoDataTransform => Box::new(PassThroughDataTransformer),
        Avellaneda => Box::new(AvellanedaDataTransformer { cfg: cfg.avellaneda.clone()
                                                                    .expect("Avellaneda config must be set for Avellaneda strategy"),
                                                            symbol: cfg.symbol.clone(),
                                                         }),
    }
}
