use strategy::{avellaneda::AvellanedaStrategy, traits::Strategy};
use crate::config::{StrategyConfig, StrategyType::Avellaneda};

pub fn build_strategy(cfg: &StrategyConfig) -> Box<dyn Strategy + Send> {
    match cfg.kind {
        Avellaneda => {
            let c = cfg.avellaneda.as_ref().expect("Missing avellaneda config");
            Box::new(AvellanedaStrategy::new(
                cfg.symbol.clone(),
                c.tick_size,
                c.quote_size,
                c.max_position,
                c.gamma,
                c.delta,
                c.xi,
            ))
        }
        // Add other strategies here
    }
}
