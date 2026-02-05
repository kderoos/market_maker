#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub kind: String,
    pub symbol: String,
}

impl StrategyConfig {
    pub fn new(kind: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self { kind: kind.into(), symbol: symbol.into() }
    }
}