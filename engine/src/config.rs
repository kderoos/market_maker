use serde::{Serialize,Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct EngineConfig {
    pub strategy: StrategyConfig,
    pub data: DataConfig,
    pub execution: ExecutionConfig,
    pub output: OutputConfig,
    pub position: PositionEngineConfig,
}
#[derive(Serialize, Deserialize, Clone)]
pub enum DataPreprocessingType {
    Avellaneda,
    NoDataTransform,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum StrategyType {
    Avellaneda,
    // Momentum,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StrategyConfig {
    pub kind: StrategyType, // "avellaneda", "momentum", etc
    pub preprocessing: DataPreprocessingType, // "avellaneda", "momentum", etc
    pub symbol: String,
    pub avellaneda: Option<AvellanedaConfig>,
    pub momentum: Option<MomentumConfig>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AvellanedaConfig {
    pub tick_size: f64,
    pub quote_size: i64,
    pub max_position: i64,
    pub gamma: f64,
    pub delta: f64,
    pub xi: f64,

    // feature params
    pub vol: VolatilityConfig,
    pub penetration: PenetrationConfig,
    pub sequencer: SequencerConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MomentumConfig {
    pub lookback: usize,
    pub threshold: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VolatilityConfig {
    pub window_len: usize,
    pub interval_s: f64,
    pub ema_half_life: f64,
    pub sigma_min: Option<f64>,
    pub sigma_max: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PenetrationConfig {
    pub window_len: usize,
    pub num_bins: usize,
    pub interval_ms: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SequencerConfig {
    pub interval_ms: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DataConfig {
    pub from_csv: bool,
    pub bitmex_api: Option<String>,
    pub tardis_root: Option<String>,
    pub trades_csv: Option<String>,
    pub quotes_csv: Option<String>,
    pub book_csv: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExecutionConfig {
    pub latency_ms: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OutputConfig {
    pub csv_path: String,
    pub exchange: String,
    pub date: String,
    pub candle_size_min: Option<u64>,
}
use common::ContractType;
#[derive(Serialize, Deserialize, Clone)]
pub struct PositionEngineConfig {
    pub contract_type: ContractType,
    pub fee_rate: f64,
}
