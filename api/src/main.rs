use std::fs;
use engine::{config::EngineConfig, Engine};
use tracing_subscriber;

pub fn load_config(path: &str) -> EngineConfig {
    let text = fs::read_to_string(path).expect("Failed to read config");
    toml::from_str(&text).expect("Invalid config.toml")
}
#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();
    let cfg = load_config("./config/avellaneda.toml");
    let engine = Engine::init(cfg);

    // Keep process alive
    futures::future::pending::<()>().await;
}
