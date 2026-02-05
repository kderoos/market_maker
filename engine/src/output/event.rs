// use serde::{Deserialize, Serialize};
use common::{Order, ExecutionEvent, Candle, PositionState};
#[derive(Debug, Clone)]
pub enum OutputEvent {
    Order(Order),
    Trade(ExecutionEvent),
    Price(Candle),
    Position(PositionState),
}