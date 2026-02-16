## Async Event-Driven Trading Engine in Rust
An asynchronous, event-driven trading engine written in Rust supporting both live exchange connectivity (BitMEX WebSocket API) and deterministic historical replay using Tardis CSV data. The system is built around a Tokio-based message-passing architecture that decouples ingestion, preprocessing, strategy evaluation, execution simulation, position tracking, and output sinks.

The project emphasizes determinism during replay, bounded channel backpressure, and modular components for reproducible strategy research.

## Key Features
- Tick-level trade, quote and order book ingestion.
- Fully asynchronous Tokio runtime using mpsc and broadcast channels.
- Event-driven fan-out architecture with bounded buffers.
- Live exchange connector (BitMEX WebSocket API).
- Deterministic historical replay using Tardis CSV data.
- Pluggable strategy interface with configurable preprocessing.
- Deterministic execution simulator with latency modeling.
- Probabilistic order fill model.
- Position tracking with P&L calculation.
- Config-driven runtime (TOML).
- Implementation of the Avellaneda–Stoikov optimal market making model [1].
- Extensions inspired by Guéant’s optimal market making framework [2].

# Components
```
                   + ----------------+    +----------+    
                   | DataTransformer | -> | Strategy |
        trade,     + ----------------+    +----------+
        quote   /                             |
               /                              |
+-----------+                                 | Order
| Connector |                                /
+-----------+               +-----------+
                ------->    | Execution |
              trade, book   +-----------+
                               |
                ExecutionEvent +-------------+
                               |             |
                    +-----------+       +--------+
                    | Position  |  -->  | Output |
                    +-----------+       +--------+
                                PositionEvent

```
Market data is ingested by a connector and fanned out to both the execution engine and the strategy pipeline. The strategy produces orders, which are executed by a deterministic execution engine. Execution events are then fanned out to the position engine and output sinks.

**Connector:** Retrieves market data either from the BitMEX WebSocket API (live mode) or from Tardis CSV files (replay mode). Normalizes trade, quote, and book updates into unified event types and forwards them to the event router.

**DataTransformer:** Optional preprocessing layer that transforms raw market updates into strategy-ready inputs (e.g., feature aggregation or Avellaneda-style data preparation).

**Strategy:** Consumes structured inputs and produces Order decisions (side, price, size) based on configurable strategy logic.

**Execution Engine:** Maintains an internal L2 order book (only total size at price) representation from connector updates. Applies latency modeling to incoming strategy orders and simulates fills using trade events and a probabilistic fill model. Emits ExecutionEvents.

**Position Engine:** Consumes execution events and maintains position state, exposure, and P&L statistics.

**Output Sink:** Receives execution events, position updates, and strategy decisions. Writes structured output to CSV and/or console based on configuration.



# Limitations & Future Work

This is a portfolio and research project, not production trading software. Areas for future improvement include:

- More robust error handling and validation.
- Additional strategies and execution models.
- Include market orders (currently only limit orders are implemented)
- More detailed backtest metrics and reporting.
- Improved CLI and configuration ergonomics.
- Extended test coverage and benchmarking.

# How to run
## Build
From the project root:
```bash
cargo build --release
```
Binary location:
```bash
target/release/mm
```
## Quickstart
```bash
target/release/mm backtest --config example/backtest.toml
```
or for real-time BitMEX mode:
```bash
target/release/mm live --config example/live.toml
```

## Configuration
The engine is fully configuration-driven via a TOML file.

Both 'live' and 'backtest' modes require the config flag:
```bash
--config path/to/config.toml
```

An example config can be found at `config/avellaneda.toml`:
```toml
[strategy]
kind = "Avellaneda"
preprocessing = "Avellaneda"
symbol = "XBTUSD"

[strategy.avellaneda]
tick_size = 0.01
quote_size = 100
max_position = 1000
gamma = 0.1
delta = 0.2
xi = 0.1

[strategy.avellaneda.vol]
window_len = 500
interval_s = 0.5
ema_half_life = 2.0

[strategy.avellaneda.penetration]
window_len = 120
num_bins = 500
interval_ms = 500

[strategy.avellaneda.sequencer]
interval_ms = 500

[data]
from_csv = true
tardis_root = "example/data_sample/"
trades_csv = "trades.csv.gz"
quotes_csv = "quotes.csv.gz"
book_csv = "book.csv.gz"

[execution]
latency_ms = 0

[output]
csv_path = "example/run/"
exchange = "bitmex"
date = "01_08_2024"
candle_size_min = 1

[position]
contract_type = "Inverse"
fee_rate = 0.0005
```
Enum values are case-sensitive (e.g. `Avellaneda`)

## Backtest mode
### Data (Tardis CSV)

Backtest mode expects three gzip-compressed CSV files:

| Config Field  | Tardis Dataset Name      | Description                              |
|---------------|--------------------------|------------------------------------------|
| `trades_csv`  | `trades`                 | Tick-level trade prints                  |
| `quotes_csv`  | `quotes`                 | Best bid/ask updates                     |
| `book_csv`    | `incremental_book_L2`    | Incremental Level 2 order book updates   |

These files can be downloaded from Tardis.dev in downloadable CSV format:
https://docs.tardis.dev/downloadable-csv-files

Example directory structure:
```bash
example/data_sample/
├── trades.csv.gz
├── quotes.csv.gz
└── book.csv.gz # incremental_book_L2 dataset
```

In the configuration file:

```toml
[data]
from_csv = true
tardis_root = "example/data_sample/"
trades_csv = "trades.csv.gz"
quotes_csv = "quotes.csv.gz"
book_csv = "book.csv.gz"
```

The first hour of data of BitMEX XBTUSD at 01-08-2024 is included in `example/data_sample/`.

### Basic usage
```bash
target/release/mm backtest \
            --config config.toml \
```
### Override data directory
```bash
target/release/mm backtest \
            --config config.toml \
            --data /mnt/tardis_data
```
This overrides `cfg.data.tardis_root`.

### Override output directory
```bash
target/release/mm backtest \
            --config config.toml \
            --data /mnt/tardis_data \
            --output ./results/
```


## References

[1] M. Avellaneda and S. Stoikov.  
*High-frequency trading in a limit order book.*  
Quantitative Finance, 8(3):217–224, 2008.  
https://people.orie.cornell.edu/sfs33/LimitOrderBook.pdf

[2] O. Guéant.  
*Optimal market making.*  
arXiv:1605.01862, 2017.  
https://arxiv.org/abs/1605.01862

