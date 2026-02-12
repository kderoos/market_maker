use crate::output::event::OutputEvent;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

struct CsvFile {
    writer: BufWriter<std::fs::File>,
    buffer: Vec<String>,
}

impl CsvFile {
    fn new(path: PathBuf, header: &str) -> Self {
        println!("Creating CSV file at: {}", path.display());
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .expect("Cannot open CSV file");

        let mut writer = BufWriter::new(file);
        writeln!(writer, "{}", header).unwrap();

        Self {
            writer,
            buffer: Vec::with_capacity(256),
        }
    }

    fn push_line_buffer(&mut self, line: String) {
        self.buffer.push(line);
        if self.buffer.len() >= 256 {
            self.flush();
        }
    }
    fn push_single_line(&mut self, line: String) {
        if let Err(e) = writeln!(self.writer, "{}", line) {
            eprintln!("csv write error: {}", e);
            return;
        }
        if let Err(e) = self.writer.flush() {
            eprintln!("csv flush error: {}", e);
        }
    }

    fn flush(&mut self) {
        for line in self.buffer.drain(..) {
            let _ = writeln!(self.writer, "{}", line);
        }
        let _ = self.writer.flush();
    }
}

pub struct SimpleCsvSink {
    trades: CsvFile,
    orders: CsvFile,
    positions: CsvFile,
    candles: CsvFile,
}

impl SimpleCsvSink {
    pub fn new(base_path: impl Into<PathBuf>, filename_prefix: &str) -> Self {
        let base = base_path.into();
        

        Self {
            trades: CsvFile::new(
                base.join(format!("{}_trades.csv", filename_prefix)),
                "symbol,ts_received,price,size,side,order_id,action",
            ),
            orders: CsvFile::new(
                base.join(format!("{}_orders.csv", filename_prefix)),
                "symbol,ts,price,size,side,client_id,type",
            ),
            positions: CsvFile::new(
                base.join(format!("{}_positions.csv", filename_prefix)),
                "timestamp,avg_price,realized_pnl,cash",
            ),
            candles: CsvFile::new(
                base.join(format!("{}_candles.csv", filename_prefix)),
                "symbol,ts_open,open,high,low,close,volume",
            ),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<OutputEvent>) {
        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event {
                        OutputEvent::Trade(trade) => {
                            let line = format!("{},{},{},{},{},{},{}",
                                trade.symbol,
                                trade.ts_received,
                                trade.price,
                                trade.size,
                                trade.side,
                                trade.order_id,
                                trade.action
                            );
                            self.trades.push_line_buffer(line);
                        }
                        OutputEvent::Order(order) => {
                            let ts = chrono::Utc::now().timestamp_micros();
                            let line = match order {
                                common::Order::Limit { symbol, side, price, size, client_id, .. } =>
                                    format!("{},{},{},{},{},{},Limit", symbol, ts, price, size, side, client_id.unwrap_or(0)),
                                // common::Order::Market { symbol, side, size, client_id, .. } =>
                                    // format!("{},{},{},{},{},{},Market", symbol, ts, 0.0, size, side, client_id.unwrap_or(0)),
                                _ => continue,
                            };
                            self.orders.push_line_buffer(line);
                        }
                        OutputEvent::Position(pos) => {
                            let line = format!("{},{},{},{},{}",
                                pos.timestamp,
                                pos.position,
                                pos.avg_price,
                                pos.realized_pnl,
                                pos.cash,
                            );
                            self.positions.push_line_buffer(line);
                        }
                        OutputEvent::Price(c) => {
                            let line = format!("{},{},{},{},{},{},{}",
                                c.symbol,
                                c.ts_open,
                                c.open,
                                c.high,
                                c.low,
                                c.close,
                                c.volume,
                            );
                            self.candles.push_single_line(line);
                        }
                    }
                }
                _ = sleep(Duration::from_millis(500)) => {
                    self.trades.flush();
                    self.orders.flush();
                    self.positions.flush();
                    self.candles.flush();
                }
            }
        }
    }
}
