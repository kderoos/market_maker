use thiserror::Error;
use std::path::PathBuf;

#[derive(Error, Debug)]
pub enum TardisError {
    /// Context-rich error when opening a specific CSV file
    #[error("failed to open CSV file: {path}")]
    OpenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Context-rich parse error with file and row information.
    #[error("CSV parse error in {path} at row {row}")]
    CsvParse {
        path: PathBuf,
        row: u64,
        #[source]
        source: csv::Error,
    },
    /// Lightweight/io-friendly variant so you can use `?` on std::io::Error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Lightweight/csv-friendly variant so you can use `?` on csv::Error
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    /// Catch-all for other error types (optional)
    #[error("unexpected error: {0}")]
    Other(#[from] anyhow::Error),

}
