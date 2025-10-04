use thiserror::Error;

#[derive(Error, Debug)]
pub enum CHError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

pub type Result<T> = std::result::Result<T, CHError>;
