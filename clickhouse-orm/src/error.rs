use thiserror::Error;

#[derive(Error, Debug)]
pub enum CHError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Query error: {0}")]
    Query(String),
}

pub type Result<T> = std::result::Result<T, CHError>;
