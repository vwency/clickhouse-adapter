pub mod client;
pub mod engine;
pub mod error;
pub mod query;
pub mod repository;

pub use clickhouse::Row;
pub use clickhouse_orm_macros::ClickHouseTable;
pub use client::CHClient;
pub use engine::{Engine, MergeTreeOps, ReplicatedMergeTreeOps};
pub use error::CHError;
pub use query::{AggregateQuery, Query};
pub use repository::Repository;

pub use chrono::{DateTime, Utc};
pub use clickhouse;
pub use serde::{Deserialize, Serialize};

pub trait ClickHouseTable {
    fn table_name() -> &'static str;
    fn create_table_sql() -> &'static str;
    fn engine() -> Engine; // НОВОЕ!
}
