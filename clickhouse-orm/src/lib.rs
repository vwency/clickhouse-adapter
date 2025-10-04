pub mod client;
pub mod domain;
pub mod engine;
pub mod query;
pub mod repository;
pub use clickhouse::Row;
pub use clickhouse_orm_macros::ClickHouseTable;
pub use domain::client::client::CHClient;
pub use domain::errors::default::CHError;
pub use domain::repository::repository::Repository;
pub use engine::{Engine, MergeTreeOps, ReplicatedMergeTreeOps};
pub use query::{AggregateQuery, Query};

pub use chrono::{DateTime, Utc};
pub use clickhouse;
pub use serde::{Deserialize, Serialize};

pub trait ClickHouseTable {
    fn table_name() -> &'static str;
    fn create_table_sql() -> &'static str;
    fn engine() -> Engine; // НОВОЕ!
}
