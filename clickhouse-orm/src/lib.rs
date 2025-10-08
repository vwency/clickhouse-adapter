pub mod domain;
pub mod infrastructure;
pub mod usecase;
pub use chrono::{DateTime, Utc};
pub use clickhouse;
pub use clickhouse::Row;
pub use clickhouse_orm_macros::ClickHouseTable;
pub use domain::client::client::CHClient;
pub use domain::engine::Engine;
pub use domain::errors::default::CHError;
pub use domain::query::query::Query;
pub use domain::repository::repository::Repository;
pub use infrastructure::adapters::engine::engine_options::{MergeTreeOps, ReplicatedMergeTreeOps};
pub use serde::{Deserialize, Serialize};
pub use usecase::query::query::AggregateQuery;
pub use usecase::repository::replicated_merge_tree::replicated_merge_tree_ops::*;
pub use usecase::repository::*;
pub trait ClickHouseTable {
    fn table_name() -> &'static str;
    fn create_table_sql() -> &'static str;
    fn engine() -> Engine;
}

pub use domain::engine::{MergeTreeFlag, ReplicatedMergeTreeFlag};
