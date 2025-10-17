pub mod application;
pub mod domain;
pub mod infrastructure;
pub use application::usecase::repository::replicated_merge_tree::*;
pub use application::usecase::repository::*;
pub use chrono::{DateTime, Utc};
pub use clickhouse;
pub use clickhouse_orm_macros::ClickHouseTable;
pub use domain::client::client::CHClient;
pub use domain::engine::Engine;
pub use domain::errors::default::CHError;
pub use domain::repository::repository::Repository;
pub use serde::{Deserialize, Serialize};
pub trait ClickHouseTable {
    fn table_name() -> &'static str;
    fn create_table_sql() -> &'static str;
    fn engine() -> Engine;
}

pub use domain::engine::{MergeTreeFlag, ReplicatedMergeTreeFlag};
