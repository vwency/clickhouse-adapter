use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "users"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "id")]
#[table_options(partition_by = "toYYYYMM(created_at)", primary_key = "id")]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: u32,
    pub last_seen: Option<u32>,
}
