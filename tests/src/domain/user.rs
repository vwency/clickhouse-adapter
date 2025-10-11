use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[ch_table = "users"]
#[ch_config(engine = "MergeTree", order_by = "id")]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: u32,
    pub last_seen: Option<u32>,
}
