use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, ClickHouseTable)]
#[table_name = "users"]
#[clickhouse(engine = "MergeTree", order_by = "id")]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: u32,
    pub last_seen: Option<u32>,
}
