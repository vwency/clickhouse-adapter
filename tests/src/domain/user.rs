use clickhouse_orm::{ClickHouseTable, Deserialize, Row, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "users"]
#[clickhouse(engine = "MergeTree")]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: u32,        // Unix timestamp
    pub last_seen: Option<u32>, // Unix timestamp
}
