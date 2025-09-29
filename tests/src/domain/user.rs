use clickhouse_orm::{ClickHouseTable, DateTime, Deserialize, Row, Serialize, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub last_seen: Option<DateTime<Utc>>,
}
