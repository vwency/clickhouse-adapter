use clickhouse_orm::{ClickHouseTable, DateTime, Deserialize, Row, Serialize, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views"]
pub struct PageView {
    pub event_time: DateTime<Utc>,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}
