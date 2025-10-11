use clickhouse_orm::{ClickHouseTable, DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, ClickHouseTable)]
#[table_name = "page_views"]
#[clickhouse(engine = "MergeTree")]
#[clickhouse(order_by = "event_time")]
#[clickhouse(partition_by = "toYYYYMM(event_time)")]
pub struct PageView {
    pub event_time: DateTime<Utc>,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}
