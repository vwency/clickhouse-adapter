use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::{ClickHouseTable, DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[ch_table = "page_views"]
#[ch_config(engine = "MergeTree", order_by = "event_time", partition_by = "toYYYYMM(event_time)")]
pub struct PageView {
    pub event_time: DateTime<Utc>,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}
