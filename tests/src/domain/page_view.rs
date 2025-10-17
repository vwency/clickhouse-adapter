use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageView {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}
