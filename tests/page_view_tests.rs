use chrono::Utc;
use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};
use tracing::info;

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

mod common;
use common::get_client;

#[tokio::test]
async fn test_create_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    let repo = PageView::repository(client);
    repo.create_table().await?;
    info!("Table page_views created");
    Ok(())
}

#[tokio::test]
async fn test_insert_page_view() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    let repo = PageView::repository(client);

    let page_view = PageView {
        id: 1,
        event_time: Utc::now().timestamp() as u32,
        user_id: 1,
        page_url: "https://example.com/home".into(),
        country: "US".into(),
        device_type: "desktop".into(),
    };

    repo.insert_one(&page_view).await?;
    info!("Inserted page_view successfully");
    Ok(())
}
