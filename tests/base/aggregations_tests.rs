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

#[path = "../common.rs"]
mod common;

#[tokio::test]
async fn test_sum() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: u64 = repo.sum("user_id", false).await?;
    info!("Sum: {}", value);
    Ok(())
}

#[tokio::test]
async fn test_avg() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: f64 = repo.avg("user_id", false).await?;
    info!("Avg: {}", value);
    Ok(())
}

#[tokio::test]
async fn test_min() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: u64 = repo.min("user_id", false).await?;
    info!("Min: {}", value);
    Ok(())
}

#[tokio::test]
async fn test_max() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: u64 = repo.max("user_id", false).await?;
    info!("Max: {}", value);
    Ok(())
}

#[tokio::test]
async fn test_sum_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: u64 = repo.sum_where("user_id", "country = 'US'", false).await?;
    info!("Sum where US: {}", value);
    Ok(())
}

#[tokio::test]
async fn test_avg_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;
    let value: f64 = repo.avg_where("user_id", "device_type = 'mobile'", false).await?;
    info!("Avg where mobile: {}", value);
    Ok(())
}
