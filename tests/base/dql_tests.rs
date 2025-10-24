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

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct CountByCountry {
    pub country: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct PartialPageView {
    pub id: u64,
    pub user_id: u64,
    pub country: String,
}

#[path = "../common.rs"]
mod common;

#[tokio::test]
async fn test_fetch_all() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows = repo.fetch_all(false).await?;
    info!("Fetched {} page_views", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_fetch_one() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let row = repo.fetch_one(false).await?;
    info!("Fetched one page_view: {:?}", row);
    Ok(())
}

#[tokio::test]
async fn test_fetch_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows = repo.fetch_where("country = 'US'", false).await?;
    info!("Fetched {} US page_views", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_fetch_where_limit() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows = repo.fetch_where_limit("device_type = 'desktop'", 10, false).await?;
    info!("Fetched {} desktop page_views (limit 10)", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_fetch_order_by() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows = repo.fetch_order_by("event_time DESC", false).await?;
    info!("Fetched {} page_views ordered by event_time", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_fetch_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows = repo.fetch_with_limit(5, 0, false).await?;
    info!("Fetched {} page_views with limit 5", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_select_columns() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows: Vec<PartialPageView> =
        repo.select_columns(&["id", "user_id", "country"], false).await?;
    info!("Selected {} rows with specific columns", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_select_columns_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let rows: Vec<PartialPageView> = repo
        .select_columns_where(&["id", "user_id", "country"], "device_type = 'mobile'", false)
        .await?;
    info!("Selected {} mobile rows with specific columns", rows.len());
    Ok(())
}

#[tokio::test]
async fn test_count() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let count = repo.count(false).await?;
    info!("Total page_views count: {}", count);
    Ok(())
}

#[tokio::test]
async fn test_count_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let count = repo.count_where("country = 'US'", false).await?;
    info!("US page_views count: {}", count);
    Ok(())
}

#[tokio::test]
async fn test_exists_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let exists = repo.exists_where("country = 'US'", false).await?;
    info!("US page_views exist: {}", exists);
    Ok(())
}

#[tokio::test]
async fn test_distinct_column() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let countries: Vec<String> = repo.distinct_column("country", false).await?;
    info!("Distinct countries: {:?}", countries);
    Ok(())
}

#[tokio::test]
async fn test_group_by() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);

    let results: Vec<CountByCountry> =
        repo.group_by("country, count() as count", "country", false).await?;
    info!("Group by results: {:?}", results);
    Ok(())
}
