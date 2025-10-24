use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_create"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewCreate {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_exists"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewExists {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_add_col"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewAddCol {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_modify_col"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewModifyCol {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_drop_col"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewDropCol {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_rename"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewRename {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_truncate"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewTruncate {
    pub id: u64,
    pub event_time: u32,
    pub user_id: u64,
    pub page_url: String,
    pub country: String,
    pub device_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "page_views_drop"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "event_time")]
#[table_options(partition_by = "toYYYYMM(event_time)", primary_key = "id")]
pub struct PageViewDrop {
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
async fn test_create_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewCreate::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    info!("Table created if not exists");
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewExists::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    let exists = repo.table_exists().await?;
    info!("Table exists: {}", exists);
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_add_column() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewAddCol::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.add_column("extra", "String").await?;
    info!("Column added");
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_modify_column() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewModifyCol::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.add_column("extra", "String").await?;
    repo.modify_column("extra", "Nullable(String)").await?;
    info!("Column modified");
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_column() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewDropCol::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.add_column("extra", "String").await?;
    repo.drop_column("extra").await?;
    info!("Column dropped");
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_rename_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewRename::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.rename_table("page_views_rename_temp").await?;
    info!("Table renamed");
    let sql = "DROP TABLE IF EXISTS page_views_rename_temp";
    repo.execute_raw(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_truncate_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewTruncate::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.truncate_table().await?;
    info!("Table truncated");
    repo.drop_table().await?;
    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageViewDrop::repository(client);
    repo.drop_table().await?;
    repo.create_table_if_not_exists().await?;
    repo.drop_table().await?;
    info!("Table dropped");
    Ok(())
}
