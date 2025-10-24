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
async fn test_insert_one() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;

    let pv = PageView {
        id: 999001,
        event_time: 123456789,
        user_id: 42,
        page_url: "/test".into(),
        country: "RU".into(),
        device_type: "mobile".into(),
    };

    repo.insert_one(&pv).await?;
    info!("Inserted single row");
    Ok(())
}

#[tokio::test]
async fn test_insert_many() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;

    let batch = vec![
        PageView {
            id: 999002,
            event_time: 123456791,
            user_id: 43,
            page_url: "/bulk".into(),
            country: "US".into(),
            device_type: "desktop".into(),
        },
        PageView {
            id: 999003,
            event_time: 123456792,
            user_id: 44,
            page_url: "/bulk2".into(),
            country: "UK".into(),
            device_type: "mobile".into(),
        },
    ];

    repo.insert_many(&batch).await?;
    info!("Inserted batch rows");
    Ok(())
}

#[tokio::test]
async fn test_update_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;

    repo.update_where("device_type = 'smart-tv'", "user_id = 42").await?;
    info!("Updated rows");
    Ok(())
}

#[tokio::test]
async fn test_delete_where() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::get_client();
    let repo = PageView::repository(client);
    repo.create_table_if_not_exists().await?;

    repo.delete_where("id >= 999000").await?;
    info!("Deleted rows");
    Ok(())
}
