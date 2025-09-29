use clickhouse_orm::{CHClient, ClickHouseTable};
use tests::domain::{PageView, User};
use tracing::{info, error, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    info!("Starting ClickHouse ORM example");

    // Передайте логин и пароль (например, user = "default", password = "ваш_пароль")
    let client = CHClient::with_credentials(
        "http://localhost:8123",
        "default",
        "default",
        "default",
    );

    let page_views = PageView::repository(client.clone());
    let users = User::repository(client.clone());

    info!("Creating tables...");

    match page_views.create_table().await {
        Ok(_) => info!("Table '{}' created successfully", PageView::table_name()),
        Err(e) => error!("Failed to create table '{}': {}", PageView::table_name(), e),
    }

    match users.create_table().await {
        Ok(_) => info!("Table '{}' created successfully", User::table_name()),
        Err(e) => error!("Failed to create table '{}': {}", User::table_name(), e),
    }

    Ok(())
}