use chrono::{TimeZone, Utc};
use clickhouse_orm::{CHClient, ClickHouseTable};
use tests::domain::{PageView, User};
use tracing::{error, info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();

    info!("Starting ClickHouse ORM example");

    let client =
        CHClient::with_credentials("http://localhost:8123", "default", "default", "default");

    let users = User::repository(client.clone());
    let page_views = PageView::repository(client.clone());

    // === 1️⃣ Создание таблиц ===
    // Create users table
    match users.create_table().await {
        Ok(_) => info!("Table 'users' created successfully"),
        Err(e) => error!("Failed to create table 'users': {}", e),
    }

    // Create page_views table
    match page_views.create_table().await {
        Ok(_) => info!("Table 'page_views' created successfully"),
        Err(e) => error!("Failed to create table 'page_views': {}", e),
    }

    // === 2️⃣ Вставка одной записи (insert_one) ===
    let now = Utc::now().timestamp() as u32;

    let user =
        User { id: 1, email: "alice@example.com".into(), created_at: now, last_seen: Some(now) };

    info!("Inserting one user...");
    if let Err(e) = users.insert_one(&user).await {
        error!("Failed to insert user: {}", e);
    } else {
        info!("Inserted user successfully!");
    }

    let query = users.query();
    let sql = format!("SELECT * FROM {}", User::table_name());
    let rows = query.client.query(&sql).fetch_all::<User>().await?;

    info!("Fetched {} users from DB:", rows.len());
    for u in rows {
        println!("{:?}", u);
    }

    Ok(())
}
