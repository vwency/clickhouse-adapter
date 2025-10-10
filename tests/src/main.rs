use chrono::Utc;
use clickhouse_orm::CHClient;
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

    if let Err(e) = users.create_table().await {
        error!("Failed to create table 'users': {}", e);
    } else {
        info!("Table 'users' created successfully");
    }

    if let Err(e) = page_views.create_table().await {
        error!("Failed to create table 'page_views': {}", e);
    } else {
        info!("Table 'page_views' created successfully");
    }

    let now = Utc::now().timestamp() as u32;
    let user =
        User { id: 1, email: "alice@example.com".into(), created_at: now, last_seen: Some(now) };

    info!("Inserting one user...");
    if let Err(e) = users.insert_one(&user).await {
        error!("Failed to insert user: {}", e);
    } else {
        info!("Inserted user successfully!");
    }

    let rows = users.fetch_all(false).await?;
    info!("Fetched {} users from DB:", rows.len());
    for u in &rows {
        println!("{:?}", u);
    }

    let emails = users.select_columns::<String>(&["email"], false).await?;
    info!("Fetched {} emails:", emails.len());
    for email in emails {
        println!("Email: {}", email);
    }

    if let Ok(parts_info) = users.get_parts_info().await {
        info!("Parts info for 'users' table:");
        for part in parts_info {
            println!(
                "Partition: {}, Name: {}, Rows: {}, Bytes: {}",
                part.partition, part.name, part.rows, part.bytes
            );
        }
    }

    Ok(())
}
