use chrono::Utc;
use clickhouse_orm::CHClient;
use tests::domain::page_view::PageView;
use tests::domain::User;
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

    if let Err(e) = users.insert_one(&user).await {
        error!("Failed to insert user: {}", e);
    } else {
        info!("Inserted user successfully!");
    }
    let page_view = PageView {
        id: 1,
        event_time: Utc::now().timestamp() as u32,
        user_id: 1,
        page_url: "https://example.com/home".into(),
        country: "US".into(),
        device_type: "desktop".into(),
    };
    if let Err(e) = page_views.insert_one(&page_view).await {
        error!("Failed to insert page view: {}", e);
    } else {
        info!("Inserted page view successfully!");
    }

    let users_rows = users.fetch_all(false).await?;
    info!("Fetched {} users from DB:", users_rows.len());
    for u in &users_rows {
        println!("{:?}", u);
    }

    let page_views_rows = page_views.fetch_all(false).await?;
    info!("Fetched {} page views from DB:", page_views_rows.len());
    for pv in &page_views_rows {
        println!("{:?}", pv);
    }

    Ok(())
}
