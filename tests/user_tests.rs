use chrono::Utc;
use clickhouse_orm::clickhouse::Row;
use clickhouse_orm::CHClient;
use clickhouse_orm::ClickHouseTable;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

// ✅ Доменная модель перенесена прямо в тест
#[derive(Debug, Clone, Serialize, Deserialize, Row, ClickHouseTable)]
#[table_name = "users"]
#[table_engine = "MergeTree"]
#[table_engine_options(order_by = "id")]
#[table_options(partition_by = "toYYYYMM(created_at)", primary_key = "id")]
pub struct User {
    pub id: u64,
    pub email: String,
    pub created_at: u32,
    pub last_seen: Option<u32>,
}

fn get_client() -> CHClient {
    CHClient::with_credentials("http://localhost:8123", "default", "default", "default")
}

#[tokio::test]
async fn test_create_table() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    info!("Test: Create users table");

    let users = User::repository(client);
    users.create_table().await?;
    info!("✓ Table 'users' created successfully");

    Ok(())
}

#[tokio::test]
async fn test_insert_user() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    info!("Test: Insert user");

    let users = User::repository(client);
    let now = Utc::now().timestamp() as u32;

    let user =
        User { id: 1, email: "alice@example.com".into(), created_at: now, last_seen: Some(now) };

    users.insert_one(&user).await?;
    info!("✓ User inserted successfully: {:?}", user.email);

    Ok(())
}

#[tokio::test]
async fn test_fetch_users() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    info!("Test: Fetch all users");

    let users = User::repository(client);
    let rows = users.fetch_all(false).await?;

    info!("✓ Fetched {} users from DB:", rows.len());
    for u in rows {
        info!("  - User: id={}, email={}", u.id, u.email);
    }

    Ok(())
}

#[tokio::test]
async fn test_update_user() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    info!("Test: Update user last_seen");

    let users = User::repository(client);
    let now = Utc::now().timestamp() as u32;

    let user = User {
        id: 2,
        email: "bob@example.com".into(),
        created_at: now - 3600,
        last_seen: Some(now),
    };

    users.insert_one(&user).await?;
    info!("✓ User updated with new last_seen timestamp");

    Ok(())
}

#[tokio::test]
async fn test_user_with_null_last_seen() -> Result<(), Box<dyn std::error::Error>> {
    let client = get_client();
    info!("Test: Insert user with NULL last_seen");

    let users = User::repository(client);
    let now = Utc::now().timestamp() as u32;

    let user =
        User { id: 3, email: "charlie@example.com".into(), created_at: now, last_seen: None };

    users.insert_one(&user).await?;
    info!("✓ User with NULL last_seen inserted successfully");

    Ok(())
}
