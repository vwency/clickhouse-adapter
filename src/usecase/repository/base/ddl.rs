use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + ClickHouseTable,
{
    pub async fn create_table(&self) -> Result<()> {
        let sql = T::create_table_sql();
        self.execute_raw(&sql).await
    }

    pub async fn create_table_if_not_exists(&self) -> Result<()> {
        let sql = T::create_table_sql().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        self.execute_raw(&sql).await
    }

    pub async fn drop_table(&self) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub async fn truncate_table(&self) -> Result<()> {
        let sql = format!("TRUNCATE TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub async fn rename_table(&self, new_name: &str) -> Result<()> {
        let sql = format!("RENAME TABLE {} TO {}", self.table_name, new_name);
        self.execute_raw(&sql).await
    }

    pub async fn add_column(&self, column_name: &str, column_type: &str) -> Result<()> {
        let sql =
            format!("ALTER TABLE {} ADD COLUMN {} {}", self.table_name, column_name, column_type);
        self.execute_raw(&sql).await
    }

    pub async fn drop_column(&self, column_name: &str) -> Result<()> {
        let sql = format!("ALTER TABLE {} DROP COLUMN {}", self.table_name, column_name);
        self.execute_raw(&sql).await
    }

    pub async fn modify_column(&self, column_name: &str, new_type: &str) -> Result<()> {
        let sql =
            format!("ALTER TABLE {} MODIFY COLUMN {} {}", self.table_name, column_name, new_type);
        self.execute_raw(&sql).await
    }

    pub async fn table_exists(&self) -> Result<bool> {
        let sql = format!(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{}'",
            self.table_name
        );
        let count: u64 = self.client.client().query(&sql).fetch_one::<u64>().await?;
        Ok(count > 0)
    }
}
