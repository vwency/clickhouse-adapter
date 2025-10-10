use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub async fn create_table(&self) -> Result<()> {
        let sql = T::create_table_sql();
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
}
