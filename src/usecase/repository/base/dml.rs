use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use clickhouse::Row;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + Row + ClickHouseTable,
    for<'a> <T as Row>::Value<'a>: Serialize,
{
    pub async fn insert_one(&self, entity: &T) -> Result<()>
    where
        for<'a> &'a T: Into<<T as Row>::Value<'a>>,
    {
        let mut insert = self.client.client().insert::<T>(self.table_name).await?;
        let value: <T as Row>::Value<'_> = entity.into();
        insert.write(&value).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()>
    where
        for<'a> &'a T: Into<<T as Row>::Value<'a>>,
    {
        if entities.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.client().insert::<T>(self.table_name).await?;
        for entity in entities {
            let value: <T as Row>::Value<'_> = entity.into();
            insert.write(&value).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn delete_where(&self, condition: &str) -> Result<()> {
        let sql = format!("ALTER TABLE {} DELETE WHERE {}", self.table_name, condition);
        self.execute_raw(&sql).await
    }

    pub async fn update_where(&self, set_clause: &str, condition: &str) -> Result<()> {
        let sql =
            format!("ALTER TABLE {} UPDATE {} WHERE {}", self.table_name, set_clause, condition);
        self.execute_raw(&sql).await
    }
}
