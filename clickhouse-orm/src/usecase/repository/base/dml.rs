use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize
        + DeserializeOwned
        + for<'a> clickhouse::Row<Value<'a> = T>
        + ClickHouseTable
        + 'static,
{
    pub async fn insert_one(&self, entity: &T) -> Result<()> {
        let mut insert = self.client.client().insert::<T>(self.table_name).await?;
        insert.write(entity).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.client().insert::<T>(self.table_name).await?;
        for entity in entities {
            insert.write(entity).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
