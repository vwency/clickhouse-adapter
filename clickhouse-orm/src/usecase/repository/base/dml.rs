use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use clickhouse::Row;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + Row + ClickHouseTable,
    for<'a> <T as Row>::Value<'a>: Serialize + From<&'a T>,
{
    pub async fn insert_one(&self, entity: &T) -> Result<()> {
        let mut insert = self.client.client().insert::<T>(self.table_name).await?;
        let value: <T as Row>::Value<'_> = entity.into();
        insert.write(&value).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()> {
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
}
