use crate::domain::errors::default::Result;
use crate::Engine;
use crate::{CHClient, ClickHouseTable};
use clickhouse::insert::Insert;
use clickhouse::Row;
use serde::{de::DeserializeOwned, Serialize};

pub struct Repository<T, F> {
    pub client: CHClient,
    pub table_name: &'static str,
    pub engine: Engine,
    pub _phantom: std::marker::PhantomData<(T, F)>,
}

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + Row + ClickHouseTable + Clone,
    for<'a> <T as Row>::Value<'a>: Serialize,
{
    pub async fn insert_one(&self, entity: &T) -> Result<()> {
        let mut insert: Insert<T> = self.client.client().insert::<T>(self.table_name).await?;
        insert.write(entity.clone()).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }

        let mut insert: Insert<T> = self.client.client().insert::<T>(self.table_name).await?;
        for entity in entities {
            insert.write(entity.clone()).await?;
        }
        insert.end().await?;
        Ok(())
    }
}
