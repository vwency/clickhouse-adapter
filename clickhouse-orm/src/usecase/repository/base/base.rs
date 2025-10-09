pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::domain::query::query::Query;
use crate::domain::repository::repository::Repository;
use crate::usecase::query::query::AggregateQuery;
use crate::{CHClient, ClickHouseTable};
use serde::{de::DeserializeOwned, Serialize};
impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn new(client: CHClient, table_name: &'static str, engine: Engine) -> Self {
        Self { client, table_name, engine, _phantom: std::marker::PhantomData }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub async fn create_table(&self) -> Result<()> {
        let sql = T::create_table_sql();
        self.execute_raw(sql).await
    }

    pub async fn drop_table(&self) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub async fn truncate_table(&self) -> Result<()> {
        let sql = format!("TRUNCATE TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub fn query(&self) -> Query<T, F> {
        Query::new(self.client.client().clone(), self.table_name, self.engine.clone())
    }

    pub fn aggregate(&self) -> AggregateQuery {
        AggregateQuery::new(self.client.client().clone(), self.table_name)
    }

    pub async fn insert_one(&self, entity: &T) -> Result<()> {
        let mut insert = self.client.client().insert(self.table_name)?;
        insert.write(entity).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }

        let mut insert = self.client.client().insert(self.table_name)?;
        for entity in entities {
            insert.write(entity).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.client.client().query(sql).execute().await?;
        Ok(())
    }
}
