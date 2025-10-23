pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::{CHClient, ClickHouseTable};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + ClickHouseTable,
{
    pub fn new(client: CHClient, table_name: &'static str, engine: Engine) -> Self {
        Self { client, table_name, engine, _phantom: PhantomData }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.client.client().query(sql).execute().await?;
        Ok(())
    }
}
