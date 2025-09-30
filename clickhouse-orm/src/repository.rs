use crate::engine::{MergeTreeOps, PartInfo, ReplicaStatus, ReplicatedMergeTreeOps};
use crate::query::{AggregateQuery, Query};
use crate::{error::Result, CHClient, ClickHouseTable, Engine};
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

pub struct Repository<T> {
    client: CHClient,
    table_name: &'static str,
    engine: Engine,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Repository<T>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn new(client: CHClient, table_name: &'static str) -> Self {
        let engine = T::engine();
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

    pub fn query(&self) -> Query<T> {
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

// MergeTree специфичные операции
impl<T> MergeTreeOps for Repository<T>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    fn optimize(&self) -> impl Future<Output = Result<()>> + Send {
        let engine = self.engine.clone();
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            if !engine.supports_optimize() {
                return Err(crate::CHError::UnsupportedOperation(format!(
                    "OPTIMIZE not supported for {:?}",
                    engine
                )));
            }

            let sql = format!("OPTIMIZE TABLE {}", table_name);
            client.client().query(&sql).execute().await?;
            Ok(())
        }
    }

    fn optimize_final(&self) -> impl Future<Output = Result<()>> + Send {
        let engine = self.engine.clone();
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            if !engine.supports_optimize() {
                return Err(crate::CHError::UnsupportedOperation(format!(
                    "OPTIMIZE FINAL not supported for {:?}",
                    engine
                )));
            }

            let sql = format!("OPTIMIZE TABLE {} FINAL", table_name);
            client.client().query(&sql).execute().await?;
            Ok(())
        }
    }

    fn get_parts_info(&self) -> impl Future<Output = Result<Vec<PartInfo>>> + Send {
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            let query = format!(
                "SELECT partition, name, rows, bytes_on_disk as bytes \
                 FROM system.parts \
                 WHERE table = '{}' AND active = 1",
                table_name
            );

            let parts =
                client.client().query(&query).fetch_all::<(String, String, u64, u64)>().await?;

            Ok(parts
                .into_iter()
                .map(|(partition, name, rows, bytes)| PartInfo { partition, name, rows, bytes })
                .collect())
        }
    }
}

// Replicated специфичные операции
impl<T> ReplicatedMergeTreeOps for Repository<T>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    fn check_replica_status(&self) -> impl Future<Output = Result<ReplicaStatus>> + Send {
        let engine = self.engine.clone();
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            if !matches!(engine, Engine::ReplicatedMergeTree { .. }) {
                return Err(crate::CHError::UnsupportedOperation(
                    "Replica operations only for ReplicatedMergeTree".to_string(),
                ));
            }

            let query = format!(
                "SELECT is_leader, absolute_delay, queue_size \
                 FROM system.replicas \
                 WHERE table = '{}'",
                table_name
            );

            let result = client.client().query(&query).fetch_one::<(u8, u64, u64)>().await?;

            Ok(ReplicaStatus {
                is_leader: result.0 == 1,
                absolute_delay: result.1,
                queue_size: result.2,
            })
        }
    }

    fn sync_replica(&self) -> impl Future<Output = Result<()>> + Send {
        let engine = self.engine.clone();
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            if !matches!(engine, Engine::ReplicatedMergeTree { .. }) {
                return Err(crate::CHError::UnsupportedOperation(
                    "SYNC REPLICA only for ReplicatedMergeTree".to_string(),
                ));
            }

            let sql = format!("SYSTEM SYNC REPLICA {}", table_name);
            client.client().query(&sql).execute().await?;
            Ok(())
        }
    }
}

impl<T> Clone for Repository<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table_name: self.table_name,
            engine: self.engine.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
