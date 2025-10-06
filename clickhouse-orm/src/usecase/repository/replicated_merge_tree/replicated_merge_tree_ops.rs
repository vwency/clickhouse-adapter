use crate::adapters::engine::engine_options::ReplicatedMergeTreeOps;
pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

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
