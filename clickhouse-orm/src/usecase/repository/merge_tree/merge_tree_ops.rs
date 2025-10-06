pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::infrastructure::adapters::engine::engine_options::MergeTreeOps;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

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
