use crate::domain::engine::ReplicatedMergeTreeFlag;
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

impl<T> Repository<T, ReplicatedMergeTreeFlag>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn sync_replica(&self) -> impl Future<Output = Result<()>> + Send {
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            let sql = format!("SYSTEM SYNC REPLICA {}", table_name);
            client.client().query(&sql).execute().await?;
            Ok(())
        }
    }
}
