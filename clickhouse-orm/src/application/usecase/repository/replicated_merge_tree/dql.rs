use crate::domain::engine::{ReplicaStatus, ReplicatedMergeTreeFlag};
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

impl<T> Repository<T, ReplicatedMergeTreeFlag>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn check_replica_status(&self) -> impl Future<Output = Result<ReplicaStatus>> + Send {
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
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
}
