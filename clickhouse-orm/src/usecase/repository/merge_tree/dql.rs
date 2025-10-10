use crate::domain::engine::MergeTreeFlag;
use crate::domain::engine::PartInfo;
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use clickhouse;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;

impl<T> Repository<T, MergeTreeFlag>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn get_parts_info(&self) -> impl Future<Output = Result<Vec<PartInfo>>> + Send {
        let table_name = self.table_name.to_string();
        let client = self.client.clone();

        async move {
            let query = format!(
                "SELECT partition, name, rows, bytes_on_disk, modification_time \
                 FROM system.parts \
                 WHERE table = '{}' AND active = 1",
                table_name
            );

            let parts = client
                .client()
                .query(&query)
                .fetch_all::<(String, String, u64, u64, u32)>()
                .await?;

            Ok(parts
                .into_iter()
                .map(|(partition, name, rows, bytes, _mod_time)| PartInfo {
                    partition,
                    name,
                    rows,
                    bytes,
                })
                .collect())
        }
    }
}
