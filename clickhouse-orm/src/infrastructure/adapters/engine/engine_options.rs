pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::infrastructure::adapters::engine::engine::CHResult;
use std::future::Future;

pub trait MergeTreeOps {
    fn get_parts_info(&self) -> impl Future<Output = Result<Vec<PartInfo>>> + Send;
    fn optimize_table(&self) -> impl Future<Output = Result<()>> + Send;
}
pub trait ReplicatedMergeTreeOps {
    fn check_replica_status(&self) -> impl Future<Output = CHResult<ReplicaStatus>> + Send;
    fn sync_replica(&self) -> impl Future<Output = CHResult<()>> + Send;
}
