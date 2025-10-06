pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::infrastructure::adapters::engine::engine::CHResult;
use std::future::Future;

pub trait MergeTreeOps {
    fn optimize(&self) -> impl Future<Output = CHResult<()>> + Send;
    fn optimize_final(&self) -> impl Future<Output = CHResult<()>> + Send;
    fn get_parts_info(&self) -> impl Future<Output = CHResult<Vec<PartInfo>>> + Send;
}

pub trait ReplicatedMergeTreeOps {
    fn check_replica_status(&self) -> impl Future<Output = CHResult<ReplicaStatus>> + Send;
    fn sync_replica(&self) -> impl Future<Output = CHResult<()>> + Send;
}
