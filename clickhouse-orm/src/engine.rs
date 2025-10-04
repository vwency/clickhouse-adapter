pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::CHError;
use std::future::Future;

pub type CHResult<T> = Result<T, CHError>;

impl Engine {
    pub fn from_sql(sql: &str) -> Self {
        if sql.contains("ReplicatedMergeTree") {
            Self::ReplicatedMergeTree { zk_path: String::new(), replica: String::new() }
        } else if sql.contains("SummingMergeTree") {
            Self::SummingMergeTree
        } else if sql.contains("AggregatingMergeTree") {
            Self::AggregatingMergeTree
        } else if sql.contains("CollapsingMergeTree") {
            Self::CollapsingMergeTree
        } else if sql.contains("ReplacingMergeTree") {
            Self::ReplacingMergeTree
        } else {
            Self::MergeTree
        }
    }

    pub fn supports_optimize(&self) -> bool {
        matches!(
            self,
            Self::MergeTree
                | Self::ReplicatedMergeTree { .. }
                | Self::SummingMergeTree
                | Self::AggregatingMergeTree
                | Self::CollapsingMergeTree
                | Self::ReplacingMergeTree
        )
    }

    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            Self::ReplacingMergeTree
                | Self::CollapsingMergeTree
                | Self::VersionedCollapsingMergeTree
        )
    }
}

// MergeTree специфичные операции
pub trait MergeTreeOps {
    fn optimize(&self) -> impl Future<Output = CHResult<()>> + Send;
    fn optimize_final(&self) -> impl Future<Output = CHResult<()>> + Send;
    fn get_parts_info(&self) -> impl Future<Output = CHResult<Vec<PartInfo>>> + Send;
}

// Replicated операции
pub trait ReplicatedMergeTreeOps {
    fn check_replica_status(&self) -> impl Future<Output = CHResult<ReplicaStatus>> + Send;
    fn sync_replica(&self) -> impl Future<Output = CHResult<()>> + Send;
}
