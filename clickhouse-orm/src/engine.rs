use std::future::Future;

#[derive(Debug, Clone, PartialEq)]
pub enum Engine {
    MergeTree,
    ReplicatedMergeTree { zk_path: String, replica: String },
    SummingMergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    VersionedCollapsingMergeTree,
    ReplacingMergeTree,
    GraphiteMergeTree,
    Log,
    TinyLog,
    Memory,
    Buffer,
    Distributed,
}

impl Engine {
    pub fn from_sql(sql: &str) -> Self {
        if sql.contains("ReplicatedMergeTree") {
            Engine::ReplicatedMergeTree { zk_path: String::new(), replica: String::new() }
        } else if sql.contains("SummingMergeTree") {
            Engine::SummingMergeTree
        } else if sql.contains("AggregatingMergeTree") {
            Engine::AggregatingMergeTree
        } else if sql.contains("CollapsingMergeTree") {
            Engine::CollapsingMergeTree
        } else if sql.contains("ReplacingMergeTree") {
            Engine::ReplacingMergeTree
        } else if sql.contains("MergeTree") {
            Engine::MergeTree
        } else {
            Engine::MergeTree
        }
    }

    pub fn supports_optimize(&self) -> bool {
        matches!(
            self,
            Engine::MergeTree
                | Engine::ReplicatedMergeTree { .. }
                | Engine::SummingMergeTree
                | Engine::AggregatingMergeTree
                | Engine::CollapsingMergeTree
                | Engine::ReplacingMergeTree
        )
    }

    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            Engine::ReplacingMergeTree
                | Engine::CollapsingMergeTree
                | Engine::VersionedCollapsingMergeTree
        )
    }
}

// Trait для MergeTree специфичных операций
pub trait MergeTreeOps {
    fn optimize(&self) -> impl Future<Output = crate::error::Result<()>> + Send;
    fn optimize_final(&self) -> impl Future<Output = crate::error::Result<()>> + Send;
    fn get_parts_info(&self) -> impl Future<Output = crate::error::Result<Vec<PartInfo>>> + Send;
}

// Trait для Replicated операций
pub trait ReplicatedMergeTreeOps {
    fn check_replica_status(
        &self,
    ) -> impl Future<Output = crate::error::Result<ReplicaStatus>> + Send;
    fn sync_replica(&self) -> impl Future<Output = crate::error::Result<()>> + Send;
}

#[derive(Debug, Clone)]
pub struct PartInfo {
    pub partition: String,
    pub name: String,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub is_leader: bool,
    pub absolute_delay: u64,
    pub queue_size: u64,
}
