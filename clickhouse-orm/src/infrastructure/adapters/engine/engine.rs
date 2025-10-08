use crate::domain::engine::Engine;
use crate::domain::errors::default::CHError;

pub type CHResult<T> = Result<T, CHError>;

impl Engine {
    pub fn from_sql(sql: &str) -> Self {
        if sql.contains("ReplicatedMergeTree") {
            Self::ReplicatedMergeTree { zk_path: String::new(), replica: String::new() }
        } else if sql.contains("SummingMergeTree") {
            Self::SummingMergeTree { columns: vec![] }
        } else if sql.contains("AggregatingMergeTree") {
            Self::AggregatingMergeTree
        } else if sql.contains("CollapsingMergeTree") {
            Self::CollapsingMergeTree { sign_column: String::new() }
        } else if sql.contains("VersionedCollapsingMergeTree") {
            Self::VersionedCollapsingMergeTree {
                sign_column: String::new(),
                version_column: String::new(),
            }
        } else if sql.contains("ReplacingMergeTree") {
            Self::ReplacingMergeTree
        } else if sql.contains("GraphiteMergeTree") {
            Self::GraphiteMergeTree
        } else if sql.contains("Log") {
            Self::Log
        } else if sql.contains("TinyLog") {
            Self::TinyLog
        } else if sql.contains("Memory") {
            Self::Memory
        } else if sql.contains("Buffer") {
            Self::Buffer
        } else if sql.contains("Distributed") {
            Self::Distributed
        } else {
            Self::MergeTree
        }
    }

    pub fn supports_optimize(&self) -> bool {
        matches!(
            self,
            Self::MergeTree
                | Self::ReplicatedMergeTree { .. }
                | Self::SummingMergeTree { .. }
                | Self::AggregatingMergeTree
                | Self::CollapsingMergeTree { .. }
                | Self::ReplacingMergeTree
        )
    }

    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            Self::ReplacingMergeTree
                | Self::CollapsingMergeTree { .. }
                | Self::VersionedCollapsingMergeTree { .. }
        )
    }
}
