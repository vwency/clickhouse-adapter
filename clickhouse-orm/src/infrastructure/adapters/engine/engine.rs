use crate::domain::engine::Engine;
use crate::domain::errors::default::CHError;

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
}

impl Engine {
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
}

impl Engine {
    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            Self::ReplacingMergeTree
                | Self::CollapsingMergeTree
                | Self::VersionedCollapsingMergeTree
        )
    }
}
