use crate::domain::engine::Engine;
use crate::domain::errors::default::CHError;

pub type CHResult<T> = Result<T, CHError>;

impl Engine {
    pub fn from_sql(sql: &str) -> Self {
        match sql {
            s if s.contains("ReplicatedMergeTree") => {
                Self::ReplicatedMergeTree { zk_path: String::new(), replica: String::new() }
            }
            s if s.contains("VersionedCollapsingMergeTree") => Self::VersionedCollapsingMergeTree {
                sign_column: String::new(),
                version_column: String::new(),
            },
            s if s.contains("CollapsingMergeTree") => {
                Self::CollapsingMergeTree { sign_column: String::new() }
            }
            s if s.contains("SummingMergeTree") => Self::SummingMergeTree { columns: vec![] },
            s if s.contains("AggregatingMergeTree") => Self::AggregatingMergeTree,
            s if s.contains("ReplacingMergeTree") => Self::ReplacingMergeTree,
            s if s.contains("GraphiteMergeTree") => Self::GraphiteMergeTree,
            s if s.contains("TinyLog") => Self::TinyLog,
            s if s.contains("Log") => Self::Log,
            s if s.contains("Memory") => Self::Memory,
            s if s.contains("Buffer") => Self::Buffer,
            s if s.contains("Distributed") => Self::Distributed,
            _ => Self::MergeTree,
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
