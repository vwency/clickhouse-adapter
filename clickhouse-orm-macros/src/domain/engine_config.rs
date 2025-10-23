use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub enum EngineType {
    MergeTree,
    ReplicatedMergeTree,
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
    Other(String),
}

impl fmt::Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use EngineType::*;
        match self {
            MergeTree => write!(f, "MergeTree"),
            ReplicatedMergeTree => write!(f, "ReplicatedMergeTree"),
            SummingMergeTree => write!(f, "SummingMergeTree"),
            AggregatingMergeTree => write!(f, "AggregatingMergeTree"),
            CollapsingMergeTree => write!(f, "CollapsingMergeTree"),
            VersionedCollapsingMergeTree => write!(f, "VersionedCollapsingMergeTree"),
            ReplacingMergeTree => write!(f, "ReplacingMergeTree"),
            GraphiteMergeTree => write!(f, "GraphiteMergeTree"),
            Log => write!(f, "Log"),
            TinyLog => write!(f, "TinyLog"),
            Memory => write!(f, "Memory"),
            Buffer => write!(f, "Buffer"),
            Distributed => write!(f, "Distributed"),
            Other(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub engine_type: EngineType,
    pub zk_path: Option<String>,
    pub replica: Option<String>,
    pub sign_column: Option<String>,
    pub version_column: Option<String>,
    pub columns: Option<Vec<String>>,
}
