#[derive(Debug, Clone)]
pub enum Engine {
    MergeTree,
    ReplicatedMergeTree { zk_path: String, replica: String },
    SummingMergeTree { columns: Vec<String> },
    AggregatingMergeTree,
    CollapsingMergeTree { sign_column: String },
    VersionedCollapsingMergeTree { sign_column: String, version_column: String },
    ReplacingMergeTree,
    GraphiteMergeTree,
    Log,
    TinyLog,
    Memory,
    Buffer,
    Distributed,
}
