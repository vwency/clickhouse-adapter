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
