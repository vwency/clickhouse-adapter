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
pub struct MergeTreeFlag;
pub struct ReplicatedMergeTreeFlag;
pub struct SummingMergeTreeFlag;
pub struct AggregatingMergeTreeFlag;
pub struct CollapsingMergeTreeFlag;
pub struct VersionedCollapsingMergeTreeFlag;
pub struct ReplacingMergeTreeFlag;
pub struct GraphiteMergeTreeFlag;
pub struct LogFlag;
pub struct TinyLogFlag;
pub struct MemoryFlag;
pub struct BufferFlag;
pub struct DistributedFlag;
