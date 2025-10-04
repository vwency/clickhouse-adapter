pub mod engine;
pub mod part_info;
pub mod replica_status;

// ✅ Реэкспорт, чтобы всё было доступно через `crate::domain::engine::*`
pub use engine::Engine;
pub use part_info::PartInfo;
pub use replica_status::ReplicaStatus;
