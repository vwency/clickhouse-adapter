#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub is_leader: bool,
    pub absolute_delay: u64,
    pub queue_size: u64,
}
