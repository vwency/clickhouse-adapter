#[derive(Debug, Clone, PartialEq)]
pub struct PartInfo {
    pub partition: String,
    pub name: String,
    pub rows: u64,
    pub bytes: u64,
}
