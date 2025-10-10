#[derive(Debug, Clone)]
pub struct TableOptions {
    pub engine: Option<String>,
    pub order_by: Option<String>,
    pub partition_by: Option<String>,
    pub primary_key: Option<String>,
    pub sample_by: Option<String>,
    pub settings: Option<String>,
}
