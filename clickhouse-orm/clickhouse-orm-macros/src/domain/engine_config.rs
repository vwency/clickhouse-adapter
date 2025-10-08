#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub engine_type: String,
    pub zk_path: Option<String>,
    pub replica: Option<String>,
    pub sign_column: Option<String>,
    pub version_column: Option<String>,
    pub columns: Option<Vec<String>>,
}
