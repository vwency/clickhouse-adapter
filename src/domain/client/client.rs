use clickhouse::Client;
use std::sync::Arc;

#[derive(Clone)]
pub struct CHClient {
    pub inner: Arc<Client>,
}
