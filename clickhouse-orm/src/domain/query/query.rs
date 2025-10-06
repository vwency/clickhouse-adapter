use crate::Engine;
use clickhouse::Client;
use std::marker::PhantomData;

pub struct Query<T> {
    pub client: Client,
    pub table_name: &'static str,
    pub engine: Engine,
    pub use_final: bool,
    pub _phantom: PhantomData<T>,
}
