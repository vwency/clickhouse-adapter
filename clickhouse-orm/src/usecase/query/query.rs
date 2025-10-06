use crate::domain::query::query::Query;
use crate::Engine;
use clickhouse::Client;
use std::marker::PhantomData;

impl<T> Query<T> {
    pub fn new(client: Client, table_name: &'static str, engine: Engine) -> Self {
        Self { client, table_name, engine, use_final: false, _phantom: PhantomData }
    }

    /// Использует FINAL (только для ReplacingMergeTree, CollapsingMergeTree)
    pub fn with_final(mut self) -> Self {
        if self.engine.supports_final() {
            self.use_final = true;
        }
        self
    }

    pub fn is_final_supported(&self) -> bool {
        self.engine.supports_final()
    }
}

pub struct AggregateQuery {
    client: Client,
    table_name: &'static str,
}

impl AggregateQuery {
    pub fn new(client: Client, table_name: &'static str) -> Self {
        Self { client, table_name }
    }
}
