use clickhouse::Client;
use std::marker::PhantomData;

pub struct Query<T> {
    client: Client,
    table_name: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> Query<T> {
    pub fn new(client: Client, table_name: &'static str) -> Self {
        Self { client, table_name, _phantom: PhantomData }
    }

    // Добавьте здесь методы для построения и выполнения запросов
}

pub struct AggregateQuery {
    client: Client,
    table_name: &'static str,
}

impl AggregateQuery {
    pub fn new(client: Client, table_name: &'static str) -> Self {
        Self { client, table_name }
    }

    // Добавьте здесь методы для агрегаций
}
