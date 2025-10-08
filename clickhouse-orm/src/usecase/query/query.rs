use crate::domain::errors::default::Result;
use crate::domain::query::query::Query;
use crate::Engine;
use clickhouse::Client;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

impl<T> Query<T>
where
    T: DeserializeOwned + clickhouse::Row,
{
    pub fn new(client: Client, table_name: &'static str, engine: Engine) -> Self {
        Self { client, table_name, engine, use_final: false, _phantom: PhantomData }
    }

    pub fn with_final(mut self) -> Self {
        if self.engine.supports_final() {
            self.use_final = true;
        }
        self
    }

    pub fn is_final_supported(&self) -> bool {
        self.engine.supports_final()
    }

    pub async fn select_column<U>(&self, column: &str) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if self.use_final { " FINAL" } else { "" };
        let sql = format!("SELECT {} FROM {}{}", column, self.table_name, final_clause);

        let rows = self.client.query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn select_columns(&self, columns: &[&str]) -> Result<Vec<T>> {
        let final_clause = if self.use_final { " FINAL" } else { "" };
        let columns_str = columns.join(", ");
        let sql = format!("SELECT {} FROM {}{}", columns_str, self.table_name, final_clause);

        let rows = self.client.query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_all(&self) -> Result<Vec<T>> {
        let final_clause = if self.use_final { " FINAL" } else { "" };
        let sql = format!("SELECT * FROM {}{}", self.table_name, final_clause);

        let rows = self.client.query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_one(&self) -> Result<Option<T>> {
        let final_clause = if self.use_final { " FINAL" } else { "" };
        let sql = format!("SELECT * FROM {}{} LIMIT 1", self.table_name, final_clause);

        let mut rows = self.client.query(&sql).fetch_all::<T>().await?;
        Ok(rows.pop())
    }

    pub async fn count(&self) -> Result<u64> {
        let final_clause = if self.use_final { " FINAL" } else { "" };
        let sql = format!("SELECT count() FROM {}{}", self.table_name, final_clause);

        let count: u64 = self.client.query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }

    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.client.query(sql).execute().await?;
        Ok(())
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

    pub async fn sum(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT sum({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn avg(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT avg({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn min(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT min({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn max(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT max({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn count(&self) -> Result<u64> {
        let sql = format!("SELECT count() FROM {}", self.table_name);
        let count: u64 = self.client.query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }
}
