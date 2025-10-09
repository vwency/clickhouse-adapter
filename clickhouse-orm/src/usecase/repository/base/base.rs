pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::{CHClient, ClickHouseTable};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn new(client: CHClient, table_name: &'static str, engine: Engine) -> Self {
        Self { client, table_name, engine, _phantom: PhantomData }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub async fn create_table(&self) -> Result<()> {
        let sql = T::create_table_sql();
        self.execute_raw(&sql).await
    }

    pub async fn drop_table(&self) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub async fn truncate_table(&self) -> Result<()> {
        let sql = format!("TRUNCATE TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub async fn insert_one(&self, entity: &T) -> Result<()> {
        let mut insert = self.client.client().insert(self.table_name)?;
        insert.write(entity).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_many(&self, entities: &[T]) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.client().insert(self.table_name)?;
        for entity in entities {
            insert.write(entity).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn fetch_all(&self, use_final: bool) -> Result<Vec<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT * FROM {}{}", self.table_name, final_clause);
        let rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_one(&self, use_final: bool) -> Result<Option<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT * FROM {}{} LIMIT 1", self.table_name, final_clause);
        let mut rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows.pop())
    }

    pub async fn select_columns<U>(&self, columns: &[&str], use_final: bool) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let columns_str = columns.join(", ");
        let sql = format!("SELECT {} FROM {}{}", columns_str, self.table_name, final_clause);
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn count(&self, use_final: bool) -> Result<u64> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT count() FROM {}{}", self.table_name, final_clause);
        let count: u64 = self.client.client().query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }

    pub async fn sum(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT sum({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.client().query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn avg(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT avg({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.client().query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn min(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT min({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.client().query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn max(&self, column: &str) -> Result<f64> {
        let sql = format!("SELECT max({}) FROM {}", column, self.table_name);
        let result: f64 = self.client.client().query(&sql).fetch_one::<f64>().await?;
        Ok(result)
    }

    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.client.client().query(sql).execute().await?;
        Ok(())
    }
}
