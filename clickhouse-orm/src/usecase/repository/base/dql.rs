use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
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
}
