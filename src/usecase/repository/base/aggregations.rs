use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + clickhouse::RowOwned + ClickHouseTable,
{
    pub async fn sum<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT sum({}) FROM {}{}", column, self.table_name, final_clause);
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }

    pub async fn avg<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT avg({}) FROM {}{}", column, self.table_name, final_clause);
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }

    pub async fn min<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT min({}) FROM {}{}", column, self.table_name, final_clause);
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }

    pub async fn max<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT max({}) FROM {}{}", column, self.table_name, final_clause);
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }

    pub async fn sum_where<U>(&self, column: &str, condition: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!(
            "SELECT sum({}) FROM {}{} WHERE {}",
            column, self.table_name, final_clause, condition
        );
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }

    pub async fn avg_where<U>(&self, column: &str, condition: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!(
            "SELECT avg({}) FROM {}{} WHERE {}",
            column, self.table_name, final_clause, condition
        );
        let result = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(result)
    }
}
