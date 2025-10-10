use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub async fn aggregate_count(&self, use_final: bool) -> Result<u64> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT count() FROM {}{}", self.table_name, final_clause);
        let count: u64 = self.client.client().query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }

    pub async fn aggregate_sum<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT sum({}) FROM {}{}", column, self.table_name, final_clause);
        let sum: U = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(sum)
    }

    pub async fn aggregate_avg<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT avg({}) FROM {}{}", column, self.table_name, final_clause);
        let avg: U = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(avg)
    }

    pub async fn aggregate_min<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT min({}) FROM {}{}", column, self.table_name, final_clause);
        let min: U = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(min)
    }

    pub async fn aggregate_max<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT max({}) FROM {}{}", column, self.table_name, final_clause);
        let max: U = self.client.client().query(&sql).fetch_one::<U>().await?;
        Ok(max)
    }

    pub async fn aggregate_group_by<U>(
        &self,
        group_columns: &[&str],
        aggregate_expr: &str,
        use_final: bool,
    ) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let group_cols_str = group_columns.join(", ");
        let sql = format!(
            "SELECT {}, {} FROM {}{} GROUP BY {}",
            group_cols_str, aggregate_expr, self.table_name, final_clause, group_cols_str
        );
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }
}
