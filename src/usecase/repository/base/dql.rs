use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + clickhouse::RowOwned + ClickHouseTable,
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

    pub async fn fetch_where(&self, condition: &str, use_final: bool) -> Result<Vec<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT * FROM {}{} WHERE {}", self.table_name, final_clause, condition);
        let rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_where_limit(
        &self,
        condition: &str,
        limit: u64,
        use_final: bool,
    ) -> Result<Vec<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!(
            "SELECT * FROM {}{} WHERE {} LIMIT {}",
            self.table_name, final_clause, condition, limit
        );
        let rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_order_by(&self, order_by: &str, use_final: bool) -> Result<Vec<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql =
            format!("SELECT * FROM {}{} ORDER BY {}", self.table_name, final_clause, order_by);
        let rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn fetch_with_limit(
        &self,
        limit: u64,
        offset: u64,
        use_final: bool,
    ) -> Result<Vec<T>> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!(
            "SELECT * FROM {}{} LIMIT {} OFFSET {}",
            self.table_name, final_clause, limit, offset
        );
        let rows = self.client.client().query(&sql).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn select_columns<U>(&self, columns: &[&str], use_final: bool) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let columns_str = columns.join(", ");
        let sql = format!("SELECT {} FROM {}{}", columns_str, self.table_name, final_clause);
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn select_columns_where<U>(
        &self,
        columns: &[&str],
        condition: &str,
        use_final: bool,
    ) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let columns_str = columns.join(", ");
        let sql = format!(
            "SELECT {} FROM {}{} WHERE {}",
            columns_str, self.table_name, final_clause, condition
        );
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn count(&self, use_final: bool) -> Result<u64> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT count() FROM {}{}", self.table_name, final_clause);
        let count: u64 = self.client.client().query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }

    pub async fn count_where(&self, condition: &str, use_final: bool) -> Result<u64> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql =
            format!("SELECT count() FROM {}{} WHERE {}", self.table_name, final_clause, condition);
        let count: u64 = self.client.client().query(&sql).fetch_one::<u64>().await?;
        Ok(count)
    }

    pub async fn exists_where(&self, condition: &str, use_final: bool) -> Result<bool> {
        let count = self.count_where(condition, use_final).await?;
        Ok(count > 0)
    }

    pub async fn distinct_column<U>(&self, column: &str, use_final: bool) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!("SELECT DISTINCT {} FROM {}{}", column, self.table_name, final_clause);
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn group_by<U>(
        &self,
        select_clause: &str,
        group_by: &str,
        use_final: bool,
    ) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };
        let sql = format!(
            "SELECT {} FROM {}{} GROUP BY {}",
            select_clause, self.table_name, final_clause, group_by
        );
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }
}
