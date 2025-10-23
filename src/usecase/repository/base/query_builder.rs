use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Serialize};

pub struct QueryBuilder {
    select: Option<String>,
    from: String,
    where_clause: Vec<String>,
    order_by: Option<String>,
    group_by: Option<String>,
    having: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    use_final: bool,
}

impl QueryBuilder {
    pub fn new(table_name: &str) -> Self {
        Self {
            select: None,
            from: table_name.to_string(),
            where_clause: Vec::new(),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            offset: None,
            use_final: false,
        }
    }

    pub fn select(mut self, columns: &str) -> Self {
        self.select = Some(columns.to_string());
        self
    }

    pub fn where_clause(mut self, condition: &str) -> Self {
        self.where_clause.push(condition.to_string());
        self
    }

    pub fn and_where(mut self, condition: &str) -> Self {
        self.where_clause.push(condition.to_string());
        self
    }

    pub fn order_by(mut self, order: &str) -> Self {
        self.order_by = Some(order.to_string());
        self
    }

    pub fn group_by(mut self, group: &str) -> Self {
        self.group_by = Some(group.to_string());
        self
    }

    pub fn having(mut self, condition: &str) -> Self {
        self.having = Some(condition.to_string());
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn use_final(mut self, use_final: bool) -> Self {
        self.use_final = use_final;
        self
    }

    pub fn build(&self, supports_final: bool) -> String {
        let mut sql = String::new();

        let select = self.select.as_deref().unwrap_or("*");
        sql.push_str(&format!("SELECT {} FROM {}", select, self.from));

        if self.use_final && supports_final {
            sql.push_str(" FINAL");
        }

        if !self.where_clause.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clause.join(" AND "));
        }

        if let Some(ref group) = self.group_by {
            sql.push_str(&format!(" GROUP BY {}", group));
        }

        if let Some(ref having) = self.having {
            sql.push_str(&format!(" HAVING {}", having));
        }

        if let Some(ref order) = self.order_by {
            sql.push_str(&format!(" ORDER BY {}", order));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }
}

impl<T, F> Repository<T, F>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + clickhouse::RowOwned + ClickHouseTable,
{
    pub fn query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self.table_name)
    }

    pub async fn execute_query<U>(&self, builder: &QueryBuilder) -> Result<Vec<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let sql = builder.build(self.engine.supports_final());
        let rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows)
    }

    pub async fn execute_query_one<U>(&self, builder: &QueryBuilder) -> Result<Option<U>>
    where
        U: DeserializeOwned + clickhouse::Row + clickhouse::RowOwned,
    {
        let sql = builder.build(self.engine.supports_final());
        let mut rows = self.client.client().query(&sql).fetch_all::<U>().await?;
        Ok(rows.pop())
    }
}
