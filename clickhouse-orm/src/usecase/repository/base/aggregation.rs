use crate::domain::errors::default::Result;
use crate::domain::repository::repository::Repository;
use crate::ClickHouseTable;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Trait для типов, которые могут быть результатом агрегации
pub trait AggregateResult: Sized + 'static {
    /// Wrapper тип для ClickHouse Row
    type Wrapper: for<'a> clickhouse::Row<Value<'a> = Self::Wrapper> + DeserializeOwned + 'static;

    /// Извлечь значение из wrapper
    fn from_wrapper(wrapper: Self::Wrapper) -> Self;
}

// Реализации для стандартных типов
impl AggregateResult for u64 {
    type Wrapper = ScalarU64;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

impl AggregateResult for i64 {
    type Wrapper = ScalarI64;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

impl AggregateResult for f64 {
    type Wrapper = ScalarF64;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

impl AggregateResult for u32 {
    type Wrapper = ScalarU32;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

impl AggregateResult for i32 {
    type Wrapper = ScalarI32;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

impl AggregateResult for f32 {
    type Wrapper = ScalarF32;
    fn from_wrapper(wrapper: Self::Wrapper) -> Self {
        wrapper.value
    }
}

// Wrapper структуры - публичные, так как они используются в публичном trait
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarU64 {
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarI64 {
    pub value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarF64 {
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarU32 {
    pub value: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarI32 {
    pub value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ScalarF32 {
    pub value: f32,
}

impl<T, F> Repository<T, F>
where
    T: Serialize
        + DeserializeOwned
        + for<'a> clickhouse::Row<Value<'a> = T>
        + ClickHouseTable
        + 'static,
{
    pub async fn aggregate_count(&self, use_final: bool) -> Result<u64> {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };

        let sql = format!("SELECT count() as value FROM {}{}", self.table_name, final_clause);

        let result = self.client.client().query(&sql).fetch_one::<ScalarU64>().await?;

        Ok(result.value)
    }

    /// Универсальный метод для агрегации с любым скалярным типом
    pub async fn aggregate_scalar<U>(
        &self,
        aggregate_fn: &str,
        column: &str,
        use_final: bool,
    ) -> Result<U>
    where
        U: AggregateResult,
    {
        let final_clause = if use_final && self.engine.supports_final() { " FINAL" } else { "" };

        let sql = format!(
            "SELECT {}({}) as value FROM {}{}",
            aggregate_fn, column, self.table_name, final_clause
        );

        let wrapper = self.client.client().query(&sql).fetch_one::<U::Wrapper>().await?;

        Ok(U::from_wrapper(wrapper))
    }

    /// Удобные методы на основе универсального
    pub async fn aggregate_sum<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: AggregateResult,
    {
        self.aggregate_scalar("sum", column, use_final).await
    }

    pub async fn aggregate_avg<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: AggregateResult,
    {
        self.aggregate_scalar("avg", column, use_final).await
    }

    pub async fn aggregate_min<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: AggregateResult,
    {
        self.aggregate_scalar("min", column, use_final).await
    }

    pub async fn aggregate_max<U>(&self, column: &str, use_final: bool) -> Result<U>
    where
        U: AggregateResult,
    {
        self.aggregate_scalar("max", column, use_final).await
    }

    pub async fn aggregate_group_by<U>(
        &self,
        group_columns: &[&str],
        aggregate_expr: &str,
        use_final: bool,
    ) -> Result<Vec<U>>
    where
        U: DeserializeOwned + for<'a> clickhouse::Row<Value<'a> = U> + 'static,
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

// Пример использования:
// let sum: u64 = repo.aggregate_sum("views", false).await?;
// let avg: f64 = repo.aggregate_avg("rating", false).await?;
// let max: i64 = repo.aggregate_max("score", true).await?;
//
// Для group by:
// #[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
// struct UserStats {
//     user_id: u64,
//     total_views: u64,
//     avg_duration: f64,
// }
// let stats: Vec<UserStats> = repo.aggregate_group_by(&["user_id"], "sum(views) as total_views, avg(duration) as avg_duration", false).await?;
