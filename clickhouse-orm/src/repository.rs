use crate::query::{AggregateQuery, Query};
use crate::{error::Result, CHClient, ClickHouseTable};
use serde::{de::DeserializeOwned, Serialize};

pub struct Repository<T> {
    client: CHClient,
    table_name: &'static str,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Repository<T>
where
    T: Serialize + DeserializeOwned + clickhouse::Row + ClickHouseTable,
{
    pub fn new(client: CHClient, table_name: &'static str) -> Self {
        Self { client, table_name, _phantom: std::marker::PhantomData }
    }

    /// Создает таблицу если её нет (основной метод)
    pub async fn create_table(&self) -> Result<()> {
        let sql = T::create_table_sql();
        self.execute_raw(sql).await
    }

    /// Удаляет таблицу
    pub async fn drop_table(&self) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    /// Очищает таблицу
    pub async fn truncate_table(&self) -> Result<()> {
        let sql = format!("TRUNCATE TABLE IF EXISTS {}", self.table_name);
        self.execute_raw(&sql).await
    }

    pub fn query(&self) -> Query<T> {
        Query::new(self.client.client().clone(), self.table_name)
    }

    pub fn aggregate(&self) -> AggregateQuery {
        AggregateQuery::new(self.client.client().clone(), self.table_name)
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

    pub async fn insert_stream(&self) -> Result<clickhouse::insert::Insert<T>> {
        Ok(self.client.client().insert(self.table_name)?)
    }

    pub async fn execute_raw(&self, sql: &str) -> Result<()> {
        self.client.client().query(sql).execute().await?;
        Ok(())
    }
}

impl<T> Clone for Repository<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table_name: self.table_name,
            _phantom: std::marker::PhantomData,
        }
    }
}
