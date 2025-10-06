pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::repository::repository::Repository;

impl<T> Clone for Repository<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table_name: self.table_name,
            engine: self.engine.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
