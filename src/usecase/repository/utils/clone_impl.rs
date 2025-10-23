pub use crate::domain::engine::{Engine, PartInfo, ReplicaStatus};
use crate::domain::repository::repository::Repository;

impl<T, F> Clone for Repository<T, F> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table_name: self.table_name,
            engine: self.engine.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
