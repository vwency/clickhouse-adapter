use crate::{CHClient, Engine};

pub struct Repository<T, F> {
    pub client: CHClient,
    pub table_name: &'static str,
    pub engine: Engine,
    pub _phantom: std::marker::PhantomData<(T, F)>,
}
