pub mod block_storage;

use std::marker::PhantomData;
use std::sync::Arc;
use resource::block_storage::BlockStorage;
use storage::block::BlockManager;
use serde::de::DeserializeOwned;
use futures::prelude::*;
use futures::stream;
use utils::uuid::UUID;

pub struct DataSet<T> where T: DeserializeOwned {
    source: Box<Stream<Item = T, Error = String>>,
}

impl <T> DataSet<T> where T: DeserializeOwned {
    pub fn new(source: Box<Stream<Item = T, Error = String>>) -> DataSet<T> {
        DataSet { source }
    }
}

impl <II, T, I> From<II> for DataSet<T>
    where T: DeserializeOwned,
          I: Iterator<Item = T> + 'static,
          II: IntoIterator<Item = T, IntoIter = I>
{
    fn from(source: II) -> Self {
        let iter = source.into_iter();
        return DataSet::new(box stream::iter_ok(iter))
    }
}

impl <T> DataSet<T> where T: DeserializeOwned + 'static {
    pub fn from_block_storage(manager: &Arc<BlockManager>, server_id: u64, id: UUID, buff_size: u64) -> Self {
        DataSet::new(box BlockStorage::new(manager, server_id, id, buff_size))
    }
}

