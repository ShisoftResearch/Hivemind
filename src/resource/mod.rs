pub mod block_storage;

use std::marker::PhantomData;
use resource::block_storage::BlockStorage;
use serde::de::DeserializeOwned;

pub enum Source<T> where T: DeserializeOwned {
    Local(Box<IntoIterator<Item = T, IntoIter = Iterator<Item = T>>>),
    Block(BlockStorage<T>)
}



pub struct DataSet<T> where T: DeserializeOwned {
    source: Source<T>,
}