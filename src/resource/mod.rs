pub mod block_storage;
pub mod global_storage;

use std::marker::PhantomData;
use std::sync::Arc;
use std::cell::RefCell;use std::borrow::Borrow;
use resource::block_storage::{BlockStorage, BlockStorageProperty};
use storage::block::BlockManager;
use serde::de::{DeserializeOwned};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use bifrost::utils::bincode;
use futures::prelude::*;
use futures::stream;
use utils::uuid::UUID;
use server::HMServer;
use actors::funcs::LocationTraced;

pub const STORAGE_BUFFER: u64 = 10;

pub enum DataSetSourceType {
    Runtime,
    BlockStorage(BlockStorageProperty)
}

#[derive(Serialize, Deserialize)]
pub enum SerdeDataSet {
    Runtime(Vec<Vec<u8>>),
    BlockStorage(BlockStorageProperty)
}

pub struct DataSet<T> where T: Serialize + DeserializeOwned {
    source: RefCell<Box<Stream<Item = T, Error = String>>>,
    pub source_type: DataSetSourceType
}

impl <T> DataSet<T> where T: Serialize + DeserializeOwned {
    fn new(source: Box<Stream<Item = T, Error = String>>, source_type: DataSetSourceType) -> DataSet<T> {
        DataSet { source: RefCell::new(source), source_type }
    }
}

impl <II, T, I> From<II> for DataSet<T>
    where T: Serialize + DeserializeOwned,
          I: Iterator<Item = T> + 'static,
          II: IntoIterator<Item = T, IntoIter = I>
{
    fn from(source: II) -> Self {
        let iter = source.into_iter();
        return DataSet::new(
            box stream::iter_ok(iter),
            DataSetSourceType::Runtime)
    }
}

impl <T> DataSet<T> where T: Serialize + DeserializeOwned + 'static {
    pub fn from_block_storage(
        manager: &Arc<BlockManager>,
        server_id: u64,
        id: UUID,
        members: Vec<u64>,
        buff_size: u64
    ) -> Self {
        DataSet::new(
            box BlockStorage::new(manager, server_id, id, buff_size),
            DataSetSourceType::BlockStorage(BlockStorageProperty {
                id, members, server_id
            }))
    }
    fn ser_data(&self) -> SerdeDataSet {
        match self.source_type {
            DataSetSourceType::Runtime => {
                let mut vec: Vec<Vec<u8>> = Vec::new();
                let mut source = self.source.borrow_mut();
                while let Ok(Async::Ready(item)) = source.poll() {
                    let data = bincode::serialize(&item);
                    vec.push(data)
                }
                SerdeDataSet::Runtime(vec)
            },
            DataSetSourceType::BlockStorage(ref prop) => SerdeDataSet::BlockStorage(prop.clone())
        }
    }
    fn from_de_data(data: SerdeDataSet) -> DataSet<T> {
        match data {
            SerdeDataSet::Runtime(ref data) => {
                Self::from(data
                    .iter()
                    .map(|item| bincode::deserialize(item))
                    .collect::<Vec<T>>())
            },
            SerdeDataSet::BlockStorage(prop) => {
                Self::from_block_storage(
                    &BlockManager::default(),
                    prop.server_id, prop.id, prop.members, STORAGE_BUFFER)
            }
        }
    }
}

impl <T> Stream for DataSet<T> where T: Serialize + DeserializeOwned + 'static {
    type Item = T;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut source = self.source.get_mut();
        source.poll()
    }
}

impl <T> Serialize for DataSet<T> where T: Serialize + DeserializeOwned + 'static {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer
    {
        let ser_struct = self.ser_data();
        SerdeDataSet::serialize(&ser_struct, serializer)
    }
}

impl <'de, T> Deserialize<'de> for DataSet<T>
    where T: Serialize + DeserializeOwned + 'static
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let de_data = SerdeDataSet::deserialize(deserializer)?;
        Ok(Self::from_de_data(de_data))

    }
}

impl <T> LocationTraced for DataSet<T>
    where T: Serialize + DeserializeOwned + 'static
{
    fn get_affinity(&self) -> Vec<u64> {
        match self.source_type {
            DataSetSourceType::BlockStorage(ref prop) => prop.members.clone(),
            _ => vec![]
        }
    }
}

pub enum DataSourceType {

}

pub struct Data<T> where T: Serialize + DeserializeOwned {
    source: Box<Future<Item = T, Error = String>>,
    pub source_type: DataSourceType

}