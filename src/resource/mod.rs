#[macro_use]
pub mod streamable_storage;
pub mod block_storage;
pub mod global_storage;
pub mod immutable_storage;

use std::marker::PhantomData;
use std::sync::Arc;
use std::cell::RefCell;
use std::borrow::Borrow;
use resource::block_storage::{BlockStorage, BlockStorageProperty};
use resource::global_storage::{GlobalManager, GlobalStorageError};
use resource::immutable_storage::{ImmutableStorage, ImmutableStorageProperty};
use storage::block::BlockManager;
use storage::immutable::ImmutableManager;
use serde::de::{DeserializeOwned};
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use bifrost::utils::bincode;
use futures::prelude::*;
use futures::{stream, future};
use utils::uuid::UUID;
use server::HMServer;
use actors::funcs::LocationTraced;

pub const STORAGE_BUFFER: u64 = 10;

pub enum DataSetSourceType {
    Runtime,
    BlockStorage(BlockStorageProperty),
    ImmutableStorage(ImmutableStorageProperty)
}

#[derive(Serialize, Deserialize)]
pub enum SerdeDataSet {
    Runtime(Vec<Vec<u8>>),
    BlockStorage(BlockStorageProperty),
    ImmutableStorage(ImmutableStorageProperty)
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
        task: UUID,
        id: UUID,
        members: Vec<u64>,
        buff_size: u64
    ) -> Self {
        DataSet::new(
            box BlockStorage::new(manager, server_id, task, id, buff_size),
            DataSetSourceType::BlockStorage(BlockStorageProperty {
                id, members, server_id, task
            }))
    }

    pub fn from_immutable_storage(
        manager: &Arc<ImmutableManager>,
        task_id: UUID,
        id: UUID,
        buff_size: u64
    )
        -> Self
    {
        DataSet::new(
            box ImmutableStorage::new(manager, task_id, id, buff_size),
            DataSetSourceType::ImmutableStorage(ImmutableStorageProperty {
                task_id, id
            })
        )
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
            DataSetSourceType::BlockStorage(ref prop) => SerdeDataSet::BlockStorage(prop.clone()),
            DataSetSourceType::ImmutableStorage(ref prop) => SerdeDataSet::ImmutableStorage(prop.clone())
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
                    prop.server_id, prop.task, prop.id, prop.members, STORAGE_BUFFER)
            },
            SerdeDataSet::ImmutableStorage(prop) => {
                Self::from_immutable_storage(
                    &ImmutableManager::default(),
                    prop.task_id, prop.id, STORAGE_BUFFER
                )
            }
        }
    }
}

impl <T> Stream for DataSet<T> where T: Serialize + DeserializeOwned + 'static {
    type Item = T;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.source.get_mut().poll()
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

////////////////////////////////////////
// Non-iterable data starts from here //
////////////////////////////////////////

pub enum DataSourceType {
    Runtime,
    GlobalStorage(UUID, Vec<u8>),
    ImmutableStorage(UUID, UUID)
}

pub struct Data<T> where T: Serialize + DeserializeOwned {
    source: RefCell<Box<Future<Item = T, Error = String>>>,
    pub source_type: DataSourceType
}

#[derive(Serialize, Deserialize)]
pub enum SerdeData {
    Runtime(Vec<u8>),
    GlobalStorage(UUID, Vec<u8>),
    ImmutableStorage(UUID, UUID)
}

impl <T> Data <T> where T: Serialize + DeserializeOwned + 'static {
    pub fn from_fut<TI, E>(f: TI) -> Self
        where T: Serialize + DeserializeOwned,
              TI: IntoFuture<Item = T, Error = E> + 'static,
              E: ToString
    {
        Data {
            source: RefCell::new(box f.into_future().map_err(|e| e.to_string())),
            source_type: DataSourceType::Runtime
        }
    }
    pub fn from_global_storage_future(
        source_fut: Box<Future<Item = Option<Vec<u8>>, Error = GlobalStorageError>>,
        id: UUID, key: Vec<u8>
    )
        -> Data<Option<T>>
    {
        let fut = source_fut
            .map_err(|e| format!("{:?}", e))
            .map(|dopt| dopt.map(|d| bincode::deserialize::<T>(&d)));
        Data {
            source: RefCell::new(box fut),
            source_type: DataSourceType::GlobalStorage(id, key)
        }
    }
    pub fn from_global_storage(manager: &Arc<GlobalManager>, id: UUID, key: Vec<u8>, cached: bool)
        -> Data<Option<T>>
    {
        let source_fut: Box<Future<Item = Option<Vec<u8>>, Error = GlobalStorageError>> =
            if cached {
                box manager.get_cached(id, &key).into_future()
            } else {
                box manager.get_newest(id, true, key.clone())
            };
        Self::from_global_storage_future(source_fut, id, key)
    }

    pub fn from_immutable(manager: &Arc<ImmutableManager>, task: UUID, key: UUID)
        -> Data<Option<T>>
    {
        Data {
            source: RefCell::new(box manager.get(task, key)
                .map(|dopt|
                    dopt.map(|d|
                        bincode::deserialize::<T>(&d)))),
            source_type: DataSourceType::ImmutableStorage(task, key)
        }
    }

    pub fn error_on_none(input: Data<Option<T>>) -> Data<T> {
        let src = input.source.into_inner();
        return Data {
            source: RefCell::new(
                box src.and_then(|opt|
                    opt.ok_or_else(||
                        "cannot find data".to_string()))),
            source_type: input.source_type
        };
    }

    pub fn ser_data(&self) -> SerdeData {
        match self.source_type {
            DataSourceType::Runtime => {
                let mut source = self.source.borrow_mut();
                while let Ok(Async::Ready(item)) = source.poll() {
                    let data = bincode::serialize(&item);
                    return SerdeData::Runtime(data)
                }
                unreachable!()
            },
            DataSourceType::GlobalStorage(ref task, ref key) =>
                SerdeData::GlobalStorage(*task, key.clone()),
            DataSourceType::ImmutableStorage(ref task, ref key) =>
                SerdeData::ImmutableStorage(*task, *key)
        }
    }

    fn from_de_data(data: SerdeData) -> Data<T> {
        match data {
            SerdeData::Runtime(ref data) =>
                Data::from(bincode::deserialize::<T>(data)),
            SerdeData::GlobalStorage(task, key) =>
                Data::from_global_storage(&GlobalManager::default(), task, key, false).unwrap(),
            SerdeData::ImmutableStorage(task, key) =>
                Data::from_immutable(&ImmutableManager::default(), task, key).unwrap()
        }
    }
}

impl <T> Data <Option<T>> where T: Serialize + DeserializeOwned + 'static {
    fn unwrap(self) -> Data<T> {
        Data::error_on_none(self)
    }
}

impl <T> From <T> for Data<T> where T: Serialize + DeserializeOwned + 'static {
    fn from(d: T) -> Self {
        Self::from_fut(Ok::<T, String>(d))
    }
}

impl <T> Future for Data<T> where T: Serialize + DeserializeOwned + 'static {
    type Item = T;
    type Error = String;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut source = self.source.get_mut();
        source.poll()
    }
}

impl <T> Serialize for Data<T> where T: Serialize + DeserializeOwned + 'static {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer
    {
        let ser_struct = self.ser_data();
        SerdeData::serialize(&ser_struct, serializer)
    }
}

impl <'de, T> Deserialize<'de> for Data<T>
    where T: Serialize + DeserializeOwned + 'static
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where
        D: Deserializer<'de>
    {
        let de_data = SerdeData::deserialize(deserializer)?;
        Ok(Self::from_de_data(de_data))
    }
}

pub fn to_optional_binary<T>(bin: &Option<T>) -> Option<Vec<u8>> where T: Serialize {
    return if let &Some(ref b) = bin { Some(bincode::serialize(b)) } else { None };
}