#![feature(plugin)]
#![feature(concat_idents)]
#![feature(box_syntax)]
#![plugin(bifrost_plugins)]

#![feature(proc_macro, conservative_impl_trait, generators)]
#![feature(box_syntax)]

#[macro_use]
extern crate log;
extern crate bifrost_hasher;
#[macro_use]
extern crate bifrost;
#[macro_use]
extern crate neb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
extern crate uuid;
extern crate parking_lot;
extern crate typedef;
extern crate futures_await as futures;
extern crate futures_cpupool;
extern crate byteorder;
extern crate libc;

#[macro_use]    
pub mod server;
pub mod actors;
pub mod storage;
pub mod utils;
pub mod scheduler;
pub mod resource;

use std::sync::Arc;
use parking_lot::Mutex;
use actors::funcs::RemoteFunc;
use resource::{DataSet, Data, STORAGE_BUFFER, to_optional_binary};
use serde::de::DeserializeOwned;
use serde::Serialize;
use storage::block::BlockManager;
use storage::global::GlobalManager;
use storage::immutable::ImmutableManager;
use utils::uuid::UUID;
use utils::stream::RepeatVec;
use server::HMServer;
use futures::{Stream, Future};
use bifrost::utils::bincode;

lazy_static!{
    pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}

/// Hive is a basic unit for each task, it have it's own preassigned server lists, which means the
/// scheduler can choose which group of the server in the cluster to run the this task with n processors
pub struct Hive {
    task_id: UUID,
    block_manager: Arc<BlockManager>,
    global_manager: Arc<GlobalManager>,
    immutable_manager: Arc<ImmutableManager>,
    members: Vec<u64>,
    server: Arc<HMServer>
}

impl Hive {
    /// Distribute data across the cluster and return the local data set for further processing
    /// this function have a scope for each task. All distributed data are only accessible in their hive
    pub fn distribute<T>(&self, source: DataSet<T>)
        -> impl Future<Item = DataSet<T>, Error = String>
        where T: Serialize + DeserializeOwned + 'static
    {
        let members = self.members.clone();
        let repeat_members = RepeatVec::new(members.clone())
            .map_err(|_| String::from(""));
        let num_members = self.members.len();
        let id = UUID::rand();
        let block_manager = self.block_manager.clone();
        let block_manager2 = self.block_manager.clone();
        let this_server_id = self.server.server_id;
        let task = self.task_id;
        let distribute_fut = source
            .map(|item: T| {
                bincode::serialize(&item)
            })
            .chunks(STORAGE_BUFFER as usize)
            .zip(repeat_members)
            .map(move |pair: (Vec<Vec<u8>>, u64)|{
                let (chunk, server_id) = pair;
                block_manager.write(server_id, &task, &id, &chunk)
            })
            .buffered(num_members)
            .for_each(|_| Ok(()));
        distribute_fut.map(move |_| {
            DataSet::from_block_storage(
                &block_manager2, this_server_id, task, id,
                members,
                STORAGE_BUFFER)
        })
    }
    /// Set a global value in the data store which visible across the cluster
    /// this function have a scope for each task. All set values are only available in their hive
    pub fn set_global<K, V>(&self, key: &K, value: &Option<V>)
        -> impl Future<Item = Data<V>, Error = String>
        where V: Serialize + DeserializeOwned + 'static, K: Serialize
    {
        let key = bincode::serialize(key);
        let value = to_optional_binary(value);
        let global_storage_mgr = self.global_manager.clone();
        let id = self.task_id;
        self.global_manager.set(id, key.clone(), value)
            .map_err(|e| format!("{:?}", e))
            .and_then(move |_|
                Data::error_on_none(
                    Data::from_global_storage(&global_storage_mgr, id, key, false)))
    }
    /// Get the global data from set function
    /// this function have a scope for each task. They can only get the value in their scope
    pub fn get_global<K, V>(&self, key: &K)
        -> Data<Option<V>>
        where V: Serialize + DeserializeOwned + 'static, K: Serialize
    {
        let key = bincode::serialize(key);
        Data::from_global_storage(&self.global_manager, self.task_id, key, false)
    }
    /// Get the cached global data from set function
    /// this function have a scope for each task. They can only get the value in their scope
    pub fn get_global_cached<K, V>(&self, key: &K)
        -> Data<Option<V>>
        where V: Serialize + DeserializeOwned + 'static, K: Serialize
    {
        let key = bincode::serialize(key);
        Data::from_global_storage(&self.global_manager, self.task_id, key, true)
    }
    /// Swap global data
    /// this function have a scope for each task. They can only get the value in their scope
    pub fn swap_global<K, V>(&self, key: &K, value: &Option<V>)
        -> Data<Option<V>>
        where V: Serialize + DeserializeOwned + 'static, K: Serialize
    {
        let key = bincode::serialize(key);
        let value = to_optional_binary(value);
        let id = self.task_id;
        Data::from_global_storage_future(
            box self.global_manager.swap(id, key.clone(), value),
            id, key
        )
    }
    /// Compare and swap global data
    /// this function have a scope for each task. They can only get the value in their scope
    pub fn compare_and_swap_global<K, V>(&self, key: &K, expect: &Option<Vec<V>>, value: &Option<V>)
        -> Data<Option<V>>
        where V: Serialize + DeserializeOwned + 'static, K: Serialize
    {
        let key = bincode::serialize(key);
        let expect = to_optional_binary(expect);
        let value = to_optional_binary(value);
        let id = self.task_id;
        Data::from_global_storage_future(
            box self.global_manager
                .compare_and_swap(id, key.clone(), &expect, &value), id, key)
    }

    /// Write data to immutable store
    /// This will write data to local block storage and report it's presents
    /// on a global registry
    /// Id to the block should be random generated to avoid collision
    /// All read operations should happened after the block has been written
    pub fn write_immutable<T>(&self, source: DataSet<T>)
        -> impl Future<Item = DataSet<T>, Error = String>
        where T: Serialize + DeserializeOwned + 'static
    {
        let task_id = self.task_id;
        let block_id = UUID::rand();
        let immutable_manager = self.immutable_manager.clone();
        let immutable_manager2 = immutable_manager.clone();
        self.immutable_manager.ensure_registed(task_id, block_id)
            .and_then(move |_| {
                source
                    .map(move |item: T| {
                        bincode::serialize(&item)
                    })
                    .chunks(STORAGE_BUFFER as usize)
                    .map(move |items: Vec<Vec<u8>>| {
                        immutable_manager.write(task_id, block_id, items)
                    })
                    .buffered(STORAGE_BUFFER as usize)
                    .for_each(|_| Ok(()))
            })
            .map(move |_| {
                 DataSet::from_immutable_storage(
                     &immutable_manager2,
                     task_id, block_id, STORAGE_BUFFER)
            })
    }

    /// Run a remote function closure across the cluster members
    /// The function closure (func: F) provided must be serializable
    pub fn run<F>(&self, func: F) where F: RemoteFunc {
        unimplemented!()
    }
    /// Return a DataSet that it's contents from the object provided
    pub fn data_from<II, T, I>(&self, source: II) -> DataSet<T>
        where T: Serialize + DeserializeOwned,
              I: Iterator<Item = T> + 'static,
              II: IntoIterator<Item = T, IntoIter = I>
    {
        DataSet::from(source)
    }
}