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
use resource::DataSet;
use serde::de::DeserializeOwned;
use storage::block::BlockManager;
use utils::uuid::UUID;
use server::HMServer;

const STORAGE_BUFFER: u64 = 10;

lazy_static!{
    pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}

pub struct Hive {
    block_manager: Arc<BlockManager>,
    server: Arc<HMServer>
}

impl Hive {
    /// Distribute data across the cluster and return the local data set for further processing
    pub fn distribute<'a, S, T>(name: &'a str, source: DataSet<T>)
        -> DataSet<T> where T: DeserializeOwned
    {
        unimplemented!()
    }
    /// Set a value in the data store which visible across the cluster
    pub fn set<'a, V>(name: &'a str, value: V) where V: serde::Serialize {
        unimplemented!()
    }
    /// Get the value from set function
    pub fn get<'a, V>(name: &'a str) -> Option<V> {
        unimplemented!()
    }
    /// Run a remote function closure across the whole cluster
    pub fn run<F>(func: F) where F: RemoteFunc {
        unimplemented!()
    }
    /// Return a DataSet that it's contents from the object provided
    pub fn data_from<II, T, I>(&self, source: II) -> DataSet<T>
        where T: DeserializeOwned,
              I: Iterator<Item = T> + 'static,
              II: IntoIterator<Item = T, IntoIter = I>
    {
        DataSet::from(source)
    }
    /// Return a DataSet that it's contents from local distributed block storage
    pub fn data_from_storage<T>(&self, id: UUID) -> DataSet<T>
        where T: DeserializeOwned + 'static
    {
        DataSet::from_block_storage(&self.block_manager, self.server.server_id, id, STORAGE_BUFFER)
    }
    /// Return a DataSet that it's contents from remote distributed block storage
    /// Typically it should be used for aggregate functions
    pub fn data_from_remote_storage<T>(&self, server_id: u64, id: UUID) -> DataSet<T>
        where T: DeserializeOwned + 'static
    {
        DataSet::from_block_storage(&self.block_manager, server_id, id, STORAGE_BUFFER)
    }
}