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

use parking_lot::Mutex;
use actors::funcs::RemoteFunc;
use resource::DataSet;
use serde::de::DeserializeOwned;

lazy_static!{
    pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}

pub struct Hive {

}

impl Hive {
    pub fn distribute<'a, S, T>(name: &'a str, source: S)
        -> DataSet<T> where S: IntoIterator<Item = T>, T: DeserializeOwned
    {
        unimplemented!()
    }
    pub fn set<'a, V>(name: &'a str, value: V) where V: serde::Serialize {
        unimplemented!()
    }
    pub fn get<'a, V>(name: &'a str) -> Option<V> {
        unimplemented!()
    }
    pub fn run<F>(func: F) where F: RemoteFunc {
        unimplemented!()
    }
}