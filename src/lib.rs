#![feature(plugin)]
#![feature(concat_idents)]
#![feature(box_syntax)]
#![plugin(bifrost_plugins)]

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
extern crate futures;
extern crate futures_cpupool;

#[macro_use]
pub mod rdd;
pub mod contexts;
pub mod server;
pub mod scheduler;
pub mod storage;

use parking_lot::Mutex;

lazy_static!{
        pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}
