#![feature(plugin)]
#![feature(box_syntax)]

#[macro_use]
extern crate log;
extern crate bifrost_hasher;
#[macro_use]
extern crate bifrost;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
extern crate uuid;
extern crate parking_lot;
extern crate typedef;
extern crate futures;

pub mod funcs;
mod global_store;
mod local_store;
mod scheduler;

use parking_lot::Mutex;

lazy_static!{
        pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}
