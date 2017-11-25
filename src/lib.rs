#![feature(plugin)]
#![feature(concat_idents)]
#![feature(box_syntax)]
#![plugin(bifrost_plugins)]

extern crate bifrost_hasher;
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
extern crate futures_cpupool;


#[macro_use]
mod rdd;
mod contexts;
mod server;

use parking_lot::Mutex;

lazy_static!{
        pub static ref INIT_LOCK: Mutex<()> = Mutex::new(());
}
