#![feature(plugin)]
#![feature(concat_idents)]
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

mod rdd;
mod contexts;
