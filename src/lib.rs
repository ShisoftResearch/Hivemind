#![feature(plugin)]
#![feature(concat_idents)]
#![plugin(bifrost_plugins)]

extern crate bifrost_hasher;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
extern crate uuid;

mod rdd;
mod contexts;
