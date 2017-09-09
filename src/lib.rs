#![feature(plugin)]
#![feature(concat_idents)]
#![plugin(bifrost_plugins)]

extern crate bifrost_hasher;
extern crate serde;
#[macro_use]
extern crate lazy_static;

mod rdd;
