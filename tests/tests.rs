#![feature(plugin)]
#![plugin(bifrost_plugins)]

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bifrost;
#[macro_use]
extern crate bifrost_hasher;

mod closure_serialize;