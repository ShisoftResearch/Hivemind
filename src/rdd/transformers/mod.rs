use std::collections::BTreeMap;
use std::cell::RefCell;
use std::any::Any;
use rdd::RDD;

pub mod map;
pub mod filter;
pub mod map_partitions;

#[derive(Clone)]
pub struct RegedTrans {
    pub construct: fn (Box<Any>) -> Result<Box<RDD>, String>,
    pub construct_args: fn (&Vec<u8>) -> Box<Any>
}


pub struct Registry {
    map: RefCell<BTreeMap<u64, RegedTrans>>
}

impl Registry {
    fn new() -> Registry {
        Registry {
            map: RefCell::new(BTreeMap::new()),
        }
    }
    pub fn register(
        &self,
        id: u64,
        construct: fn (Box<Any>) -> Result<Box<RDD>, String>,
        construct_args: fn (&Vec<u8>) -> Box<Any>
    ) {
        let mut reg = self.map.borrow_mut();
        reg.insert(id, RegedTrans {
            construct, construct_args
        });
    }
    pub fn get(&self, id: u64) -> Option<RegedTrans> {
        let borrowed = self.map.borrow();
        borrowed.get(&id).cloned()
    }
}

unsafe impl Sync for Registry {}

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}