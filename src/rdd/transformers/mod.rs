use std::collections::BTreeMap;
use std::cell::RefCell;
use std::any::Any;

pub mod map;

pub struct RegistryTransformer {
    constructor: fn (Box<Any>) -> Result<Box<Any>, String>
}

impl RegistryTransformer {
    pub fn new(
        constructor: fn (Box<Any>) -> Result<Box<Any>, String>
    ) -> RegistryTransformer {
        RegistryTransformer {
            constructor
        }
    }
}

pub struct Registry {
    map: RefCell<BTreeMap<u64, RegistryTransformer>>
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
        constructor: fn (Box<Any>) -> Result<Box<Any>, String>
    ) {
        let mut reg = self.map.borrow_mut();
        reg.insert(id, RegistryTransformer::new(
            constructor
        ));
    }
}

unsafe impl Sync for Registry {}

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}