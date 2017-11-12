use std::collections::BTreeMap;
use std::cell::RefCell;

pub mod map;

pub struct RegistryTransformer {

}

impl RegistryTransformer {
    pub fn new() -> RegistryTransformer {
        RegistryTransformer {

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
}

unsafe impl Sync for Registry {}

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}