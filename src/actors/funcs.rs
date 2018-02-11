use std::cell::{RefCell, BorrowMutError};
use std::collections::BTreeMap;
use std::mem::transmute;
use libc;
use serde::{Serialize, Deserialize};
use std::any::Any;
use futures::prelude::*;

pub use INIT_LOCK;

#[derive(Clone)]
pub struct RegistryRDDFunc {
    pub id: u64,
    pub func: *const libc::c_void,
    pub decode: fn(&Vec<u8>) -> Box<Any>
}

pub struct Registry {
    map: RefCell<BTreeMap<u64, RegistryRDDFunc>>
}

impl Registry {
    pub fn new() -> Registry {
        Registry {
            map: RefCell::new(BTreeMap::new())
        }
    }
    pub fn register(
        &self, id: u64,
        func: *const libc::c_void,
        decode: fn(&Vec<u8>) -> Box<Any>
    ) -> Result<(), BorrowMutError> {
        let mut m = self.map.try_borrow_mut()?;
        m.insert(id, RegistryRDDFunc { id, func, decode});
        Ok(())
    }
    pub fn get(&self, id: u64) -> Option<RegistryRDDFunc> {
        let m = self.map.borrow();
        m.get(&id).cloned()
    }
}

unsafe impl Sync for Registry {}

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

pub trait RemoteFunc: Serialize + for <'a> Deserialize<'a> + Sized + Clone + 'static {

    type Out;
    type Err;

    fn call(this: Self) -> Box<Future<Item = Self::Out, Error = Self::Err>>;
    fn id() -> u64;
    fn decode(bytes: &Vec<u8>) -> Box<Any> {
        let de_res: Self = ::bifrost::utils::bincode::deserialize(bytes);
        box de_res
    }
    fn encode(&self) -> Vec<u8> {
        ::bifrost::utils::bincode::serialize(self)
    }
    fn register<'a>() -> Result<(), BorrowMutError> {
        REGISTRY.register(
            Self::id(),
            Self::call as *const libc::c_void,
            Self::decode
        )
    }
}

pub fn to_any<T>(x: T) -> Box<Any>
    where T: Any + 'static
{
    Box::new(x)
}

//mod test {
//    use super::*;
//    use bifrost::utils::bincode;
//    use futures::prelude::*;
//
//    def_remote_func!(
//        APlusB (a: u64, b: u64) -> u64 | () {
//            a + b
//        }
//        AMultB (a: u32, b: u32) -> u32 | () {
//            a * b
//        }
//        AMultC (a: u32) -> u32 | () {
//            a * c
//        }
//    );
//    fn prepare_registry() {
//        let lock = INIT_LOCK.lock();
//        APlusB::register().unwrap();
//        AMultB::register().unwrap();
//        AMultC::register().unwrap();
//    }
//    fn reg_call<A, R>(regf: &RegistryRDDFunc, closure: &Box<Any>, params: A) -> Result<R, String>
//        where R: Any + Clone, A: Any
//    {
//        (regf.func)(closure, &to_any(params)).inner()
//    }
//    #[test]
//    fn test_a_b_rdd() {
//        assert_eq!(APlusB::call(&box APlusB{}.into_any(), &to_any((1 as u64, 2 as u64))).inner::<u64>().unwrap(), 3);
//        assert_eq!(AMultB::call(&box AMultB{}.into_any(), &to_any((2 as u32, 3 as u32))).inner::<u32>().unwrap(), 6);
//        assert_eq!(AMultC::call(&box AMultC{c: 5}.into_any(), &to_any((2 as u32,))).inner::<u32>().unwrap(), 10);
//    }
//    #[test]
//    fn register_and_invoke_from_registry_by_ptr() {
//        prepare_registry();
//        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
//        let reg_func_b = REGISTRY.get(AMultB::id()).unwrap();
//        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();
//
//        assert_eq!(reg_call::<(u64, u64), u64>(&reg_func_a, &APlusB{}.into_any(), (1, 2)).unwrap(), 3);
//        assert_eq!(reg_call::<(u32, u32), u32>(&reg_func_b, &AMultB{}.into_any(), (2, 3)).unwrap(), 6);
//        assert_eq!(reg_call::<(u32,), u32>(&reg_func_c, &AMultC{c: 5}.into_any(), (2,)).unwrap(), 10);
//    }
//    #[test]
//    fn decode_from_register() {
//        prepare_registry();
//        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
//        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();
//
//        let ai = APlusB{};
//        let ci = AMultC{c: 5};
//
//        let ab = bincode::serialize(&ai);
//        let cb = bincode::serialize(&ci);
//
//        let a_de_any = (reg_func_a.decode)(&ab);
//        let c_de_any = (reg_func_c.decode)(&cb);
//
//        let a_de = a_de_any.downcast_ref::<APlusB>().unwrap();
//        let c_de = c_de_any.downcast_ref::<AMultC>().unwrap();
//
//        assert_eq!(&ai, a_de);
//        assert_eq!(&ci, c_de);
//    }
//}