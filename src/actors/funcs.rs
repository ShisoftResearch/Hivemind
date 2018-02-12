use std::cell::{RefCell, BorrowMutError};
use std::collections::BTreeMap;
use std::mem::transmute;
use libc;
use serde::{Serialize, Deserialize};
use std::any::Any;
use futures::prelude::*;
use std::mem;

pub use INIT_LOCK;

#[derive(Clone)]
pub struct RegistedFunc {
    pub id: u64,
    pub func: *const libc::c_void,
    pub decode: fn(&Vec<u8>) -> Box<Any>
}

pub struct Registry {
    map: RefCell<BTreeMap<u64, RegistedFunc>>
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
        m.insert(id, RegistedFunc { id, func, decode});
        Ok(())
    }
    pub fn get(&self, id: u64) -> Option<RegistedFunc> {
        let m = self.map.borrow();
        m.get(&id).cloned()
    }
}

impl RegistedFunc {
    pub fn call(&self, func: Box<Any>) -> Box<Any> {
        let call_func: fn(Box<Any>) -> Box<Any> = unsafe { transmute(self.func) };
        return call_func(func)
    }
    pub fn decode(&self, data: &Vec<u8>) -> Box<Any> {
        return (self.decode)(data)
    }
}

unsafe impl Sync for Registry {}

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

pub trait RemoteFunc: Serialize + for <'a> Deserialize<'a> + Sized + Clone + 'static {

    type Out;
    type Err;

    fn call(self: Box<Self>) -> Box<Future<Item = Self::Out, Error = Self::Err>>;
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

mod test {
    use super::*;
    use bifrost::utils::bincode;
    use futures::prelude::*;

    struct DummyFuture;
    impl Future for DummyFuture {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            Ok(Async::Ready(()))
        }
    }

    def_remote_func!(
        APlusB (a: u64, b: u64) -> u64 | () {
            await!(DummyFuture);
            Ok(self.a + self.b)
        }
        AMultB (a: u32, b: u32) -> u32 | () {
            Ok(self.a * self.b)
        }
        AMultC (a: u32, c: u32) -> u32 | () {
            Ok(self.a * self.c)
        }
    );
    fn prepare_registry() {
        let lock = INIT_LOCK.lock();
        APlusB::register().unwrap();
        AMultB::register().unwrap();
        AMultC::register().unwrap();
    }

    #[test]
    fn decode_from_register() {
        prepare_registry();
        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
        let reg_func_b = REGISTRY.get(AMultB::id()).unwrap();
        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();

        let ai = APlusB{a: 1, b: 2};
        let bi = AMultB{a: 2, b: 3};
        let ci = AMultC{a: 4, c: 5};

        let ab = bincode::serialize(&ai);
        let bb = bincode::serialize(&bi);
        let cb = bincode::serialize(&ci);

        let a_de_any = (reg_func_a.decode)(&ab);
        let b_de_any = (reg_func_b.decode)(&bb);
        let c_de_any = (reg_func_c.decode)(&cb);

        let a_de_any_res = reg_func_a.call(a_de_any);
        let b_de_any_res = reg_func_b.call(b_de_any);
        let c_de_any_res = reg_func_c.call(c_de_any);

        let a_de_res: Box<Future<Item = u64, Error = ()>> = unsafe { mem::transmute(a_de_any_res) };
        let b_de_res: Box<Future<Item = u32, Error = ()>> = unsafe { mem::transmute(b_de_any_res) };
        let c_de_res: Box<Future<Item = u32, Error = ()>> = unsafe { mem::transmute(c_de_any_res) };

        assert_eq!(a_de_res.wait().unwrap(), 3);
        assert_eq!(b_de_res.wait().unwrap(), 6);
        assert_eq!(c_de_res.wait().unwrap(), 20);
    }
}