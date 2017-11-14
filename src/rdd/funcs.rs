use std::cell::{RefCell, BorrowMutError};
use std::collections::BTreeMap;
use std::mem::transmute;
use serde::{Serialize, Deserialize};
use std::any::Any;

// RDD functions will compiled at application compile time. The only way to get the the function at
// runtime by ids is to register it's runtime pointer in the registry.

// Each RDD that have a function will contain only function id in the RDD struct. Actual function
// pointer will get from the registry. Because the pointer is just a address number and does not
// carry any useful type meta data, it will be hard to ensure type safety (or even not to do it)

// User also have to register functions manually, because it seems like macro is pure function and
// it is not likely to introduce side effect and emmit the registry code elsewhere

// Spark like RDD closure may not be possible because manual register require a identifier.
// To ensure partial safety, it will only check number of parameter for RDD functions
#[derive(Clone, Copy)]
pub struct RegistryRDDFunc {
    pub id: u64,
    pub func: fn(&Box<Any>, &Box<Any>) -> RDDFuncResult,
    pub decode: fn(&Vec<u8>) -> Box<Any>,
    pub clone: fn(&Box<Any>) -> Box<Any>,
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
        func: fn(&Box<Any>, &Box<Any>) -> RDDFuncResult,
        decode: fn(&Vec<u8>) -> Box<Any>, clone: fn(&Box<Any>) -> Box<Any>
    ) -> Result<(), BorrowMutError> {
        let mut m = self.map.try_borrow_mut()?;
        m.insert(id, RegistryRDDFunc { id, func, decode, clone });
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

#[derive(Debug)]
pub enum RDDFuncResult {
    Ok(Box<Any>),
    Err(String)
}

impl RDDFuncResult {
    pub fn cast<T: Clone + 'static>(&self) -> Result<T, String>
    {
        match self {
            &RDDFuncResult::Ok(ref data) => {
                match data.downcast_ref::<T>() {
                    Some(res) => {
                        return Ok(res.clone())
                    },
                    None => {
                        return Err(format!("RDD result type mismatch"))
                    }
                }
            },
            &RDDFuncResult::Err(ref e) => {
                return Err(format!("RDD result is error: {}", e))
            }
        }
    }
    pub fn unwrap_to_any(self) -> Box<Any> {
        match self {
            RDDFuncResult::Ok(data) => {
                data
            },
            RDDFuncResult::Err(err) => {
                panic!("cannot unwrap rdd func result: {}", err);
            }
        }
    }
}

pub trait RDDFunc: Serialize + Sized + Clone + 'static {
    fn call(closure: &Box<Any>, args: &Box<Any>) -> RDDFuncResult;
    fn id() -> u64;
    fn decode(bytes: &Vec<u8>) -> Box<Any>;
    fn boxed_clone(closure: &Box<Any>) -> Box<Any>;
    fn into_any(self) -> Box<Any> {
        Box::new(self)
    }
    fn register() -> Result<(), BorrowMutError> {
        REGISTRY.register(
            Self::id(),
            Self::call,
            Self::decode,
            Self::boxed_clone,
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
    use parking_lot::Mutex;
    use bifrost::utils::bincode;

    def_rdd_func!(
        APlusB (a: u64, b: u64)[] -> u64 {
            a + b
        }
        AMultB (a: u32, b: u32)[] -> u32 {
            a * b
        }
        AMultC (a: u32)[c: u32] -> u32 {
            a * c
        }
    );
    lazy_static!{
        pub static ref INIT_LOCK: Mutex<bool> = Mutex::new(false);
    }
    fn prepare_registry() {
        let mut inited = INIT_LOCK.lock();
        if *inited {
            return;
        }
        APlusB::register().unwrap();
        AMultB::register().unwrap();
        AMultC::register().unwrap();
        *inited = true;
    }
    fn reg_call<A, R>(regf: &RegistryRDDFunc, closure: &Box<Any>, params: A) -> Result<R, String>
        where R: Any + Clone, A: Any
    {
        (regf.func)(closure, &to_any(params)).cast()
    }
    #[test]
    fn test_a_b_rdd() {
        assert_eq!(APlusB::call(&box APlusB{}.into_any(), &to_any((1 as u64, 2 as u64))).cast::<u64>().unwrap(), 3);
        assert_eq!(AMultB::call(&box AMultB{}.into_any(), &to_any((2 as u32, 3 as u32))).cast::<u32>().unwrap(), 6);
        assert_eq!(AMultC::call(&box AMultC{c: 5}.into_any(), &to_any((2 as u32,))).cast::<u32>().unwrap(), 10);
    }
    #[test]
    fn register_and_invoke_from_registry_by_ptr() {
        prepare_registry();
        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
        let reg_func_b = REGISTRY.get(AMultB::id()).unwrap();
        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();

        assert_eq!(reg_call::<(u64, u64), u64>(&reg_func_a, &APlusB{}.into_any(), (1, 2)).unwrap(), 3);
        assert_eq!(reg_call::<(u32, u32), u32>(&reg_func_b, &AMultB{}.into_any(), (2, 3)).unwrap(), 6);
        assert_eq!(reg_call::<(u32,), u32>(&reg_func_c, &AMultC{c: 5}.into_any(), (2,)).unwrap(), 10);
    }
    #[test]
    fn decode_from_register() {
        prepare_registry();
        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();

        let ai = APlusB{};
        let ci = AMultC{c: 5};

        let ab = bincode::serialize(&ai);
        let cb = bincode::serialize(&ci);

        let a_de_any = (reg_func_a.decode)(&ab);
        let c_de_any = (reg_func_c.decode)(&cb);

        let a_de = a_de_any.downcast_ref::<APlusB>().unwrap();
        let c_de = c_de_any.downcast_ref::<AMultC>().unwrap();

        assert_eq!(&ai, a_de);
        assert_eq!(&ci, c_de);
    }
}