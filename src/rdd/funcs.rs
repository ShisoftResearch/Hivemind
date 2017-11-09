use std::cell::{RefCell, BorrowMutError};
use std::collections::HashMap;
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
    pub func_ptr: *const (),
}

impl RegistryRDDFunc {
    pub unsafe fn call<F, A, R>(&self, func_obj: &F, params: A) -> Result<R, String>
        where F: RDDFunc,
              R: Any + Clone,
              A: Any
    {
        let func = transmute::<_, fn(&F, Box<Any>) -> RDDFuncResult>(self.func_ptr);
        func(func_obj, Box::new(params)).cast()
    }
}

pub struct Registry {
    map: RefCell<HashMap<u64, RegistryRDDFunc>>
}

impl Registry {
    pub fn new() -> Registry {
        Registry {
            map: RefCell::new(HashMap::new())
        }
    }
    pub fn register(&self, id: u64, ptr: *const ()) -> Result<(), BorrowMutError> {
        let mut m = self.map.try_borrow_mut()?;
        m.insert(id, RegistryRDDFunc {
            id, func_ptr: ptr
        });
        Ok(())
    }
    pub fn get<'a>(&self, id: u64) -> Option<RegistryRDDFunc> {
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
}

pub trait RDDFunc: Serialize {
    fn call(&self, args: Box<::std::any::Any>) -> RDDFuncResult;
    fn id() -> u64;
    fn decode(bytes: &Vec<u8>) -> Self where Self: Sized;
    fn register() -> Result<(), BorrowMutError> {
        REGISTRY.register(Self::id(), Self::call as *const ())
    }
}

macro_rules! count_args {
    () => {0u64};
    ($_head:tt $($tail:tt)*) => {1u64 + count_args!($($tail)*)};
}

macro_rules! fn_id {
    ($expr: tt) => {
        ::bifrost_hasher::hash_str(concat!(module_path!(), "::", stringify!($expr)))
    };
}

macro_rules! def_rdd_func {
    ($($name: ident($($farg:ident : $argt: ty),*)
                   [$($enclosed:ident : $ety: ty),*] -> $rt:ty $body:block)*) =>
    {
        $(
            #[derive(Serialize, Deserialize, Clone)]
            pub struct $name {
               $(pub $enclosed: $ety),*
            }
            impl RDDFunc for $name {
                fn call(&self, args: Box<::std::any::Any>) -> RDDFuncResult {
                    match args.downcast_ref::<( $($argt,)* )>() {
                        Some(args) => {
                            let &( $($farg,)* ) = args;
                            let ( $($enclosed,)* ) = ( $(self.$enclosed,)* );
                            return RDDFuncResult::Ok(Box::new($body as $rt));
                        },
                        None => {
                            return RDDFuncResult::Err(format!("Cannot cast type: {:?}", args));
                        }
                    }
                }
                fn id() -> u64 {
                    fn_id!($name)
                }
                fn decode(bytes: &Vec<u8>) -> Self where Self: Sized {
                    ::bifrost::utils::bincode::deserialize(bytes)
                }
            }
        )*
    };
}

mod test {
    use super::*;
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
    #[test]
    fn test_a_b_rdd() {
        assert_eq!(APlusB{}.call(Box::new((1 as u64, 2 as u64))).cast::<u64>().unwrap(), 3);
        assert_eq!(AMultB{}.call(Box::new((2 as u32, 3 as u32))).cast::<u32>().unwrap(), 6);
        assert_eq!(AMultC{c: 5}.call(Box::new((2 as u32,))).cast::<u32>().unwrap(), 10);
    }
    #[test]
    fn register_and_invoke_from_registry_by_ptr() {
        APlusB::register().unwrap();
        AMultB::register().unwrap();
        AMultC::register().unwrap();
        let reg_func_a = REGISTRY.get(APlusB::id()).unwrap();
        let reg_func_b = REGISTRY.get(AMultB::id()).unwrap();
        let reg_func_c = REGISTRY.get(AMultC::id()).unwrap();

        assert_eq!(unsafe { reg_func_a.call::<_, (u64, u64), u64>(&APlusB{}, (1, 2)).unwrap()}, 3);
        assert_eq!(unsafe { reg_func_b.call::<_, (u32, u32), u32>(&AMultB{}, (2, 3)).unwrap()}, 6);
        assert_eq!(unsafe { reg_func_c.call::<_, (u32,), u32>(&AMultC{c: 5}, (2,)).unwrap()}, 10);
    }
}