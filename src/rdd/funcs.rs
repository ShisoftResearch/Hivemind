use std::cell::{RefCell, BorrowMutError};
use std::collections::HashMap;
use std::mem::transmute;
use serde::{Serialize, Deserialize};

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
    pub args: u64 // for validation
}

impl RegistryRDDFunc {
//    pub unsafe fn call<F, A, R>(&self, func_obj: &F, params: A) -> R where F: RDDFunc<A, R> {
//        let func = transmute::<_, fn(&F, A) -> R>(self.func_ptr);
//        func(func_obj, params)
//    }
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
    pub fn register(&self, id: u64, ptr: *const (), args: u64) -> Result<(), BorrowMutError> {
        let mut m = self.map.try_borrow_mut()?;
        m.insert(id, RegistryRDDFunc {
            id: id, func_ptr: ptr, args: args
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

pub trait RDDFunc {
    fn call<ARGS, FR>(&self, args: ARGS) -> FR
        where ARGS: ::std::any::Any,
              FR:   ::std::any::Any;
    fn id() -> u64;
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
                   [$($enclosed:ident : $ety: ty),*] -> $rt:ty $body:block)*) => {
        $(
            #[derive(Serialize, Deserialize, Clone)]
            pub struct $name {
               $(pub $enclosed: $ety),*
            }
            impl RDDFunc for $name {
                fn call<ARGS>(this: Self, args: ARGS) -> $rt where ARGS: ::std::any::Any {
                    let value_any = args as &::std::any::Any;
                    let ( $($farg,)* ) = args;
                    let ( $($enclosed,)* ) = ( $(self.$enclosed,)* );
                    $body
                }
                fn id() -> u64 {
                    fn_id!($name)
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
        ARefAddBRef(a: &'static u64, b: &'static u64)[] -> u64 {
            a + b
        }
    );
    #[test]
    fn test_a_b_rdd() {
        assert_eq!(APlusB{}.call((1, 2)), 3);
        assert_eq!(AMultB{}.call((2, 3)), 6);
        assert_eq!(AMultC{c: 5}.call((2,)), 10);
        assert_eq!(ARefAddBRef{}.call((&1, &2)), 3)
    }
}