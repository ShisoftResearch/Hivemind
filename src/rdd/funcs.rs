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

pub trait RDDFunc<FA, FR>: Serialize + Clone + Sized {
    type ARGS;
    fn call<'a>(&self, args: FA) -> FR;
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
            impl RDDFunc<($($argt,)*), $rt> for $name {
                type ARGS = ( $($argt,)*);
                fn call<'a>(&self, args: Self::ARGS) -> $rt {
                    let ( $($farg,)* ) = args;
                    let ( $($enclosed,)* ) = ( $(self.$enclosed,)* );
                    $body
                }
            }
        )*
    };
}

mod Test {
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
        assert_eq!(APlusB{}.call((1, 2)), 3);
        assert_eq!(AMultB{}.call((2, 3)), 6);
        assert_eq!(AMultC{c: 5}.call((2,)), 10);
    }
}