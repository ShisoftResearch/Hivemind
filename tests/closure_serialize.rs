use bifrost::utils::bincode::*;
use bifrost_hasher::hash_str;

macro_rules! fn_id {
    ($expr: tt) => {
        hash_str(concat!(module_path!(), "::", stringify!($expr)))
    };
}

macro_rules! serialize {
    ($fn: expr) => {
       let func_id = fn_id!($fn);

    };
}

#[test]
pub fn native_closure() {
    serialize!(|x: u32| 1);
}