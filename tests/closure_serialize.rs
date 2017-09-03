use bifrost::utils::bincode::*;
use bifrost_hasher::hash_str;

macro_rules! fn_id {
    ($expr: tt) => {
        hash_str(stringify!($expr))
    };
}

macro_rules! seirialize {
    ($fn: expr) => {
       let func_id = fn_id!($fn) as u64;

    };
}

#[test]
pub fn native_closure() {
    seirialize!(|x: u32| 1);
}