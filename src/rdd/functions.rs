// RDD functions will compiled at application compile time. The only way to get the the function at
// runtime by ids is to register it's runtime pointer in the registry.

// Each RDD that have a function will contain only function id in the RDD struct. Actual function
// pointer will get from the registry. Because the pointer is just a address number and does not
// carry any useful type meta data, it will be hard to ensure type safety (or even not to do it)

// User also have to register functions manually, because it seems like macro is pure function and
// it is not likely to introduce side effect and emmit the registry code elsewhere

// Spark like RDD closure may not be possible because manual register require a identifier.
// To ensure partial safety, it will only check number of parameter for RDD functions

pub trait RDDFunc<F, FA, FR> where F: Fn(FA) -> FR {
    fn id() -> u64;
    fn call(args: FA) -> FR;
    fn args() -> u64;
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
    ($name: ident($($arg:ident : $t: ty),*) -> $rt:ty $body:block) => {
        pub struct $name;
        impl RDDFunc<fn(($($t),*)) -> $rt, ($($t),*), $rt> for $name {
            fn id() -> u64 { fn_id!($name) }
            fn call(args: ($($t),*)) -> $rt {
                let ($($arg),*) = args;
                $body
            }
            fn args() -> u64 { count_args!($($arg),*) }
        }
    };
}

mod Test {
    use rdd::functions::RDDFunc;
    def_rdd_func!(Test (a: u64, b: u64) -> u64 {
        a + b
    });
    #[test]
    fn test_a_plus_b_rdd() {
        assert_eq!(Test::call((1, 2)), 3);
        println!("a + b rdd function id is: {}", Test::id());
    }
}