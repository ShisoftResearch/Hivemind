// RDD functions will compiled at application compile time. The only way to get the the function at
// runtime by ids is to register it's runtime pointer in the registry.

// Each RDD that have a function will contain only function id in the RDD struct. Actual function
// pointer will get from the registry. Because the pointer is just a address number and does not
// carry any useful type meta data, it will be hard to ensure type safety (or even not to do it)

// User also have to register functions manually, because it seems like macro is pure function and
// it is not likely to introduce side effect and emmit the registry code elsewhere

// Spark like RDD closure may not be possible because manual register require a identifier.
// To ensure partial safety, it will only check number of parameter for RDD functions

pub trait RDDFunc {

}

macro_rules! count_args {
    () => {0usize};
    ($_head:tt $($tail:tt)*) => {1usize + count_tts!($($tail)*)};
}

