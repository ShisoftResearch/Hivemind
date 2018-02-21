#[macro_export]
macro_rules! ident_id {
    ($expr: tt) => {
        ::bifrost_hasher::hash_str(concat!(module_path!(), "::", stringify!($expr)))
    };
}

#[macro_export]
macro_rules! def_remote_func {
    ($($name: ident($($enclosed:ident : $ety: ty),*) [$($dataset:ident : $dty: ty),*]
        -> $rt:ty | $re: ty $body:block)*) =>
    {
        $(
            #[derive(Serialize, Deserialize)]
            pub struct $name {
               $(pub $enclosed: $ety,)*
               $(pub $dataset: $dty ,)*

            }
            impl RemoteFunc for $name {

                type Out = $rt;
                type Err =  $re;

                fn call(self: Box<Self>)
                    -> Box<Future<Item = Self::Out, Error = Self::Err>>
                {
                    $(let $enclosed: $ety = self.$enclosed;)*
                    box async_block!($body)
                }
                fn id() -> u64 {
                    ident_id!($name)
                }
                fn get_affinity(&self) -> Vec<u64> {
                    use $crate::actors::funcs::LocationTraced;
                    let mut id_set = ::std::collections::HashSet::new();
                    $(id_set.extend(self.$dataset.get_affinity());)*
                    return id_set.into_iter().collect();
                }
            }
        )*
    };
}