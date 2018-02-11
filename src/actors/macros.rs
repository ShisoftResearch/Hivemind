#[macro_export]
macro_rules! ident_id {
    ($expr: tt) => {
        ::bifrost_hasher::hash_str(concat!(module_path!(), "::", stringify!($expr)))
    };
}

#[macro_export]
macro_rules! def_remote_func {
    ($($name: ident($($enclosed:ident : $ety: ty),*) -> $rt:ty | $re: ty $body:block)*) =>
    {
        $(
            #[derive(Serialize, Deserialize, Debug, Clone)]
            pub struct $name {
               $(pub $enclosed: $ety),*
            }
            impl RemoteFunc for $name {

                type Out = $rt;
                type Err =  $re;

                #[async (boxed)]
                fn call(this: Self)
                    -> Result<Self::Out, Self::Err>
                {
                    let ( $($enclosed,)* ) = ( $(this.$enclosed,)* );
                    $body
                }
                fn id() -> u64 {
                    ident_id!($name)
                }
            }
        )*
    };
}