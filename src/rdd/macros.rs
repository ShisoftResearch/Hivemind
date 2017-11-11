#[macro_export]
macro_rules! impl_rdd_tracker {
    ($name: ident) => {
        impl RDDTracker for $name {
            fn trans_id() -> u64 {
                ident_id!($name)
            }
        }
    };
}

#[macro_export]
macro_rules! count_args {
    () => {0u64};
    ($_head:tt $($tail:tt)*) => {1u64 + count_args!($($tail)*)};
}

#[macro_export]
macro_rules! ident_id {
    ($expr: tt) => {
        ::bifrost_hasher::hash_str(concat!(module_path!(), "::", stringify!($expr)))
    };
}

#[macro_export]
macro_rules! def_rdd_func {
    ($($name: ident($($farg:ident : $argt: ty),*)
                   [$($enclosed:ident : $ety: ty),*] -> $rt:ty $body:block)*) =>
    {
        $(
            #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
            pub struct $name {
               $(pub $enclosed: $ety),*
            }
            impl RDDFunc for $name {
                fn call(closure: Box<::std::any::Any>, args: Box<::std::any::Any>)
                    -> RDDFuncResult
                {
                    match closure.downcast_ref::<Self>() {
                        Some(closure) => {
                             match args.downcast_ref::<( $($argt,)* )>() {
                                Some(args) => {
                                    let &( $($farg,)* ) = args;
                                    let ( $($enclosed,)* ) = ( $(closure.$enclosed,)* );
                                    return RDDFuncResult::Ok(Box::new($body as $rt));
                                },
                                None => {
                                    return RDDFuncResult::Err(format!("Cannot cast type: {:?}", args));
                                }
                            }
                        },
                        None => {
                          return RDDFuncResult::Err(format!("closure is not for the rdd function {:?}", closure));
                        }
                    }
                }
                fn id() -> u64 {
                    ident_id!($name)
                }
                fn decode(bytes: &Vec<u8>) -> Self {
                    ::bifrost::utils::bincode::deserialize(bytes)
                }
            }
        )*
    };
}