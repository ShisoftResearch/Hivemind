#[macro_export]
macro_rules! impl_rdd_tracker {
    ($name: ident ($($carg:ident : $cargt: ty),*) $constructor:block) => {
        impl RDDTracker for $name {
            fn trans_id() -> u64 {
                ident_id!($name)
            }
            fn new(args: Box<Any>) -> Result<Self, String> {
                match args.downcast_ref::<( $($cargt,)* )>() {
                    Some(args) => {
                        let &( $(ref $carg,)* ) = args;
                        return $constructor
                    },
                    None => {
                        return Err(format!("Cannot cast type to create rdd: {:?}", args));
                    }
                }
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
            #[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
            pub struct $name {
               $(pub $enclosed: $ety),*
            }
            impl RDDFunc for $name {
                fn call(closure: &Box<::std::any::Any>, args: Box<::std::any::Any>)
                    -> RDDFuncResult
                {
                    match closure.downcast_ref::<Self>() {
                        Some(_closure) => {
                             match args.downcast_ref::<( $($argt,)* )>() {
                                Some(args) => {
                                    let &( $(ref $farg,)* ) = args;
                                    let ( $(ref $enclosed,)* ) = ( $(_closure.$enclosed,)* );
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
                fn decode(bytes: &Vec<u8>) -> Box<Any>{
                    let closure: Self = ::bifrost::utils::bincode::deserialize(bytes);
                    Box::new(closure)
                }
                fn boxed_clone(closure: &Box<Any>) -> Box<Any> {
                    match closure.downcast_ref::<Self>() {
                        Some(closure) => {
                            box closure.clone()
                        },
                        None => {
                            panic!(format!("closure is not for the rdd function {:?}", closure));
                        }
                    }
                }
            }
        )*
    };
}