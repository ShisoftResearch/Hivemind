use self::funcs::RDDFunc;
use super::contexts::TaskContext;
use std::any::{Any, TypeId};
use uuid::Uuid;
#[macro_use]
pub mod macros;
pub mod funcs;
pub mod script;
pub mod transformers;

pub type AnyIter = Box<Iterator<Item = Box<Any + 'static>> + 'static>;

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Hash,
    Copy, Clone,
    Serialize, Deserialize
)]
pub struct RDDID {
    bytes: [u8; 16],
}

impl RDDID {
    pub fn rand() -> RDDID {
        let uuid = Uuid::new_v4();
        RDDID {
            bytes: *(uuid.as_bytes())
        }
    }
}

pub struct Partition {
    index: u32
}

pub trait RDD: Any {
    fn compute(
        &self,
        iter: AnyIter,
        partition: &Partition,
    ) -> AnyIter;
    fn get_dependencies(&self) -> &Vec<&Box<RDD>>;
    fn id(&self) -> RDDID;
//    fn map(&self, func: Box<RDDFunc>) -> MapRDD {
//        MapRDD::new(func)
//    }
//    fn filter(&self, func: Box<RDDFunc>) -> FilterRDD {
//        FilterRDD::new(func)
//    }
//    fn flat_map<F>(&self, func: F) -> FlatMapRDD<F, I, O> where F: RDDFunc<I, O> {
//        unimplemented!()
//    }
//    fn map_partitions<F>(&self, func: F) -> MapPartitionsRDD<F, I, O> where F: RDDFunc<I, O> {
//        unimplemented!()
//    }
}

pub trait RDDTracker {
    fn trans_id() -> u64;
}

pub struct RDDC {
    raw_bytes: Option<Vec<u8>>
}

