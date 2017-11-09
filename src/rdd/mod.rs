mod funcs;
use self::funcs::RDDFunc;
// use self::transformations::*;
use super::contexts::TaskContext;
use std::any::{Any, TypeId};
use uuid::Uuid;

#[derive(Ord, PartialOrd, PartialEq, Eq, Hash)]
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
        iter: Box<Iterator<Item = Any>>,
        partition: &Partition,
    ) -> Box<Iterator<Item = Any>>;
    fn get_partitions(&self) -> &Vec<Partition>;
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


pub struct RDDC {
    raw_bytes: Option<Vec<u8>>
}

