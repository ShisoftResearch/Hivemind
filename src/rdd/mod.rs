mod funcs;
mod transformations;
use self::funcs::RDDFunc;
use self::transformations::*;
use super::contexts::TaskContext;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Partition {
    index: u32
}
pub trait Dependency: Serialize {
    fn rdd<DD, I, O>() -> DD where DD: RDD<I, O>;
}

pub trait RDD<I, O>: Serialize + Clone {
    fn compute(
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition,
    ) -> Box<Iterator<Item = O>>;
    fn get_partitions(&self) -> &Vec<Partition>;
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency;
    fn id(&self) -> u64;
    fn set_id(&mut self, id: u64);
    fn map<F>(&self, func: F) -> MapRDD<F, I, O> {
        MapRDD::new(func)
    }
    fn filter<F>(&self, func: F) -> FilterRDD<F, I> {
        FilterRDD::new(func)
    }
//    fn flat_map<F>(&self, func: F) -> FlatMapRDD<F, I, O> where F: RDDFunc<I, O> {
//        unimplemented!()
//    }
//    fn map_partitions<F>(&self, func: F) -> MapPartitionsRDD<F, I, O> where F: RDDFunc<I, O> {
//        unimplemented!()
//    }
}
