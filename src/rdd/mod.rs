mod funcs;
mod transformations;
use self::funcs::RDDFunc;
use self::transformations::*;
use super::contexts::task::TaskContext;
use serde::{Deserialize, Serialize};

pub trait Partition: Serialize {
    fn index() -> u32;
    fn iter<T, OI>() -> OI where OI: Iterator<Item = T>;
}
pub trait Dependency: Serialize {
    fn rdd<DD, I, O>() -> DD where DD: RDD<I, O>;
}

pub trait RDD<I, O>: Serialize {

    fn compute<P, OI>(&self, partition: P, context: &TaskContext) -> OI
        where OI: Iterator<Item = O>, P: Partition;
    fn get_partitions<P>(&self) -> Vec<P> where P: Partition;
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency;
    fn id(&self) -> u64;

    fn map<F>(&self, func: F) -> MapRDD<F, I, O> where F: RDDFunc<I, O> {
        unimplemented!()
    }
    fn filter<F>(&self, func: F) -> FilterRDD<F, I> where F: RDDFunc<I, bool> {
        unimplemented!()
    }
    fn flat_map<F>(&self, func: F) -> FlatMapRDD<F, I, O> where F: RDDFunc<I, O> {
        unimplemented!()
    }
    fn map_partitions<F>(&self, func: F) -> MapPartitionsRDD<F, I, O> where F: RDDFunc<I, O> {
        unimplemented!()
    }
}
