mod funcs;
mod transformations;
use self::funcs::RDDFunc;
use self::transformations::*;
use super::contexts::task::TaskContext;

pub trait Partition {
    fn index() -> u32;
}
pub trait Dependency {
    fn rdd<DD, T>() -> DD where DD: RDD<T>;
}

pub trait RDD<IN> {

    fn compute<P, ITER>(&self, partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition;
    fn get_partitions<P>(&self) -> Vec<P> where P: Partition;
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency;

    fn map<FN, OUT>(&self, func: FN) -> MapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
    fn filter<FN>(&self, func: FN) -> FilterRDD<FN, IN> where FN: RDDFunc<IN, bool> {
        unimplemented!()
    }
    fn flat_map<FN, OUT>(&self, func: FN) -> FlatMapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
    fn map_partitions<FN, OUT>(&self, func: FN) -> MapPartitionsRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
}
