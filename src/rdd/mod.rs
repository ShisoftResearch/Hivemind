mod funcs;
mod transformations;
use self::funcs::RDDFunc;
use self::transformations::*;
use super::contexts::task::TaskContext;

pub trait Partition {
    fn index() -> u32;
}

pub trait RDD<IN> {

    fn compute<P, ITER>(partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition;

    fn map<FN, OUT>(func: FN) -> MapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
    fn filter<FN>(func: FN) -> FilterRDD<FN, IN> where FN: RDDFunc<IN, bool> {
        unimplemented!()
    }
    fn flat_map<FN, OUT>(func: FN) -> FlatMapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
    fn map_partitions<FN, OUT>(func: FN) -> MapPartitionsRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
        unimplemented!()
    }
}
