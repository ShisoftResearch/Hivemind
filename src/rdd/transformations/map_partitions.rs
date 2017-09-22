use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::Partition;

pub struct MapPartitionsRDD<FN, IN, OUT> {
    func_id: u64,
    marker: PhantomData<(FN, IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for MapPartitionsRDD<FN, IN, OUT> {
    fn compute<P, ITER>(partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition {
        unimplemented!()
    }
}