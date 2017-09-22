use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::Partition;

pub struct FilterRDD<FN, IN> {
    func_id: u64,
    marker: PhantomData<(FN, IN)>
}

impl<FN, IN> RDD<IN> for FilterRDD<FN, IN> {
    fn compute<P, ITER>(partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition {
        unimplemented!()
    }
}