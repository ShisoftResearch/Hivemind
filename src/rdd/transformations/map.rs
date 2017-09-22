use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::Partition;

pub struct MapRDD<FN, IN, OUT> {
    func_id: u64,
    marker: PhantomData<(FN, IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for MapRDD<FN, IN, OUT> {
    fn compute<P, ITER>(&self, partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition {
        unimplemented!()
    }
    fn get_partitions<P>(&self) -> Vec<P> where P: Partition {
        unimplemented!()
    }
}