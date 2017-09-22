use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::{Partition, Dependency};

pub struct FilterRDD<FN, IN> {
    func_id: u64,
    marker: PhantomData<(FN, IN)>
}

impl<FN, IN> RDD<IN> for FilterRDD<FN, IN> {
    fn compute<P, ITER>(&self, partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition {
        unimplemented!()
    }
    fn get_partitions<P>(&self) -> Vec<P> where P: Partition {
        unimplemented!()
    }
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency {
        unimplemented!()
    }
}