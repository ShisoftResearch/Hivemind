use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::{Partition, Dependency};

#[derive(Serialize, Deserialize)]
pub struct FlatMapRDD<FN, IN, OUT> {
    func_id: u64,
    marker: PhantomData<(FN, IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for FlatMapRDD<FN, IN, OUT> {
    fn compute<P, ITER>(&self, partition: P, context: &TaskContext) -> ITER where ITER: Iterator, P: Partition {
        unimplemented!()
    }
    fn get_partitions<P>(&self) -> Vec<P> where P: Partition {
        unimplemented!()
    }
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency {
        unimplemented!()
    }
    fn id(&self) -> u64 {
        unimplemented!()
    }
}