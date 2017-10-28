use rdd::RDD;
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::{Partition, Dependency};

#[derive(Serialize, Deserialize)]
pub struct MapRDD<FN, IN, OUT> where FN: RDDFunc<IN, OUT> {
    closure: FN,
    marker: PhantomData<(IN, OUT)>
}

impl<FN, IN, OUT> RDD<IN> for MapRDD<FN, IN, OUT>  where FN: RDDFunc<IN, OUT> {
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