use rdd::{RDD, Partition, Dependency};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use std::iter::Map;

#[derive(Serialize, Deserialize)]
pub struct MapRDD<F, I, O> where F: RDDFunc<(I), O> {
    closure: F,
    marker: PhantomData<(I, O)>
}

impl<F, I, O> RDD<I, O> for MapRDD<F, I, O> where F: RDDFunc<(I), O> {
    fn compute<P, OI>(&self, partition: P, context: &TaskContext) -> OI
        where OI: Iterator<Item = O>, P: Partition {
        partition
            .iter()
            .map(|x: I| self.closure.call((x)))
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