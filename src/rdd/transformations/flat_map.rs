use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::{Partition, Dependency};

#[derive(Serialize, Deserialize)]
pub struct FlatMapRDD<F, I, O> {
    func_id: u64,
    marker: PhantomData<(F, I, O)>
}

impl<F, I, O> RDD<I, O> for FlatMapRDD<F, I, O> {
    fn compute(
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition,
        context: &TaskContext
    ) -> Box<Iterator<Item = O>> {
        unimplemented!()
    }
    fn get_partitions(&self) -> &Vec<Partition> {
        unimplemented!()
    }
    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency {
        unimplemented!()
    }
    fn id(&self) -> u64 {
        unimplemented!()
    }
}