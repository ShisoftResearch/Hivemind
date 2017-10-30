use rdd::RDD;
use std::marker::PhantomData;
use contexts::task::TaskContext;
use super::super::{Partition, Dependency};

#[derive(Serialize, Deserialize)]
pub struct FilterRDD<F, I> {
    func_id: u64,
    marker: PhantomData<(F, I)>
}

//impl<F, I, O> RDD<I, O> for FilterRDD<F, I> {
//    fn compute<II, OI>(&self, iter: II, partition: &Partition, context: &TaskContext) -> OI
//        where OI: Iterator<Item = O>, II: Iterator<Item = I> {
//        unimplemented!()
//    }
//    fn get_partitions(&self) -> &Vec<Partition> {
//        unimplemented!()
//    }
//    fn get_dependencies<DEP>(&self) -> Vec<DEP> where DEP: Dependency {
//        unimplemented!()
//    }
//    fn id(&self) -> u64 {
//        unimplemented!()
//    }
//}