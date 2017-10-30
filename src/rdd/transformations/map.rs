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

impl<F, I, O> RDD<I, O> for MapRDD<F, I, O>
    where F: RDDFunc<(I), O> + 'static, I: 'static, O: 'static
{
    fn compute(
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition,
        context: &TaskContext
    ) -> Box<Iterator<Item = O>>
    {
        let closure = self.closure.clone();
        Box::new(iter.map(move |x| closure.call((x))))
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