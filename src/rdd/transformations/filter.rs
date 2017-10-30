use rdd::{RDD, Partition, Dependency};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use serde::Serialize;
use contexts::task::TaskContext;

#[derive(Serialize, Deserialize, Clone)]
pub struct FilterRDD<F, I> {
    closure: F,
    marker: PhantomData<I>
}

impl<F, I> RDD<I, I> for FilterRDD<F, I>
    where F: RDDFunc<(I), bool> + Clone + 'static,
          I: Serialize + Clone + 'static
{
    fn compute (
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition,
        context: &TaskContext
    ) -> Box<Iterator<Item = I>>
    {
        let closure = self.closure.clone();
        let p = move |x: &I| {
            closure.call(x.clone()) // TODO: noclone
        };
        let out = iter.filter(p);
        Box::new(out)
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