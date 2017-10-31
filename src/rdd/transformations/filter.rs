use rdd::{RDD, Partition, Dependency, RDDID};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone)]
pub struct FilterRDD<F, I> {
    id: RDDID,
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
        partition: &Partition
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
    fn id(&self) -> RDDID { self.id }
}

impl <F, I> FilterRDD <F, I> {
    pub fn new(func: F) -> FilterRDD<F, I> {
        FilterRDD {
            id: RDDID::rand(),
            closure: func,
            marker: PhantomData
        }
    }
}