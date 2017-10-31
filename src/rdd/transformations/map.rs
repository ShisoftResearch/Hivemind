use rdd::{RDD, Partition, Dependency, RDDID};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone)]
pub struct MapRDD<F, I, O> {
    id: RDDID,
    closure: F,
    marker: PhantomData<(I, O)>
}

impl<F, I, O> RDD<I, O> for MapRDD<F, I, O>
    where F: RDDFunc<(I), O> + 'static + Clone,
          I: 'static + Clone,
          O: 'static + Clone
{
    fn compute(
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition
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
    fn id(&self) -> RDDID { self.id }
}

impl <F, I, O> MapRDD <F, I, O> {
    pub fn new(func: F) -> MapRDD<F, I ,O> {
        MapRDD {
            id: RDDID::rand(),
            closure: func,
            marker: PhantomData
        }
    }
}