use rdd::{RDD, Partition, RDDID};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;
use uuid::Uuid;

pub struct MapRDD {
    id: RDDID,
    closure: Box<RDDFunc>
}

impl RDD for MapRDD {
    fn compute<I, O>(
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
    fn get_dependencies(&self) -> &Vec<&Box<RDD>> {
        unimplemented!()
    }
    fn id(&self) -> RDDID { self.id }
}

impl MapRDD  {
    pub fn new<F>(func: F) -> MapRDD
        where F: RDDFunc
    {
        MapRDD {
            id: RDDID::rand(),
            closure: Box::new(func),
        }
    }
}