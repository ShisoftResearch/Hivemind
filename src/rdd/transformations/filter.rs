use rdd::{RDD, Partition, RDDID};
use rdd::funcs::RDDFunc;
use std::marker::PhantomData;

pub struct FilterRDD{
    id: RDDID,
    closure: Box<RDDFunc>,
}

impl RDD for FilterRDD {
    fn compute <I, O> (
        &self,
        iter: Box<Iterator<Item = I>>,
        partition: &Partition
    ) -> Box<Iterator<Item = O>>
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
    fn get_dependencies(&self) -> &Vec<&Box<RDD>> {
        unimplemented!()
    }
    fn id(&self) -> RDDID { self.id }
}

impl FilterRDD {
    pub fn new<F>(func: F) -> FilterRDD
        where F: RDDFunc
    {
        FilterRDD {
            id: RDDID::rand(),
            closure: func
        }
    }
}