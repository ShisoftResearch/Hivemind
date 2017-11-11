use rdd::{RDD, RDDTracker, funcs, RDDID, Partition, AnyIter};
use rdd::funcs::{RDDFunc, RDDFuncResult};
use std::any::Any;

pub struct Map {
    closure: Box<Any>,
    func: fn(Box<Any>, Box<Any>) -> RDDFuncResult,
    func_id: u64
}

impl_rdd_tracker!(Map);

impl RDD for Map {
    fn compute (
        &self,
        iter: AnyIter,
        partition: &Partition
    ) -> AnyIter {
        let func = (self.func);
        let iter = iter.map(|d: Box<Any>|
            func(self.closure, d).unwrap_to_any());
        Box::new(iter)
    }
    fn get_dependencies(&self) -> &Vec<&Box<RDD>> {
        unimplemented!()
    }
    fn id(&self) -> RDDID {
        unimplemented!()
    }
}