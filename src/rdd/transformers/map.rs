use rdd::{RDD, RDDTracker, funcs, RDDID, Partition, AnyIter};
use rdd::funcs::{RDDFunc, RDDFuncResult, REGISTRY as FuncREG};
use std::any::Any;

pub struct Map {
    closure: Box<Any>,
    func: fn(&Box<Any>, Box<Any>) -> RDDFuncResult,
    clone: fn(&Box<Any>) -> Box<Any>,
}

impl_rdd_tracker!{
    Map (func_id: u64, closure_data: Vec<u8>) {
        let reg_func = FuncREG.get(*func_id);
        // let closure = unsafe { reg_func.decode }
        unimplemented!()
    }
}

impl RDD for Map {
    fn compute (
        &self,
        iter: AnyIter,
        partition: &Partition
    ) -> AnyIter {
        let func = (self.func);
        let clone_closure = (self.clone);
        let closure = clone_closure(&self.closure);
        let iter = iter.map(move |d: Box<Any>|
            func(&closure, d).unwrap_to_any());
        Box::new(iter)
    }
    fn get_dependencies(&self) -> &Vec<&Box<RDD>> {
        unimplemented!()
    }
    fn id(&self) -> RDDID {
        unimplemented!()
    }
}