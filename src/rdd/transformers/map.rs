use std::any::Any;
use std::rc::{Rc, Weak};
use rdd::{RDD, RDDTracker, funcs, RDDID, Partition, AnyIter};
use rdd::funcs::{RDDFunc, RDDFuncResult, REGISTRY as FuncREG};
use rdd::transformers::{Registry, REGISTRY};
use scheduler::dag::partitioner::Partitioner;

pub struct Map {
    closure: Box<Any>,
    func: fn(&Box<Any>, &Box<Any>) -> RDDFuncResult,
    clone: fn(&Box<Any>) -> Box<Any>,
}

impl_rdd_trans_tracker!{
    Map (func_id: u64, closure_data: Vec<u8>) {
        let reg_func = FuncREG.get(*func_id).ok_or("cannot find rdd function")?;
        let closure = (reg_func.decode)(closure_data);
        let func = reg_func.func;
        let clone = reg_func.clone;
        Ok(Map{  closure, func, clone })
    }
}

impl RDD for Map {
    fn compute (
        &self,
        iter: AnyIter,
        partition: &Weak<Partition>
    ) -> AnyIter {
        let func = (self.func);
        let clone_closure = (self.clone);
        let closure = clone_closure(&self.closure);
        let iter = iter.map(move |d: Box<Any>|
            func(&closure, &d).unwrap_to_any());
        Box::new(iter)
    }
    fn get_dependencies(&self) -> &Vec<Weak<RDD>> {
        unimplemented!()
    }
    fn get_partitioner(&self) -> &Weak<Partitioner> {
        unimplemented!()
    }
    fn id(&self) -> RDDID {
        unimplemented!()
    }
}