use std::any::Any;
use std::rc::{Rc, Weak};
use std::borrow::Borrow;
use rdd::{RDD, RDDTracker, funcs, RDDID, Partition, AnyIter};
use rdd::funcs::{RDDFunc, RDDFuncResult, REGISTRY as FuncREG, to_any};
use rdd::transformers::{Registry, REGISTRY};
use partitioner::Partitioner;

pub struct MapPartitions {
    closure: Box<Any>,
    func: fn(&Box<Any>, &Box<Any>) -> RDDFuncResult,
    clone: fn(&Box<Any>) -> Box<Any>,
}

impl_rdd_trans_tracker!{
    MapPartitions (func_id: u64, closure_data: Vec<u8>) {
        let reg_func = FuncREG.get(*func_id).ok_or("cannot find rdd function")?;
        let closure = (reg_func.decode)(closure_data);
        let func = reg_func.func;
        let clone = reg_func.clone;
        Ok(MapPartitions{  closure, func, clone })
    }
}

impl RDD for MapPartitions {
    fn compute (
        &self,
        iter: AnyIter,
        partition: &Weak<Partition>
    ) -> AnyIter {
        let func = (self.func);
        let clone_closure = (self.clone);
        let closure = clone_closure(&self.closure);
        let iter_: &Box<Any> = &to_any(iter);
        func(&closure, iter_).inner().unwrap()
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