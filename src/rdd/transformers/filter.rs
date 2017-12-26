use rdd::{RDD, RDDTracker, funcs, RDDID, Partition, AnyIter};
use rdd::funcs::{RDDFunc, RDDFuncResult, REGISTRY as FuncREG};
use rdd::transformers::{Registry, REGISTRY};
use partitioner::Partitioner;
use std::any::Any;
use std::rc::{Rc, Weak};

pub struct Filter {
    closure: Box<Any>,
    func: fn(&Box<Any>, &Box<Any>) -> RDDFuncResult,
    clone: fn(&Box<Any>) -> Box<Any>,
}

impl_rdd_trans_tracker!{
    Filter (func_id: u64, closure_data: Vec<u8>) {
        let reg_func = FuncREG.get(*func_id).ok_or("cannot find rdd function")?;
        let closure = (reg_func.decode)(closure_data);
        let func = reg_func.func;
        let clone = reg_func.clone;
        Ok(Filter{  closure, func, clone })
    }
}

impl RDD for Filter {
    fn compute (
        &self,
        iter: AnyIter,
        partition: &Weak<Partition>
    ) -> AnyIter {
        let func = (self.func);
        let clone_closure = (self.clone);
        let closure = clone_closure(&self.closure);
        let iter =
            iter.filter(move |d: &Box<Any>| -> bool {
                func(&closure, d).inner().unwrap()
            });
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