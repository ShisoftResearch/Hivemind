use rdd::{RDDTracker, funcs};
use rdd::RDD;
use std::any::Any;

pub struct Map {
    closure: Box<Any>,
    func_id: u64
}

impl_rdd_tracker!(Map);

//impl RDD for Map {
//    fn compute(
//        &self,
//        iter: Box<Iterator<Item = I>>,
//        partition: &Partition
//    ) -> Box<Iterator<Item = O>> {
//
//    }
//}