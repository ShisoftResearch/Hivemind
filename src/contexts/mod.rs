use uuid::Uuid;
use super::rdd::{RDD, RDDID};
use std::collections::HashMap;
use std::any::{Any, TypeId};
use std::cell::RefCell;

// #[derive(Serialize, Deserialize, Clone)]
pub struct TaskContext {
    rdds: RefCell<HashMap<RDDID, Box<Any>>>
}

impl TaskContext {
    pub fn new() -> TaskContext {
        TaskContext {
            rdds: RefCell::new(HashMap::new())
        }
    }
//    pub fn new_rdd<I, O, R>(&self, rdd: R) where R: RDD<I, O> + Sized {
//        let mut rdds = self.rdds.borrow_mut();
//        rdds.insert(rdd.id(), Box::new(rdd));
//    }
}