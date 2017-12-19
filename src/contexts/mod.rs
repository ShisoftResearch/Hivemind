use uuid::Uuid;
use rdd::{RDD, RDDID};
use std::collections::HashMap;
use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::rc::Rc;

pub mod script;

pub struct JobContext {
    rdds: HashMap<RDDID, Rc<RDD>>
}

impl JobContext {
    pub fn new() -> JobContext {
        JobContext {
            rdds: HashMap::new()
        }
    }
}