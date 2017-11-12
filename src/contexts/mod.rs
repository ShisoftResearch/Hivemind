use uuid::Uuid;
use super::rdd::{RDD, RDDID};
use std::collections::BTreeMap;
use std::any::{Any, TypeId};
use std::cell::RefCell;

pub mod script;

// #[derive(Serialize, Deserialize, Clone)]
pub struct TaskContext {
    rdds: BTreeMap<RDDID, Box<RDD>>
}

impl TaskContext {
    pub fn new() -> TaskContext {
        TaskContext {
            rdds: BTreeMap::new()
        }
    }
}