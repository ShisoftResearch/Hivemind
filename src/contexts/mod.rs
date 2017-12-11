use uuid::Uuid;
use super::rdd::{RDD, RDDID};
use std::collections::BTreeMap;
use std::any::{Any, TypeId};
use std::cell::RefCell;

pub mod script;

// #[derive(Serialize, Deserialize, Clone)]
pub struct JobContext {
    rdds: BTreeMap<RDDID, Box<RDD>>
}

impl JobContext {
    pub fn new() -> JobContext {
        JobContext {
            rdds: BTreeMap::new()
        }
    }
}