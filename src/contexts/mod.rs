use uuid::Uuid;
use super::rdd::RDD;
use std::collections::HashMap;

pub struct TaskContext {
    rdds: HashMap<Uuid, Box<RDD>>
}