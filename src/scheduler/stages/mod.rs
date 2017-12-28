use rdd::{RDD, RDDID};
use std::rc::{Weak, Rc};

pub struct Stage {
    // a stage contains serial of rdds that in narrow dependency
    // the ordering are forward topological sorted
    rdds: Vec<Weak<RDD>>,
    // stages dependency
    deps: Vec<Weak<Stage>>
}

struct Stages {
    stages: Vec<Rc<Stage>>
}