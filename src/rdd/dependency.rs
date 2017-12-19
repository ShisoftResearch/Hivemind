use std::rc::Weak;
use super::RDD;
use scheduler::dag::partitioner::Partitioner;

pub struct Shuffle {
    pub rdd: Weak<RDD>,
    pub partitioner: Weak<Partitioner>
}


// TODO: WIP
impl Shuffle {
    pub fn new(
        rdd: Weak<RDD>,
        partitioner: Weak<Partitioner>,
    ) -> Shuffle {
        Shuffle { rdd, partitioner }
    }
}

pub trait Narrow {
    fn get_parents(&self, partition_id: u64) -> Vec<u64>;
}

pub struct OneToOne {
    pub rdd: Weak<RDD>
}

impl Narrow for OneToOne {
    fn get_parents(&self, partition_id: u64) -> Vec<u64> {
        vec![partition_id]
    }
}

impl OneToOne {
    pub fn new(rdd: Weak<RDD>) -> Self {
        OneToOne { rdd }
    }
}

pub struct Range {
    rdd: Weak<RDD>,
    in_start: u64,
    out_start: u64,
    length: u64,
}

impl Narrow for Range {
    fn get_parents(&self, partition_id: u64) -> Vec<u64> {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            vec![partition_id - self.out_start + self.in_start]
        } else {
            vec![]
        }
    }
}

impl Range {
    pub fn new (
        rdd: Weak<RDD>,
        in_start: u64,
        out_start: u64,
        length: u64
    ) -> Range {
        Range {rdd, in_start, out_start, length}
    }
}



pub enum  Dependency {
    Narrow(Box<Narrow>),
    Shuffle(Shuffle)
}