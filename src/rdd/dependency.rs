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
    fn get_parent(&self, partition_id: usize) -> usize;
}

pub struct OneToOne {
    pub rdd: Weak<RDD>
}

impl Narrow for OneToOne {
    fn get_parent(&self, partition_id: usize) -> usize {
        partition_id
    }
}

impl OneToOne {
    pub fn new(rdd: Weak<RDD>) -> Self {
        OneToOne { rdd }
    }
}

pub struct Range {
    rdd: Weak<RDD>,
    in_start: usize,
    out_start: usize,
    length: usize,
}

impl Narrow for Range {
    fn get_parent(&self, partition_id: usize) -> usize {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            partition_id - self.out_start + self.in_start
        } else {
            // since the value will not running cross platform, it should be fine
            ::std::usize::MAX
        }
    }
}

impl Range {
    pub fn new (
        rdd: Weak<RDD>,
        in_start: usize,
        out_start: usize,
        length: usize
    ) -> Range {
        Range {rdd, in_start, out_start, length}
    }
}



pub enum  Dependency {
    Narrow(Box<Narrow>),
    Shuffle(Shuffle)
}