// Partitioner is part of dynamic scheduler. It is allowed to run directly on RDDs (for sampling).

use serde::Serialize;
use rdd::Partition;

pub mod hash;
pub mod range;
pub mod cons_hash;

pub trait Partitioner {
    fn num_partitions(&self) -> usize;
    fn get_partition(&self, key: &Vec<u8>) -> usize;
}