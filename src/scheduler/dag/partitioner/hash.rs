use super::Partitioner;
use rdd::Partition;

use bifrost_hasher::hash_bytes;

pub struct HashPartitioner {
    partitions: usize
}

impl Partitioner for HashPartitioner {
    fn num_partitions(&self) -> usize {
        self.partitions
    }

    fn get_partition(&self, key: &Vec<u8>) -> usize {
        let hash = hash_bytes(key) as usize;
        return hash % self.partitions
    }
}
