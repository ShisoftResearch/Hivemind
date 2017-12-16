// this partitioner use consistent hashing from bifrost

use super::Partitioner;
use rdd::Partition;
use bifrost::conshash::ConsistentHashing;
use std::collections::BTreeMap;
use std::sync::Arc;

use bifrost_hasher::hash_bytes;

pub struct ConsistentHashingPartitioner {
    partitions: usize,
    server_map: BTreeMap<u64, usize>, // map server id to partition
    cons_hash: Arc<ConsistentHashing>
}

impl Partitioner for ConsistentHashingPartitioner {
    fn num_partitions(&self) -> usize {
        self.partitions
    }

    fn get_partition(&self, key: &Vec<u8>) -> usize {
        let hash = hash_bytes(key);
        let server_id = self.cons_hash.get_server_id(hash).unwrap_or(0);
        self.server_map.get(&server_id).cloned().unwrap_or(0)
    }
}

impl ConsistentHashingPartitioner {
    pub fn new(partitions: &Vec<Partition>, cons_hash: &Arc<ConsistentHashing>) -> Self {
        let mut server_map = BTreeMap::new();
        for p in partitions {
            server_map.insert(p.server, p.index);
        }
        ConsistentHashingPartitioner {
            server_map,
            partitions: partitions.len(),
            cons_hash: cons_hash.clone()
        }
    }
}
