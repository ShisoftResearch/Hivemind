// this partitioner use consistent hashing from bifrost

use super::Partitioner;
use rdd::Partition;
use bifrost::conshash::ConsistentHashing;
use std::collections::{HashMap, BTreeSet};
use std::sync::Arc;

use bifrost_hasher::hash_bytes;

pub struct ConsistentHashingPartitioner {
    partitions: usize,
    server_map: HashMap<u64, Vec<usize>>, // map server id to partition
    cons_hash: Arc<ConsistentHashing>
}

impl Partitioner for ConsistentHashingPartitioner {
    fn num_partitions(&self) -> usize {
        self.partitions
    }

    fn get_partition(&self, key: &Vec<u8>) -> usize {
        let hash = hash_bytes(key);
        let server_id = self.cons_hash.get_server_id(hash).unwrap_or(0);
        match self.server_map.get(&server_id) {
            Some(l) => l
                .get(server_id as usize % l.len())
                .cloned()
                .unwrap(),
            None => 0
        }
    }
}

impl ConsistentHashingPartitioner {
    pub fn new(partitions: &Vec<Partition>, cons_hash: &Arc<ConsistentHashing>) -> Self {
        let mut server_map = HashMap::new();
        for p in partitions {
            server_map
                .entry(p.server)
                .or_insert_with(|| BTreeSet::new())
                .insert(p.index);
        }
        ConsistentHashingPartitioner {
            server_map: server_map
                .into_iter()
                .map(|(k, v)| {
                    let mut parts: Vec<usize> = Vec::new();
                    parts = v.into_iter().collect();
                    (k, parts)
                })
                .collect(),
            partitions: partitions.len(),
            cons_hash: cons_hash.clone()
        }
    }
}
