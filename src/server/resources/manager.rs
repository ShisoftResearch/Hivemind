use bifrost::conshash::ConsistentHashing;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Occupation {
    job_id: u64,
    workers: u32,
    memory: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Job {
    data: Vec<u8>,
    decoder_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ComputeNodes {
    memory: u64,
    processors: u64,
    node_id: u64,
    online: bool
}

pub struct ResourceManager {
    occupations: Mutex<BTreeMap<u64, Vec<Occupation>>>,
    compute_nodes: Mutex<BTreeMap<u64, ComputeNodes>>,
    jobs: Mutex<BTreeMap<u64, Job>>
}

