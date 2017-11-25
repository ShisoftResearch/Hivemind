use bifrost::conshash::ConsistentHashing;
use neb::dovahkiin::types::Map;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

raft_state_machine! {
    def cmd register_node(node: ComputeNode);
    def cmd register_task(
        task: Task,
        occupations: Vec<Occupation>
    ) -> Vec<Occupation>;

    def cmd degegister_node(node_id: u64);
    def cmd task_ended(task_id: u64, status: TaskStatus);

    def qry tasks() -> Vec<Task>;
    def qry nodes() -> Vec<ComputeNode>;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Occupation {
    pub task_id: u64,
    pub stage_id: u64,
    pub workers: u32,
    pub memory: u64,
    pub node_id: u64,
    pub status: OccupationStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Task {
    id: u64,
    name: String,
    status: TaskStatus,
    stages: Vec<u64>,
    meta: Map,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ComputeNode {
    address: String,
    memory: u64,
    processors: u64,
    node_id: u64,
    online: bool,
    occupations: BTreeMap<u64, Occupation>
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Succeed,
    Failed,
    Canceled
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum OccupationStatus {
    Occupied, // resource is ready for use
    Scheduled, // queued
    NotReliable,
    None,
}


pub struct ResourceManager {
    compute_nodes: Mutex<BTreeMap<u64, ComputeNode>>,
    tasks: Mutex<BTreeMap<u64, Task>>
}

