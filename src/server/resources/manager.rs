use bifrost::conshash::ConsistentHashing;
use neb::dovahkiin::types::Map;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

raft_state_machine! {
    def cmd register_node(node: ComputeNode);
    def cmd register_task(task: Task);

    def cmd degegister_node(node_id: u64);
    def cmd task_ended(task_id: u64, status: TaskStatus);

    def cmd acquire(
        occupations: Vec<Occupation>
    ) -> Result<Vec<Occupation>, String>;

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

pub struct ResourceManager {
    compute_nodes: Mutex<BTreeMap<u64, ComputeNode>>,
    tasks: Mutex<BTreeMap<u64, Task>>
}

