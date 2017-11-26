use bifrost::conshash::ConsistentHashing;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::{
    SMCallback, NotifyError};
use bifrost::utils::bincode;
use neb::dovahkiin::types::Map;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::Mutex;

raft_state_machine! {
    def cmd register_node(node: ComputeNode) | String;
    def cmd register_task(
        task: Task,
        occupations: Vec<Occupation>
    ) -> Vec<Occupation> | String;

    def cmd degegister_node(node_id: u64) | String;
    def cmd task_ended(task_id: u64, status: TaskStatus) | String;
    def cmd occupation_ended(
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) | String; // this will trigger on_scheduled_occupied

    def qry tasks() -> Vec<Task>;
    def qry nodes() -> Vec<ComputeNode>;

    def sub on_scheduled_occupied() -> Vec<Occupation>;
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
    Released,
    NotReliable,
    None,
}


pub struct ResourceManager {
    compute_nodes: BTreeMap<u64, ComputeNode>,
    tasks: BTreeMap<u64, Task>,
    callback: SMCallback,
    sm_id: u64
}

impl StateMachineCmds for ResourceManager {
    fn register_node(&mut self, node: ComputeNode) -> Result<(), String> {
        unimplemented!()
    }
    fn register_task(&mut self, task: Task, occupations: Vec<Occupation>)
        -> Result<Vec<Occupation>, String>
    {
        unimplemented!()
    }
    fn degegister_node(&mut self, node_id: u64) -> Result<(), String> {
        unimplemented!()
    }
    fn task_ended(&mut self, task_id: u64, status: TaskStatus) -> Result<(), String> {
        unimplemented!()
    }
    fn occupation_ended(
        &mut self,
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) -> Result<(), String> {
        unimplemented!()
    }
    fn tasks(&self) -> Result<Vec<Task>, ()> {
        unimplemented!()
    }
    fn nodes(&self) -> Result<Vec<ComputeNode>, ()> {
        unimplemented!()
    }
}

impl StateMachineCtl for ResourceManager {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        return self.id();
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        // data from resource manager will be
        // generated in runtime only
        None
    }
    fn recover(&mut self, data: Vec<u8>) {}
}