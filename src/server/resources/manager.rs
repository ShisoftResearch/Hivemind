use bifrost::conshash::ConsistentHashing;
use bifrost::raft::RaftService;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::{
    SMCallback, NotifyError};
use bifrost::membership::raft::commands::{
    on_group_member_offline, on_group_member_online,
    on_group_member_joined, on_group_member_left};
use bifrost::membership::client::{Member as ClientMember};
use bifrost::utils::bincode;
use neb::dovahkiin::types::Map;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(HIVEMIND_RESOURCE_MANAGER) as u64;

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
    compute_nodes: Arc<RwLock<BTreeMap<u64, ComputeNode>>>,
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
        let nodes = self.compute_nodes.read();
        Some(bincode::serialize(&(&nodes.clone(), &self.tasks)))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let pack:(BTreeMap<u64, ComputeNode>, BTreeMap<u64, Task>) = bincode::deserialize(&data);
        self.compute_nodes = Arc::new(RwLock::new(pack.0));
        self.tasks = pack.1;
    }
}

impl ResourceManager {
    pub fn new(raft: Arc<RaftService>, group_id: u64, membership_cb: &SMCallback) -> ResourceManager {
        let nodes = Arc::new(RwLock::new(BTreeMap::<u64, ComputeNode>::new()));
        let manager = ResourceManager{
            compute_nodes: nodes.clone(),
            tasks: BTreeMap::new(),
            callback: SMCallback::new(DEFAULT_SERVICE_ID, raft),
            sm_id: DEFAULT_SERVICE_ID
        };
        let nr1 = nodes.clone();
        membership_cb.internal_subscribe(
            &on_group_member_offline::new(&group_id),
            move |changes| {
                mark_member(false, changes, &nr1)
        });
        let nr2 = nodes.clone();
        membership_cb.internal_subscribe(
            &on_group_member_online::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(true, changes, &nr2)
            });
        let nr3 = nodes.clone();
        membership_cb.internal_subscribe(
            &on_group_member_joined::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(true, changes, &nr3)
            });
        let nr4 = nodes.clone();
        membership_cb.internal_subscribe(
            &on_group_member_left::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(false, changes, &nr4)
            });
        return manager;
    }
}

fn mark_member(
    online: bool,
    changes: &Result<(ClientMember, u64), ()>,
    nodes: &Arc<RwLock<BTreeMap<u64, ComputeNode>>>
) {
    if let &Ok((ref member, _)) = changes {
        let mut nr = nodes.write();
        if let Some(ref mut node) = nr.get_mut(&member.id) {
            node.online = online;
        }
    }
}