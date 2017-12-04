// Resource manager is for tracking and notifying compute nodes, tasks and occupations changes
// It does not do any actual

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
use itertools::Itertools;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(HIVEMIND_RESOURCE_MANAGER) as u64;

raft_state_machine! {
    def cmd register_node(node: ComputeNode) | RegisterNodeError;
    def cmd register_task(
        task: Task,
        occupations: Vec<Occupation>
    ) | RegisterTaskError;

    def cmd degegister_node(node_id: u64) | String;
    def cmd task_ended(task_id: u64, status: TaskStatus) | String;
    def cmd change_occupation_status(
        task_id: u64,
        stage_id: u64,
        node_id: u64,
        status: OccupationStatus
    ) | ChangeOccupationStatusError;

    def qry tasks() -> Vec<Task>;
    def qry nodes() -> Vec<ComputeNode>;

    def sub on_member_changed() -> ComputeNode;
    def sub on_occupation_changed() -> Occupation;
}

#[derive(Serialize, Deserialize)]
pub enum RegisterNodeError {
    NodeAlreadyExisted
}

#[derive(Serialize, Deserialize)]
pub enum RegisterTaskError {
    NodeIdNotFound(u64),
    OccupationStatusNotScheduled
}

#[derive(Serialize, Deserialize)]
pub enum ChangeOccupationStatusError {
    CannotFindOccupation,
    OccupationTaskNotMatch,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Occupation {
    pub task_id: u64,
    pub stage_id: u64,
    pub workers: u32,
    pub memory: u64,
    pub node_id: u64,
    pub status: OccupationStatus,
    pub last_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Task {
    id: u64,
    name: String,
    status: TaskStatus,
    stages: Vec<u64>,
    nodes: Vec<u64>,
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OccupationStatus {
    Running,
    Released,
    Scheduled, // default initial status
}


pub struct ResourceManager {
    compute_nodes: Arc<RwLock<BTreeMap<u64, ComputeNode>>>,
    tasks: BTreeMap<u64, Task>,
    callback: Arc<CallbackTrigger>,
    sm_id: u64
}

impl StateMachineCmds for ResourceManager {
    fn register_node(&mut self, node: ComputeNode) -> Result<(), RegisterNodeError> {
        let mut nodes = self.compute_nodes.write();
        if !nodes.contains_key(&node.node_id) {
            nodes.insert(node.node_id, node);
            return Ok(())
        } else {
            return Err(RegisterNodeError::NodeAlreadyExisted)
        }
    }
    fn register_task(&mut self, mut task: Task, occupations: Vec<Occupation>)
        -> Result<(), RegisterTaskError>
    {
        task.nodes = occupations
            .iter()
            .map(|occ| occ.node_id)
            .collect();
        task.nodes.dedup();
        let mut nodes = self.compute_nodes.write();
        // check all occupation nodes are available
        for occ in &occupations {
            if occ.status != OccupationStatus::Scheduled {
                return Err(RegisterTaskError::OccupationStatusNotScheduled)
            }
            if !nodes.contains_key(&occ.node_id) {
                return Err(RegisterTaskError::NodeIdNotFound(occ.node_id))
            }
        }
        // append occupations to their nodes
        for occ in occupations {
            nodes.get_mut(&occ.node_id).unwrap().occupations.insert(occ.stage_id, occ);
        }
        // append task
        self.tasks.insert(task.id, task);
        return Ok(())
    }
    fn degegister_node(&mut self, node_id: u64) -> Result<(), String> {
        let mut nodes = self.compute_nodes.write();
        nodes.remove(&node_id);
        return Ok(());
    }
    fn task_ended(&mut self, task_id: u64, status: TaskStatus) -> Result<(), String> {
        if let Some(task) = self.tasks.get_mut(&task_id) {
            task.status = status;
            Ok(())
        } else {
            Err(format!("Cannot find task with id {}", task_id))
        }
    }
    fn change_occupation_status(
        &mut self,
        task_id: u64,
        stage_id: u64,
        node_id: u64,
        status: OccupationStatus,
    ) -> Result<(), ChangeOccupationStatusError> {
        let mut nodes = self.compute_nodes.write();
        if let Some(mut node) = nodes.get_mut(&node_id) {
            if let Some(mut occ) = node.occupations.get_mut(&stage_id) {
                if occ.task_id == task_id {
                    occ.status = status;
                    self.callback.sm_callback.notify(
                        &commands::on_occupation_changed::new(),
                        Ok(occ.clone())
                    );
                    return Ok(())
                } else {
                    return Err(ChangeOccupationStatusError::OccupationTaskNotMatch)
                }
            }
        }
        return Err(ChangeOccupationStatusError::CannotFindOccupation);
    }
    fn tasks(&self) -> Result<Vec<Task>, ()> {
        Ok(self.tasks.values().cloned().collect())
    }
    fn nodes(&self) -> Result<Vec<ComputeNode>, ()> {
        let mut nodes = self.compute_nodes.read();
        Ok(nodes.values().cloned().collect())
    }
}

impl StateMachineCtl for ResourceManager {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        return self.sm_id;
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
        let callback = Arc::new(CallbackTrigger {
            sm_callback: SMCallback::new(DEFAULT_SERVICE_ID, raft)
        });
        let manager = ResourceManager{
            compute_nodes: nodes.clone(),
            tasks: BTreeMap::new(),
            sm_id: DEFAULT_SERVICE_ID,
            callback: callback.clone(),
        };
        let nr1 = nodes.clone();
        let cb1 = callback.clone();
        membership_cb.internal_subscribe(
            &on_group_member_offline::new(&group_id),
            move |changes| {
                mark_member(false, changes, &nr1, &cb1)
        });
        let nr2 = nodes.clone();
        let cb2 = callback.clone();
        membership_cb.internal_subscribe(
            &on_group_member_online::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(true, changes, &nr2, &cb2)
            });
        let nr3 = nodes.clone();
        let cb3 = callback.clone();
        membership_cb.internal_subscribe(
            &on_group_member_joined::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(true, changes, &nr3, &cb3)
            });
        let nr4 = nodes.clone();
        let cb4 = callback.clone();
        membership_cb.internal_subscribe(
            &on_group_member_left::new(&group_id),
            move |changes: &Result<(ClientMember, u64), ()>| {
                mark_member(false, changes, &nr4, &cb4)
            });
        return manager;
    }
}

fn mark_member(
    online: bool,
    changes: &Result<(ClientMember, u64), ()>,
    nodes: &Arc<RwLock<BTreeMap<u64, ComputeNode>>>,
    callback: &Arc<CallbackTrigger>
) {
    if let &Ok((ref member, _)) = changes {
        let mut nr = nodes.write();
        if let Some(ref mut node) = nr.get_mut(&member.id) {
            node.online = online;
            let node_for_update = node.clone();
            callback.sm_callback.notify(
                &commands::on_member_changed::new(), Ok(node_for_update)
            );
        }
    }
}

struct CallbackTrigger {
    sm_callback: SMCallback
}