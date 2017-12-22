// Resource manager is for tracking and notifying compute nodes, tasks and occupations changes
// It does not do any actual scheduling and node placement (scheduler will do it)
// Scheduler must first register task by calling `register_task` with occupations and their
//  placement. All of those occupation status should be initialized to `Scheduled`
// In the meanwhile, if there is resources available, RM may change occupation status into
//  `Running` and return new status to scheduler on `register_task`, for performance consideration.
// When scheduler received notification from RM about occupation changes, it should try to
//  compete with other nodes through `try_acquire_node_resource` API. If it returns true, then
//  the scheduler is safe to use the resource like sending job to the node. If RM return false,
//  the scheduler should wait for another new resource notification from RM.


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
    def cmd register_node(node: ComputeNode) | RegisterNodeError;
    def cmd register_task(
        task: Task,
        occupations: Vec<Occupation>
    ) -> Vec<Occupation> | RegisterTaskError;

    def cmd degegister_node(node_id: u64) | String;
    def cmd task_ended(task_id: u64, status: TaskStatus) | String;

    def cmd try_acquire_node_resource(
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) -> bool | ChangeOccupationStatusError;

    def cmd release_occupation(
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) -> bool | ChangeOccupationStatusError;

    def qry tasks() -> Vec<Task>;
    def qry nodes() -> Vec<ComputeNode>;

    def sub on_member_changed() -> ComputeNode;
    def sub on_occupation_changed() -> Occupation;
    def sub on_resource_available() -> Occupation;
}

macro_rules! acquire_res {
    ($s: expr, $node: expr, $occ: expr) => {
        $occ.status = OccupationStatus::Running;
        $node.memory_remains -= $occ.memory;
        $node.processors_remains -= $occ.workers;
        $s.notify_occupation_changes($occ);
    };
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
    OccupationTaskNotMatch
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
    memory_remains: u64,
    processors: u32,
    processors_remains: u32,
    node_id: u64,
    online: bool,
    occupations: BTreeMap<u64, Occupation>,
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
    callback: Arc<CallbackTrigger>
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
    fn register_task(&mut self, mut task: Task, mut occupations: Vec<Occupation>)
        -> Result<Vec<Occupation>, RegisterTaskError>
    {
        task.nodes = occupations
            .iter()
            .map(|occ| occ.node_id)
            .collect();
        task.nodes.dedup();
        let mut nodes = self.compute_nodes.write();
        // check all occupation
        for occ in &mut occupations {
            if occ.status != OccupationStatus::Scheduled {
                return Err(RegisterTaskError::OccupationStatusNotScheduled)
            }
            if let Some(mut node) = nodes.get_mut(&occ.node_id) {
                if can_afford_occupation(
                    node.memory_remains, occ.memory,
                    node.processors_remains, occ.workers
                ) {
                    acquire_res!(self, node, occ);
                }
            } else {
                return Err(RegisterTaskError::NodeIdNotFound(occ.node_id))
            }

        }
        let mut running_occupations = Vec::new();
        // append occupations to their nodes and also check it's status
        for occ in occupations {
            if occ.status == OccupationStatus::Running {
                running_occupations.push(occ.clone());
            }
            nodes.get_mut(&occ.node_id).unwrap().occupations.insert(occ.stage_id, occ);
        }
        // append task
        self.tasks.insert(task.id, task);
        return Ok(running_occupations)
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
    fn try_acquire_node_resource(
        &mut self,
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) -> Result<bool, ChangeOccupationStatusError> {
        let mut nodes = self.compute_nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            if let Some(mut occ) = node.occupations.get_mut(&stage_id) {
                if occ.task_id == task_id {
                    if can_afford_occupation(
                        node.memory_remains, occ.memory,
                        node.processors_remains, occ.workers
                    ) {
                        acquire_res!(self, node, occ);
                        return Ok(true)
                    } else {
                        return Ok(false)
                    }
                } else {
                    return Err(ChangeOccupationStatusError::OccupationTaskNotMatch)
                }
            }
        }
        return Err(ChangeOccupationStatusError::CannotFindOccupation);
    }
    fn release_occupation(
        &mut self,
        task_id: u64,
        stage_id: u64,
        node_id: u64
    ) -> Result<bool, ChangeOccupationStatusError> {
        let mut nodes = self.compute_nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            if let Some(mut occ) = node.occupations.get_mut(&stage_id) {
                if occ.task_id == task_id {
                    if occ.status == OccupationStatus::Running {
                        occ.status = OccupationStatus::Released;
                        node.memory_remains += occ.memory;
                        node.processors_remains += occ.workers;
                        self.notify_occupation_changes(occ);
                        return Ok(true)
                    } else {
                        return Ok(false)
                    }
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
        let nodes = self.compute_nodes.read();
        Ok(nodes.values().cloned().collect())
    }
}

impl StateMachineCtl for ResourceManager {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        DEFAULT_SERVICE_ID
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
    pub fn new(raft: Arc<RaftService>, group_id: u64, membership_cb: &SMCallback) {
        let nodes = Arc::new(RwLock::new(BTreeMap::<u64, ComputeNode>::new()));
        let callback = Arc::new(CallbackTrigger {
            sm_callback: SMCallback::new(DEFAULT_SERVICE_ID, raft.clone())
        });
        let manager = ResourceManager{
            compute_nodes: nodes.clone(),
            tasks: BTreeMap::new(),
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
        raft.register_state_machine(box manager);
    }
    fn notify_occupation_changes(&self, occ: &Occupation) {
        self.callback.sm_callback.notify(
            &commands::on_occupation_changed::new(),
            Ok(occ.clone())
        );
        if occ.status == OccupationStatus::Released {
            self.callback.sm_callback.notify(
                &commands::on_resource_available::new(),
                Ok(occ.clone())
            );
        }
    }
}

impl ComputeNode {
    pub fn new<'a>(
        address: &'a str,
        node_id: u64,
        memory: u64,
        processors: u32,
    ) -> ComputeNode {
        ComputeNode {
            address: address.to_string(),
            memory,
            memory_remains: memory,
            processors,
            processors_remains: processors,
            node_id,
            online: true,
            occupations: BTreeMap::new(),
        }
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

fn can_afford_occupation(
    node_mem: u64, occ_mem: u64,
    node_proc: u32, occ_proc: u32
) -> bool {
    node_mem >= occ_mem && node_proc >= occ_proc
}

struct CallbackTrigger {
    sm_callback: SMCallback
}