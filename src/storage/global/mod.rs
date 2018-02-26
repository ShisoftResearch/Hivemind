// Global storage have a replicated backend and it's values are also cached locally.
// It is mutable key-value store, changes will be broadcast to all listening nodes

mod state_machine;

use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;
use std::collections::{HashMap, BTreeMap};
use parking_lot::RwLock;
use std::sync::Arc;
use futures::prelude::*;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum GlobalStorageError {
    StoreNotExisted,
    StoreExisted,
}

pub struct GlobalManager {
    sm_client: client::SMClient,
    local_cache: RwLock<BTreeMap<UUID, Arc<HashMap<Vec<u8>, Vec<u8>>>>>,
}

raft_state_machine! {
    def cmd create_store(id: UUID) | GlobalStorageError;
    def cmd invalidate(id: UUID) | GlobalStorageError;

    def cmd set(id: UUID, key: Vec<u8>, val: Option<Vec<u8>>) | GlobalStorageError;
    def cmd swap(id: UUID, key: Vec<u8>, val: Option<Vec<u8>>) -> Option<Vec<u8>> | GlobalStorageError;
    def cmd compare_and_swap(id: UUID, key: Vec<u8>, expect: Option<Vec<u8>>, val: Option<Vec<u8>>) -> Option<Vec<u8>> | GlobalStorageError;

    def qry get(id: UUID, key: Vec<u8>) -> Option<Vec<u8>> | GlobalStorageError;
    def qry dump(id: UUID) -> HashMap<Vec<u8>, Vec<u8>> | GlobalStorageError;

    def sub on_changed(id: UUID) -> (Vec<u8>, Option<Vec<u8>>);
    def sub on_invalidation(id: UUID);
}

impl GlobalManager {
    // to use the store, global store raft service must be initialized
    pub fn new(raft_client: &Arc<RaftClient>) -> GlobalManager {
        let sm_client = client::SMClient::new(state_machine::RAFT_SM_ID, &raft_client);
        GlobalManager {
            sm_client,
            local_cache: RwLock::new(BTreeMap::new())
        }
    }

    pub fn prepare(&self, id: UUID) -> Result<Result<(), GlobalStorageError>, ExecError> {
        let task_cache = match self.sm_client.dump(&id).wait()? {
            Ok(map) => map,
            Err(GlobalStorageError::StoreNotExisted) => {
                self.sm_client.create_store(&id).wait().unwrap().unwrap();
                HashMap::new()
            },
            Err(e) => return Ok(Err(e))
        };
        let mut local_cache = self.local_cache.write();
        local_cache.insert(id, task_cache);
        return Ok(Ok(()))
    }
}

