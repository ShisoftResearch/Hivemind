pub mod state_machine;

use std::sync::Arc;
use std::collections::{BTreeSet, BTreeMap};
use super::block::{BlockManager, BlockCursor};
use bifrost::utils::async_locks::{RwLock};
use bifrost::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;
use futures::prelude::*;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum ImmutableStorageRegistryError {
    RegistryNotExisted,
    RegistryExisted,
    ItemExisted
}

pub struct ImmutableManager {
    block_manager: Arc<BlockManager>,
    registry_client: Arc<state_machine::client::SMClient>,
    local_owned_blocks: Arc<RwLock<BTreeMap<UUID, Arc<RwLock<BTreeSet<UUID>>>>>>,
    server_id: u64
}

impl ImmutableManager {

    pub fn new(block_manager: &Arc<BlockManager>, raft_client: &Arc<RaftClient>, server_id: u64)
        -> Arc<ImmutableManager>
    {
        Arc::new(ImmutableManager {
            block_manager: block_manager.clone(),
            registry_client: Arc::new(state_machine::client::SMClient::new(state_machine::RAFT_SM_ID, &raft_client)),
            local_owned_blocks: Arc::new(RwLock::new(BTreeMap::new())),
            server_id
        })
    }

    pub fn read(&self, cursor: BlockCursor)
        -> Box<Future<Item = (Vec<Vec<u8>>, BlockCursor), Error = String>>
    {
        unreachable!("Will not implemented, use block storage streaming")
    }


    pub fn write(&self, task: UUID, id: UUID, items: &Vec<Vec<u8>>)
        -> Box<Future<Item = Vec<u64>, Error = String>>
    {
        //self.ensure_registed(task, id)
        unimplemented!()
    }

    pub fn remove(&self, task: UUID, id: UUID)
        -> Box<Future<Item = Option<()>, Error = String>>
    {
        unimplemented!()
    }

    pub fn get(&self, task: UUID, key: &UUID)
        -> Box<Future<Item = Option<Vec<u8>>, Error = String>>
    {
        unimplemented!()
    }

    pub fn set(&self, task: UUID, key: &Vec<u8>, value: &UUID)
        -> Box<Future<Item =  (), Error = String>>
    {
        unimplemented!()
    }

    pub fn ensure_registed(&self, task_id: UUID, key: UUID) -> impl Future<Item = Option<()>, Error = String> {
        let reg_client = self.registry_client.clone();
        let server_id = self.server_id;
        self.local_owned_blocks
            .read_async()
            .map_err(|_| "unexpected".to_string())
            .and_then(move |tasks| {
                tasks.get(&task_id)
                    .map(|owned| owned.clone())
                    .ok_or_else(|| "task not found".to_string())
            })
            .and_then(move |local_owned_lock| {
                async_block! {
                    {
                        let owned = await!(local_owned_lock.read_async()).unwrap();
                        if owned.contains(&key) {
                            return Ok(None)
                        }
                    }
                    {
                        let mut owned = await!(local_owned_lock.write_async()).unwrap();
                        owned.insert(key);
                    }
                    {
                        await!(reg_client.set_location(&task_id, &key, &server_id)
                            .map_err(|e| format!("Registry exec error {:?}", e))
                            .and_then(|r| r.map_err(|e| format!("Registry error {:?}", e))))?
                    }
                    return Ok(Some(()))
                }
            })
    }
}