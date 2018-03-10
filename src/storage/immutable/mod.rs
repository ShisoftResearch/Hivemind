pub mod state_machine;

use std::sync::Arc;
use std::collections::{BTreeSet, BTreeMap};
use super::block::{BlockManager, BlockCursor};
use bifrost::utils::async_locks::{RwLock};
use bifrost::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;
use futures::prelude::*;

type LocalOwnedBlocks = Arc<RwLock<BTreeMap<UUID, Arc<RwLock<BTreeSet<UUID>>>>>>;
const BLOCK_COPY_BUFFER: u64 = 50;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum ImmutableStorageRegistryError {
    RegistryNotExisted,
    RegistryExisted,
    ItemNotExisted
}

pub struct ImmutableManager {
    block_manager: Arc<BlockManager>,
    registry_client: Arc<state_machine::client::SMClient>,
    local_owned_blocks: LocalOwnedBlocks,
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

    fn clone_block(&self, starting_cursor: &BlockCursor)
        -> Box<Future<Item = (), Error = String>>
    {
        let mut cursor = starting_cursor.clone();
        cursor.pos = 0;
        unimplemented!()
    }

    pub fn read(&self, cursor: BlockCursor)
        -> Box<Future<Item = (Vec<Vec<u8>>, BlockCursor), Error = String>>
    {
        // the optimal way is to stream remote block contents to local when user requested
        // but tracking block integrity is a tedious work
        // so the solution is to copy the whole block to local if it does not present
        unimplemented!()
    }


    pub fn write(&self, task: UUID, id: UUID, items: Vec<Vec<u8>>)
        -> impl Future<Item = Vec<u64>, Error = String>
    {
        let block_manager = self.block_manager.clone();
        let server_id = self.server_id;
        self.ensure_registed(task, id)
            .and_then(move |_| block_manager.write(server_id, &task, &id, &items))
    }

    pub fn get(&self, task: UUID, key: UUID)
        -> impl Future<Item = Option<Vec<u8>>, Error = String>
    {
        let block_manager = self.block_manager.clone();
        let reg_client = self.registry_client.clone();
        let server_id = self.server_id;
        let local_owned_blocks = self.local_owned_blocks.clone();
        let reg_client = self.registry_client.clone();
        async_block! {
            let local_cache = await!(block_manager.get(server_id, &task, &task, &key))?;
            if local_cache.is_some() {
                return Ok(local_cache);
            } else {
                let servers = await!(reg_client.get_location(&task, &key))
                    .map_err(|e| format!("Registry exec error {:?}", e))
                    .and_then(|r| r.map_err(|e| format!("Get server locations from registry error {:?}", e)))?;
                match servers {
                    Some(server_ids) => {
                        for remote_server_id in server_ids {
                            if let Ok(remote) = await!(block_manager.get(remote_server_id, &task, &task, &key)) {
                                if let Some(remote_value) = remote {
                                    await!(block_manager.set(server_id, &task, &task, &key, &remote_value))?;
                                    await!(ensure_registed(reg_client, local_owned_blocks, server_id, task, key))?;
                                    return Ok(Some(remote_value));
                                }
                            }
                        }
                    },
                    None => {}
                }
                return Ok(None);
            }
        }
    }

    pub fn set(&self, task: UUID, key: UUID, value: Vec<u8>)
        -> impl Future<Item =  (), Error = String>
    {
        let block_manager = self.block_manager.clone();
        let server_id = self.server_id;
        self.ensure_registed(task, key) // use key here, so key must be unique in one task
            .and_then(move |_| block_manager.set(server_id, &task, &task, &key, &value))
    }

    pub fn ensure_registed(&self, task_id: UUID, key: UUID) -> impl Future<Item = Option<()>, Error = String> {
        let reg_client = self.registry_client.clone();
        let server_id = self.server_id;
        ensure_registed(reg_client, self.local_owned_blocks.clone(), server_id, task_id, key)
    }
}

pub fn ensure_registed(
    reg_client: Arc<state_machine::client::SMClient>,
    local_owned_blocks: LocalOwnedBlocks,
    server_id: u64, task_id: UUID, key: UUID
) -> impl Future<Item = Option<()>, Error = String> {
    local_owned_blocks
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