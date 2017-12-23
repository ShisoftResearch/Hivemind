// Block manager for shuffle and data sharing
// Blocks will be saved at where it will be needed. It's id will be registered on a raft state machine
// When shuffling, the block will be available on other nodes by fetching it from the node that generated it
// Block is like a blob container without addressing table. It can be appended and read in sequel, lookup is impossible
// Blocks also support lazy loading and streaming, which means it have a cursor so a request can fetch partial of it.

use std::fs::{File, remove_file};
use std::io;
use std::io::{BufWriter, Seek, SeekFrom};
use std::collections::HashMap;
use std::io::{Write, Read};
use std::sync::Arc;

use bifrost::raft::RaftService;
use bifrost::raft::client::RaftClient;
use bifrost::rpc::DEFAULT_CLIENT_POOL;

use parking_lot::{RwLock, Mutex};
use byteorder::{ByteOrder, LittleEndian};
use futures::{Future, future};

use utils::uuid::UUID;
use server::members::LiveMembers;

use storage::block::registry::client::{SMClient as RegClient};

pub mod client;
pub mod registry;
mod server;

pub struct BlockManager {
    registry_client: RegClient,
    server_mapping_cache: Mutex<HashMap<UUID, u64>>,
    members: Arc<LiveMembers>
}

impl BlockManager {
    pub fn new(
        members: &Arc<LiveMembers>,
        raft: &Arc<RaftService>,
        client: &Arc<RaftClient>,
    ) -> Arc<BlockManager> {
        let registry = registry::BlockRegistry::new();
        let manager = BlockManager {
            members: members.clone(),
            registry_client: RegClient::new(registry::DEFAULT_SERVICE_ID, client),
            server_mapping_cache: Mutex::new(HashMap::new())
        };

        raft.register_state_machine(box registry);
        Arc::new(manager)
    }
    pub fn read(&self, cursor: BlockCursor)
        -> Box<Future<Item = (Vec<Vec<u8>>, BlockCursor), Error = String>>
    {
        match self.get_service(cursor.id) {
            Ok(service) => {
                box service
                    .read(
                        &cursor.id,
                        &cursor.pos,
                        &cursor.limit
                    )
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
                    .map(|(data, pos)| {
                        let mut c = cursor;
                        c.pos = pos;
                        (data, c)
                    })
            },
            Err(e) => box future::err(e)
        }

    }
    pub fn write(&self, id: UUID, items: &Vec<Vec<u8>>)
        -> Box<Future<Item = u64, Error = String>>
    {
        match self.get_service(id) {
            Ok(service) => {
                box service
                    .write(&id, items)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }

    }
    pub fn remove(&self, id: UUID) -> Box<Future<Item = Option<()>, Error = String>> {
        match self.get_service(id) {
            Ok(service) => {
                box service
                    .remove(&id)
                    .map_err(|e| format!("{:?}", e))
                    .map(|r| r.ok())
            },
            Err(e) => box future::err(e)
        }
    }
    fn get_service(&self, id: UUID) -> Result<Arc<server::AsyncServiceClient>, String> {
        let server_id = self.server_mapping_cache
            .lock()
            .entry(id)
            .or_insert_with(||
                self.registry_client
                    .get(&id)
                    .unwrap_or(Ok(Some(0)))
                    .unwrap_or(Some(0))
                    .unwrap_or(0))
            .clone();
        if server_id == 0 {
            return Err("cannot find server in registry".to_string())
        }
        // shortcut is enabled in bifrost, no need to check locality
        let client = match {
            if let Some(member) = self.members.members_guarded().get(&server_id) {
                DEFAULT_CLIENT_POOL.get(&member.address)
            } else {
                return Err("cannot find client".to_string());
            }
        } {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("error on read from block manager {:?}", e);
                error!("{}", &msg);
                return Err(msg);
            }
        };
        Ok(server::AsyncServiceClient::new(server::DEFAULT_SERVICE_ID, &client))
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum ReadLimitBy {
    Size(u64),
    Items(u64)
}

#[derive(Serialize, Deserialize)]
pub struct BlockCursor {
    id: UUID,
    pos: u64,
    limit: ReadLimitBy,
}

impl BlockCursor {
    pub fn new(id: UUID, limit: ReadLimitBy) -> BlockCursor {
        BlockCursor {
            id, pos: 0, limit
        }
    }
}