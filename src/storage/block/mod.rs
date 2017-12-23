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

use parking_lot::{RwLock, Mutex};
use byteorder::{ByteOrder, LittleEndian};

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
    pub fn read(&self, id: UUID, limit: &ReadLimitBy) -> Option<Vec<Vec<u8>>> {
        let server_id = self.server_mapping_cache
            .lock()
            .entry(id)
            .or_insert_with(||
                self.registry_client
                    .get(&id)
                    .unwrap()
                    .unwrap()
                    .unwrap_or(0))
            .clone();
        // shortcut is enabled in bifrost, no need to check locality
        // let client =
        // let server_addr = self.resource_manager.
        unimplemented!()
    }
}

#[derive(Serialize, Deserialize)]
pub enum ReadLimitBy {
    Size(u64),
    Items(u64)
}