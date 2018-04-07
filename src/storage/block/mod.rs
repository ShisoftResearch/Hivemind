// Block manager for shuffle and data sharing
// Blocks will be saved at where it will be needed.
// When shuffling, the block will be available on other nodes by fetching it from the node that generated it
// Block is like a blob container without addressing table. It can be appended and read in sequel.
// Lookup is accomplished by a runtime address map
// Blocks also support lazy loading and streaming, which means it have a cursor so a request can fetch partial of it.

use super::*;
use std::fs::{File, remove_file};
use std::io;
use std::io::{BufWriter, Seek, SeekFrom};
use std::collections::HashMap;
use std::io::{Write, Read};

use bifrost::rpc::DEFAULT_CLIENT_POOL;

use parking_lot::{RwLock, Mutex};
use byteorder::{ByteOrder, LittleEndian};

use utils::uuid::UUID;

pub mod server;

service! {
    rpc read(task: UUID, id: UUID, pos: u64, limit: ReadLimitBy) -> (Vec<Vec<u8>>, u64) | String;
    rpc exists(task: UUID, id: UUID) -> bool | String;
    rpc write(task: UUID, id: UUID, items: Vec<Vec<u8>>) -> Vec<u64> | String;
    rpc remove(task: UUID, id: UUID) | String;

    rpc get(task: UUID, id: UUID, key: UUID) -> Option<Vec<u8>> | String;
    rpc set(task: UUID, id: UUID, key: UUID, value: Vec<u8>) | String;
    rpc unset(task: UUID, id: UUID, key: UUID) -> Option<()> | String;

    rpc remove_task(task: UUID) | String;
    rpc new_task(task: UUID);
}

lazy_static! {
    static ref DEFAULT: RwLock<Option<Arc<BlockManager>>> = RwLock::new(None);
}

pub struct BlockManager {
    server_mapping_cache: Mutex<HashMap<UUID, u64>>,
    members: Arc<LiveMembers>
}

impl BlockManager {
    pub fn new(
        members: &Arc<LiveMembers>
    ) -> Arc<BlockManager> {
        let manager = BlockManager {
            members: members.clone(),
            server_mapping_cache: Mutex::new(HashMap::new())
        };
        let refer = Arc::new(manager);
        let mut def_val = DEFAULT.write();
        *def_val = Some(refer.clone());
        return refer;
    }
    pub fn default() -> Arc<BlockManager> {
        let def = DEFAULT.read();
        return def.clone().unwrap();
    }
    pub fn new_task(&self, server_id: u64, task_id: UUID)
        -> Box<Future<Item = (), Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .new_task(task_id)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r.map_err(|e| unreachable!()))
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn remove_task(&self, server_id: u64, task_id: UUID)
        -> Box<Future<Item = (), Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .remove_task(task_id)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r.map_err(|e| unreachable!()))
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn read(&self, server_id: u64, cursor: BlockCursor)
        -> Box<Future<Item = (Vec<Vec<u8>>, BlockCursor), Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .read(
                        cursor.task,
                        cursor.id,
                        cursor.pos,
                        cursor.limit
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
    pub fn write(&self, server_id: u64, task: UUID, id: UUID, items: Vec<Vec<u8>>)
        -> Box<Future<Item = Vec<u64>, Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .write(task, id, items)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }

    }
    pub fn remove(&self, server_id: u64, task: UUID, id: UUID)
        -> Box<Future<Item = Option<()>, Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .remove(task, id)
                    .map_err(|e| format!("{:?}", e))
                    .map(|r| r.ok())
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn get(&self, server_id: u64, task: UUID, id: UUID, key: UUID)
        -> Box<Future<Item = Option<Vec<u8>>, Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .get(task, id, key)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn set(&self, server_id: u64, task: UUID, id: UUID, key: UUID, value: Vec<u8>)
               -> Box<Future<Item =  (), Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .set(task, id, key, value)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn unset(&self, server_id: u64, task: UUID, id: UUID, key: UUID)
        -> Box<Future<Item = Option<()>, Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .unset(task, id, key)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }
    }
    pub fn exists(&self, server_id: u64, task: UUID, id: UUID)
        -> Box<Future<Item = bool, Error = String>>
    {
        match self.get_service(server_id) {
            Ok(service) => {
                box service
                    .exists(task, id)
                    .map_err(|e| format!("{:?}", e))
                    .and_then(|r| r)
            },
            Err(e) => box future::err(e)
        }
    }
    fn get_service(&self, server_id: u64) -> Result<Arc<AsyncServiceClient>, String> {
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
        Ok(AsyncServiceClient::new(server::BLOCK_OWNER_DEFAULT_SERVICE_ID, &client))
    }
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum ReadLimitBy {
    Size(u64),
    Items(u64)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockCursor {
    pub pos: u64,
    pub task: UUID,
    pub id: UUID,
    pub limit: ReadLimitBy,
}

impl BlockCursor {
    pub fn new(task: UUID, id: UUID, limit: ReadLimitBy) -> BlockCursor {
        BlockCursor {
            task, id, pos: 0, limit
        }
    }
}