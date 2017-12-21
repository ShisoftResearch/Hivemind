// Block manager for shuffle and data sharing
// Blocks will be saved at where it will be needed. It's id will be registered on a raft state machine
// When shuffling, the block will be available on other nodes by fetching it from the node that generated it
// Block is like a blob container without addressing table. It can be appended and read in sequel, lookup is impossible
// Blocks also support lazy loading and streaming, which means it have a cursor so a request can fetch partial of it.

use std::fs::File;
use std::io::BufWriter;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;

pub mod client;

pub struct BlockManager {
    owned_blocks: RwLock<HashMap<Uuid, Arc<RwLock<LocalOwnedBlock>>>>
}

pub struct LocalOwnedBlock {
    id: Uuid,
    buffer: Vec<u8>,
    buffer_pos: u64,
    local_file: BufWriter<File>
}

