use super::streamable_storage::*;
use storage::block::{BlockManager, BlockCursor, ReadLimitBy};
use futures::prelude::*;
use utils::uuid::UUID;
use std::sync::Arc;
use bifrost::utils::bincode;
use serde::de::DeserializeOwned;


pub struct BlockStorage<T> where T: DeserializeOwned {
    server_id: u64,
    block_id: UUID,
    manager: Arc<BlockManager>,
    cursor: BlockCursor,
    fut: Option<BuffFut<T>>,
    buffer: Box<Iterator<Item = T>>
}

impl <T> BlockStorage<T> where T: DeserializeOwned + 'static {
    pub fn new(manager: &Arc<BlockManager>, server_id: u64, task: UUID, id: UUID, buff_size: u64) -> BlockStorage<T> {
        return BlockStorage {
            server_id,
            block_id: id,
            manager: manager.clone(),
            cursor: BlockCursor::new(task, id, ReadLimitBy::Items(buff_size)),
            fut: None,
            buffer: box vec![].into_iter()
        }
    }
}

impl <T> BufferedStreamableStorage<T> for BlockStorage<T> where T: DeserializeOwned + 'static {

    #[inline]
    fn read_batch(&mut self) -> Box<Future<Item=(Vec<Vec<u8>>, BlockCursor), Error=String>> {
        self.manager.read(self.server_id, self.cursor.clone())
    }

    impl_stream_sources!();

}

impl_stream!(BlockStorage);

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockStorageProperty {
    pub id: UUID,
    pub task: UUID,
    pub members: Vec<u64>,
    pub server_id: u64
}