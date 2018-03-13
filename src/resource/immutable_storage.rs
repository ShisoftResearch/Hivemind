use std::sync::Arc;
use std::marker::PhantomData;
use super::streamable_storage::*;
use storage::immutable::{ImmutableManager};
use storage::block::{BlockCursor, ReadLimitBy};
use serde::de::DeserializeOwned;
use utils::uuid::UUID;
use futures::prelude::*;
use bifrost::utils::bincode;

pub struct ImmutableStorage<T> where T: DeserializeOwned {
    block_id: UUID,
    manager: Arc<ImmutableManager>,
    marker: PhantomData<T>,
    cursor: BlockCursor,
    fut: Option<BuffFut<T>>,
    buffer: Box<Iterator<Item = T>>
}

impl <T> ImmutableStorage<T> where T: DeserializeOwned + 'static {
    pub fn new(manager: &Arc<ImmutableManager>, task: UUID, id: UUID, buff_size: u64) -> ImmutableStorage<T> {
        return ImmutableStorage {
            block_id: id,
            manager: manager.clone(),
            marker: PhantomData,
            cursor: BlockCursor::new(task, id, ReadLimitBy::Items(buff_size)),
            fut: None,
            buffer: box vec![].into_iter()
        }
    }
}

impl <T> BufferedStreamableStorage<T> for ImmutableStorage<T> where T: DeserializeOwned + 'static {

    #[inline]
    fn read_batch(&mut self) -> Box<Future<Item=(Vec<Vec<u8>>, BlockCursor), Error=String>> {
        box self.manager.read(self.cursor.clone())
    }

    impl_stream_sources!();

}

impl_stream!(ImmutableStorage);

#[derive(Serialize, Deserialize, Clone)]
pub struct ImmutableStorageProperty {
    pub task_id: UUID,
    pub id: UUID
}