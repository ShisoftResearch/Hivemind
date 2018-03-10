use storage::block::{BlockManager, BlockCursor, ReadLimitBy};
use futures::prelude::*;
use utils::uuid::UUID;
use parking_lot::Mutex;
use std::sync::Arc;
use bifrost::utils::bincode;
use serde::de::DeserializeOwned;

type BuffFut<T> = Box<Future<Item = (Vec<T>, BlockCursor), Error = String>>;

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

impl <T> Stream for BlockStorage<T> where T: DeserializeOwned + 'static {
    type Item = T;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let iter_buf = self.buffer.next();
        if let Some(item) = iter_buf {
            // item buffered and exists
            return Ok(Async::Ready(Some(item)))
        } else {
            // buffer is empty
            let mut fut_ready = false;
            if let Some(ref mut fut) = self.fut {
                // future exists, poll from it
                match fut.poll() {
                    Ok(Async::Ready((items, cursor))) => {
                        if items.len() < 1 {
                            return Ok(Async::Ready(None))
                        }
                        self.cursor.pos = cursor.pos;
                        self.buffer = box items.into_iter();
                        fut_ready = true;
                    },
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => return Err(e)
                }
            }
            if fut_ready {
                self.fut = None;
                return self.poll()
            }
            // No future and iterator is empty, go get one
            let new_fut = self.manager
                .read(self.server_id, self.cursor.clone())
                .map(|(ds, cursor)| {
                    (ds.iter().map(|d| bincode::deserialize(d)).collect(), cursor)
                });
            self.fut = Some(box new_fut);
            return Ok(Async::NotReady)
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockStorageProperty {
    pub id: UUID,
    pub task: UUID,
    pub members: Vec<u64>,
    pub server_id: u64
}