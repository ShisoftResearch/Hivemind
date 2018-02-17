use storage::block::{BlockManager, BlockCursor};
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