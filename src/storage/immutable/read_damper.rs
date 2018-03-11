use std::sync::Arc;
use std::collections::{BTreeSet, BTreeMap};
use utils::uuid::UUID;
use futures::prelude::*;
use bifrost::utils::async_locks::{Mutex, MutexGuard};
use parking_lot::{Mutex as PlMutex};

// damp read operations to prevent parallel clone the same remote block
#[derive(Clone)]
pub struct CloneDamperManager {
    inner: Arc<CloneDamperManagerInner>
}

pub struct CloneDamperManagerInner {
    blocks: Arc<Mutex<BTreeMap<UUID, Mutex<()>>>>
}

pub struct Cloning {
    id: UUID,
    mgr_inner: Arc<CloneDamperManagerInner>,
    damper: MutexGuard<()>
}

impl Cloning {
    pub fn new(id: UUID, blocks: &Arc<CloneDamperManagerInner>, damper: MutexGuard<()>) -> Cloning {
        Cloning {
            id, damper,
            mgr_inner: blocks.clone()
        }
    }
}

impl Drop for Cloning {
    #[inline]
    fn drop(&mut self) {
        self.mgr_inner.blocks.lock().remove(&self.id);
    }
}

impl CloneDamperManager {
    pub fn new() -> CloneDamperManager {
        CloneDamperManager {
            inner: Arc::new(CloneDamperManagerInner {
                blocks: Arc::new(Mutex::new(BTreeMap::new()))
            })
        }
    }

    pub fn damp(&self, id: UUID) -> impl Future<Item = Option<Cloning>, Error = ()> {
        let inner = self.inner.clone();
        async_block! {
            let mut blocks = await!(inner.blocks.lock_async())?;
            if blocks.contains_key(&id) {
                await!(blocks.get(&id).unwrap().lock_async());
                return Ok(None);
            }
            let mutex = Mutex::new(());
            let guard = mutex.lock();
            blocks.insert(id, mutex);
            Ok(Some(Cloning::new(id, &inner, guard)))
        }
    }
}