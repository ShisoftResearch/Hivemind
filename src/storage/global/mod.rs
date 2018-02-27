// Global storage have a replicated backend and it's values are also cached locally.
// It is mutable key-value store, changes will be broadcast to all listening nodes

mod state_machine;

use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;
use std::collections::{HashMap, BTreeMap};
use parking_lot::RwLock;
use std::sync::Arc;
use futures::prelude::*;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum GlobalStorageError {
    StoreNotExisted,
    StoreExisted,
}

type LocalCacheRef = Arc<RwLock<BTreeMap<UUID, Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>>>>;

pub struct GlobalManager {
    sm_client: client::SMClient,
    local_cache: LocalCacheRef,
}

raft_state_machine! {
    def cmd create_store(id: UUID) | GlobalStorageError;
    def cmd invalidate(id: UUID) | GlobalStorageError;

    def cmd set(id: UUID, key: Vec<u8>, val: Option<Vec<u8>>) | GlobalStorageError;
    def cmd swap(id: UUID, key: Vec<u8>, val: Option<Vec<u8>>) -> Option<Vec<u8>> | GlobalStorageError;
    def cmd compare_and_swap(id: UUID, key: Vec<u8>, expect: Option<Vec<u8>>, val: Option<Vec<u8>>) -> Option<Vec<u8>> | GlobalStorageError;

    def qry get(id: UUID, key: Vec<u8>) -> Option<Vec<u8>> | GlobalStorageError;
    def qry dump(id: UUID) -> HashMap<Vec<u8>, Vec<u8>> | GlobalStorageError;

    def sub on_changed(id: UUID) -> (Vec<u8>, Option<Vec<u8>>);
    def sub on_invalidation(id: UUID);
}

impl GlobalManager {
    // to use the store, global store raft service must be initialized
    pub fn new(raft_client: &Arc<RaftClient>) -> GlobalManager {
        let sm_client = client::SMClient::new(state_machine::RAFT_SM_ID, &raft_client);
        GlobalManager {
            sm_client,
            local_cache: Arc::new(RwLock::new(BTreeMap::new()))
        }
    }

    pub fn prepare(&self, id: UUID) -> Result<Result<(), GlobalStorageError>, ExecError> {
        let task_cache = match self.sm_client.dump(&id).wait()? {
            Ok(map) => map,
            Err(GlobalStorageError::StoreNotExisted) => {
                match self.sm_client.create_store(&id).wait() {
                    Ok(Ok(())) => {},
                    Ok(Err(GlobalStorageError::StoreExisted)) => {},
                    Ok(Err(e)) => return Ok(Err(e)),
                    Err(e) => return Err(e)
                };
                HashMap::new()
            },
            Err(e) => return Ok(Err(e))
        };
        let mut local_cache = self.local_cache.write();
        local_cache.insert(id, Arc::new(RwLock::new(task_cache)));
        return Ok(Ok(()))
    }

    // should be called only once, by the task manager
    pub fn invalidate(&self, id: UUID) -> Result<Result<(), GlobalStorageError>, ExecError> {
        let res = self.sm_client.invalidate(&id).wait();
        match res {
            Ok(Ok(())) => {
                let mut cache = self.local_cache.write();
                cache.remove(&id);
            },
            _ => {}
        }
        return res;
    }
    fn update_cache(
        id: UUID,
        cache_lock: LocalCacheRef,
        key: Vec<u8>, value: Option<Vec<u8>>
    ) {
        let task_cache_lock = {
            let mut cache = cache_lock.write();
            cache
                .entry(id)
                .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                .clone()
        };
        let mut task_cache = task_cache_lock.write();
        match value {
            Some(v) => {
                task_cache.insert(key, v);
            },
            None => {
                task_cache.remove(&key);
            }
        };
    }
    pub fn set(&self, id: UUID, key: Vec<u8>, value: Option<Vec<u8>>)
        -> impl Future<Item = Result<(), GlobalStorageError>, Error = ExecError>
    {
        let cache_lock = self.local_cache.clone();
        self.sm_client.set(&id, &key, &value)
            .map(move |res|
                res.map(move |_| {Self::update_cache(id, cache_lock, key, value)}))
    }
    fn swap(&self, id: UUID, key: Vec<u8>, value: Option<Vec<u8>>)
            -> impl Future<Item = Result<Option<Vec<u8>>, GlobalStorageError>, Error = ExecError>
    {
        let cache_lock = self.local_cache.clone();
        self.sm_client.swap(&id, &key, &value)
            .map(move |res|
                res.map(move |old| { Self::update_cache(id, cache_lock, key, value); old }))
    }
    fn compare_and_swap(&self, id: UUID, key: Vec<u8>, expect: &Option<Vec<u8>>, value: &Option<Vec<u8>>)
        -> impl Future<Item = Result<Option<Vec<u8>>, GlobalStorageError>, Error = ExecError>
    {
        let cache_lock = self.local_cache.clone();
        self.sm_client.compare_and_swap(&id, &key, expect, value)
            .map(move |res|
                res.map(move |actual| { Self::update_cache(id, cache_lock, key, actual.clone()); actual }))
    }
    fn get(&self, id: UUID, key: Vec<u8>)
        -> Result<Option<Vec<u8>>, GlobalStorageError>
    {
        let task_cache_lock = {
            let cache = self.local_cache.read();
            match cache.get(&id) {
                Some(c) => c.clone(),
                None => return Err(GlobalStorageError::StoreNotExisted)
            }
        };
        let task_cache = task_cache_lock.read();
        return Ok(task_cache.get(&key).cloned())
    }
}

