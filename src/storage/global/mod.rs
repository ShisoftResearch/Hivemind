// Global storage have a replicated backend and it's values are also cached locally.
// It is mutable key-value store, changes will be broadcast to all listening nodes

mod state_machine;

use bifrost::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;
use std::collections::{HashMap, BTreeMap, HashSet};
use parking_lot::RwLock;
use std::sync::Arc;
use futures::prelude::*;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum GlobalStorageError {
    StoreNotExisted,
    StoreExisted,
    SubscriptionError
}

type LocalCacheRef = Arc<RwLock<BTreeMap<UUID, Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>>>>;

pub struct GlobalManager {
    sm_client: client::SMClient,
    local_cache: LocalCacheRef,
    sub_receipts: Arc<RwLock<BTreeMap<UUID, SubscriptionReceipt>>>,
    raft_client: Arc<RaftClient>
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
}

impl GlobalManager {
    // to use the store, global store raft service must be initialized
    pub fn new(raft_client: &Arc<RaftClient>) -> GlobalManager {
        let sm_client = client::SMClient::new(state_machine::RAFT_SM_ID, &raft_client);
        GlobalManager {
            sm_client,
            local_cache: Arc::new(RwLock::new(BTreeMap::new())),
            sub_receipts: Arc::new(RwLock::new(BTreeMap::new())),
            raft_client: raft_client.clone()
        }
    }
    pub fn prepare(&self, id: UUID) -> Result<Result<(), GlobalStorageError>, ExecError> {
        let mut local_cache = self.local_cache.write();
        if local_cache.contains_key(&id) {
            return Ok(Err(GlobalStorageError::StoreNotExisted))
        }
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
        let task_cache = Arc::new(RwLock::new(task_cache));
        let task_cache_clone = task_cache.clone();
        match self.sm_client.on_changed(
            move |res| {
                match res {
                    Ok(pair) => {
                        let mut cache = task_cache_clone.write();
                        let (key, value) = pair;
                        match value {
                            Some(v) => { cache.insert(key, v); },
                            None => { cache.remove(&key); }
                        }
                    },
                    Err(_) => {
                        warn!("unexpected global callback")
                    }
                }
            }, &id
        ).wait() {
            Ok(Ok(receipt)) => {
                let mut sub_map = self.sub_receipts.write();
                sub_map.insert(id, receipt);
            },
            Ok(Err(e)) => {
                error!("Error on global store event subscription {:?}", e);
                return Ok(Err(GlobalStorageError::SubscriptionError));
            },
            Err(e) => return Err(e)
        }
        local_cache.insert(id, task_cache);
        return Ok(Ok(()))
    }

    // should be called only once, by the task manager
    pub fn invalidate(&self, id: UUID) -> Result<Result<(), GlobalStorageError>, ExecError> {
        let mut cache = self.local_cache.write();
        if !cache.contains_key(&id) {
            return Ok(Err(GlobalStorageError::StoreNotExisted));
        }
        let res = self.sm_client.invalidate(&id).wait();
        match res {
            Ok(Ok(())) => {
                match self.sub_receipts.write().remove(&id) {
                    Some(receipt) => { self.raft_client.unsubscribe(receipt).wait(); },
                    None => {}
                }
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

