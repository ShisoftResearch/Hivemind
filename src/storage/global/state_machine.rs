use super::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use bifrost::raft::RaftService;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::SMCallback;

pub static RAFT_SM_ID: u64 = hash_ident!(GLOBAL_STORAGE_STATE_MACHINE) as u64;

pub struct GlobalStore {
    data: BTreeMap<UUID, HashMap<Vec<u8>, Vec<u8>>>,
    callback: SMCallback
}

impl GlobalStore {
    pub fn new(raft_service: &Arc<RaftService>) -> GlobalStore {
        GlobalStore {
            data: BTreeMap::new(),
            callback: SMCallback::new(RAFT_SM_ID, raft_service.clone())
        }
    }
}

impl StateMachineCmds for GlobalStore {
    fn create_store(&mut self, id: UUID) -> Result<(), GlobalStorageError> {
        self.assert_not_existed(&id)?;
        self.data.insert(id, HashMap::new());
        return Ok(());
    }
    fn invalidate(&mut self, id: UUID) -> Result<(), GlobalStorageError> {
        self.assert_existed(&id)?;
        self.data.remove(&id);
        self.callback.notify(&commands::on_invalidation::new(&id), Ok(()));
        return Ok(());
    }

    fn set(&mut self, id: UUID, key: Vec<u8>, val: Option<Vec<u8>>)
        -> Result<(), GlobalStorageError>
    {
        self.assert_existed(&id)?;
        {
            let mut store = self.data.get_mut(&id).unwrap();
            match val {
                Some(ref v) => store.insert(key.clone(), v.clone()),
                None => store.remove(&key)
            };
        }
        self.notify_change(&id, key, val);
        return Ok(());
    }
    fn swap(&mut self, id: UUID, key: Vec<u8>, val: Option<Vec<u8>>)
        -> Result<Option<Vec<u8>>, GlobalStorageError>
    {
        self.assert_existed(&id)?;
        let res = {
            let mut store = self.data.get_mut(&id).unwrap();
            match val {
                Some(ref v) => return Ok(store.insert(key.clone(), v.clone())),
                None => return Ok(store.remove(&key))
            }
        };
        self.notify_change(&id, key, val);
        return Ok(res);
    }
    fn compare_and_swap(&mut self, id: UUID, key: Vec<u8>, expect: Option<Vec<u8>>, val: Option<Vec<u8>>)
        -> Result<Option<Vec<u8>>, GlobalStorageError>
    {
        self.assert_existed(&id)?;
        let (changed, actual) = {
            let mut store = self.data.get_mut(&id).unwrap();
            let actual = store.get(&key).cloned();
            let matched = expect == actual;
            if matched {
                match val {
                    Some(ref v) => store.insert(key.clone(), v.clone()),
                    None => store.remove(&key)
                };
            }
            (matched, actual)
        };
        if changed {
            self.notify_change(&id, key, val);
        }
        return Ok(actual);
    }
    fn get(&self, id: UUID, key: Vec<u8>) -> Result<Option<Vec<u8>>, GlobalStorageError> {
        self.assert_existed(&id)?;
        let store = self.data.get(&id).unwrap();
        return Ok(store.get(&key).cloned());
    }
    fn dump(&self, id: UUID) -> Result<HashMap<Vec<u8>, Vec<u8>>, GlobalStorageError> {
        self.assert_existed(&id)?;
        return Ok(self.data.get(&id).unwrap().clone());
    }
}

impl GlobalStore {
    fn assert_not_existed(&self, id: &UUID) -> Result<(), GlobalStorageError> {
        if self.data.contains_key(id) {
            Err(GlobalStorageError::StoreExisted)
        } else {
            Ok(())
        }
    }
    fn assert_existed(&self, id: &UUID) -> Result<(), GlobalStorageError> {
        if !self.data.contains_key(id) {
            Err(GlobalStorageError::StoreNotExisted)
        } else {
            Ok(())
        }
    }
    fn notify_change(&self, id: &UUID, key: Vec<u8>, val: Option<Vec<u8>>) {
        self.callback.notify(&commands::on_changed::new(id), Ok((key, val)));
    }
}

impl StateMachineCtl for GlobalStore {
    raft_sm_complete!();
    fn id(&self) -> u64 {10}
    fn snapshot(&self) -> Option<Vec<u8>> { None }
    fn recover(&mut self, data: Vec<u8>) {}
}