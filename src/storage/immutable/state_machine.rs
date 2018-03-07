use super::*;
use std::collections::{BTreeMap};
use std::sync::Arc;
use bifrost::raft::RaftService;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::SMCallback;


pub static RAFT_SM_ID: u64 = hash_ident!(IMMUTABLE_STORAGE_REGISTRY_STATE_MACHINE) as u64;

raft_state_machine! {
    def cmd create_registry(id: UUID) | ImmutableStorageRegistryError;
    def cmd set_location(id: UUID, key: UUID, server: u64) | ImmutableStorageRegistryError;
    def qry get_location(id: UUID, key: UUID) -> u64 | ImmutableStorageRegistryError;
    def cmd dispose_registry(id: UUID) | ImmutableStorageRegistryError;
}

pub struct ImmutableStorageRegistry {
    registry: BTreeMap<UUID, BTreeMap<UUID, u64>>
}

impl StateMachineCmds for ImmutableStorageRegistry {
    fn create_registry(&mut self, id: UUID) -> Result<(), ImmutableStorageRegistryError> {
        if self.registry.contains_key(&id) {
            return Err(ImmutableStorageRegistryError::RegistryExisted)
        }
        self.registry.insert(id, BTreeMap::new());
        return Ok(())
    }

    fn set_location(&mut self, id: UUID, key: UUID, server: u64) -> Result<(), ImmutableStorageRegistryError> {
        if let Some(ref mut m) = self.registry.get_mut(&id) {
            if m.contains_key(&key) {
                return Err(ImmutableStorageRegistryError::ItemExisted);
            } else {
                m.insert(key, server);
                return Ok(())
            }
        } else {
            return Err(ImmutableStorageRegistryError::RegistryNotExisted)
        }
    }

    fn get_location(&self, id: UUID, key: UUID) -> Result<u64, ImmutableStorageRegistryError> {
        if let Some(ref m) = self.registry.get(&id) {
            if let Some(server) = m.get(&key) {
                return Ok(*server)
            } else {
                return  Err(ImmutableStorageRegistryError::ItemExisted)
            }
        } else {
            return Err(ImmutableStorageRegistryError::RegistryNotExisted)
        }
    }

    fn dispose_registry(&mut self, id: UUID) -> Result<(), ImmutableStorageRegistryError> {
        if self.registry.remove(&id).is_some() {
            return Ok(())
        } else {
            return Err(ImmutableStorageRegistryError::RegistryNotExisted)
        }
    }
}

impl StateMachineCtl for ImmutableStorageRegistry {
    raft_sm_complete!();
    fn id(&self) -> u64 {10}
    fn snapshot(&self) -> Option<Vec<u8>> { None }
    fn recover(&mut self, data: Vec<u8>) {}
}

impl ImmutableStorageRegistry {
    pub fn new() -> ImmutableStorageRegistry {
        ImmutableStorageRegistry {
            registry: BTreeMap::new()
        }
    }
}