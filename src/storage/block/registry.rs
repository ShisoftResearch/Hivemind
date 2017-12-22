use bifrost::raft::state_machine::StateMachineCtl;
use std::collections::HashMap;
use utils::uuid::UUID;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(HIVEMIND_BLOCK_REGISTRY) as u64;

pub struct BlockRegistry {
    server_mapping: HashMap<UUID, u64>
}

raft_state_machine! {
    def cmd register(id: UUID, host: u64);
    def cmd deregister(id: UUID);
}

impl StateMachineCmds for BlockRegistry {
    fn register(&mut self, id: UUID, host: u64) -> Result<(), ()> {
        self.server_mapping.insert(id, host);
        Ok(())
    }
    fn deregister(&mut self, id: UUID) -> Result<(), ()> {
        self.server_mapping.remove(&id);
        Ok(())
    }
}

impl StateMachineCtl for BlockRegistry {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        DEFAULT_SERVICE_ID
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
       unimplemented!()
    }
    fn recover(&mut self, data: Vec<u8>) {
        unimplemented!()
    }
}