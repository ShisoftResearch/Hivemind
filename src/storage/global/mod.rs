mod state_machine;

use utils::uuid::UUID;
use std::collections::HashMap;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum GlobalStorageError {
    StoreNotExisted,
    StoreExisted,
}

pub struct GlobalManager {
    sm_client: client::SMClient
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

