pub mod state_machine;
use bifrost::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use bifrost::raft::state_machine::master::ExecError;
use utils::uuid::UUID;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum ImmutableStorageRegistryError {
    RegistryNotExisted,
    RegistryExisted,
    ItemExisted
}