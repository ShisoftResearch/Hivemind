pub mod block;
pub mod global;
pub mod immutable;

pub use std::sync::Arc;
pub use server::members::LiveMembers;
pub use bifrost::raft::client::{
    RaftClient,
    SubscriptionError,
    SubscriptionReceipt};


pub struct StorageManagers {
    pub block: Arc<block::BlockManager>,
    pub global: Arc<global::GlobalManager>,
    pub immutable: Arc<immutable::ImmutableManager>,
}

impl StorageManagers {
    pub fn new(
        live_members: &Arc<LiveMembers>,
        raft_client: &Arc<RaftClient>,
        block_manager: &Arc<block::BlockManager>,
        server_id: u64
    ) -> Arc<StorageManagers> {
        let block_manager =
            block::BlockManager::new(live_members);
        Arc::new(
            StorageManagers {
                block: block_manager.clone(),
                global: global::GlobalManager::new(raft_client),
                immutable: immutable::ImmutableManager::new(
                    &block_manager, raft_client, server_id
                )
            })
    }
}