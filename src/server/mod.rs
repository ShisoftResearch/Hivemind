use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOptions {
    pub processors: u16,
}

pub struct HMServer {
    pub raft_service: Option<Arc<raft::RaftService>>,
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub member_pool: rpc::ClientPool,
    pub server_id: u64
}

impl HMServer {

}