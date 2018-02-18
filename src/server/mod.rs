use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use std::sync::Arc;

pub mod resources;
pub mod members;

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotInitMemberTable,
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient,
    CannotInitializeSchemaServer(sm_master::ExecError),
    StandaloneMustAlsoBeMetaServer,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOptions {
    pub processors: u16,
    pub address: String,
    pub group_name: String,
    pub meta_members: Vec<String>,
    pub storage: String
}

pub struct HMServer {
    pub rpc: Arc<rpc::Server>,
    pub member_pool: rpc::ClientPool,
    pub member_service: Arc<MemberService>,
    pub server_id: u64
}

impl HMServer {
    pub fn new(
        opts: &ServerOptions,
        rpc: &Arc<rpc::Server>,
    ) -> Result<Arc<HMServer>, ServerError> {
        let member_service = HMServer::load_cluster_clients(&opts, &rpc)?;
        Ok(Arc::new(
            HMServer {
                rpc: rpc.clone(),
                member_pool: rpc::ClientPool::new(),
                member_service,
                server_id: rpc.server_id
            }
        ))
    }

    fn join_group(opt: &ServerOptions, raft_client: &Arc<RaftClient>) -> Result<Arc<MemberService>, ServerError> {
        let member_service = MemberService::new(&opt.address, raft_client);
        match member_service.join_group(&opt.group_name) {
            Ok(_) => Ok(member_service),
            Err(e) => {
                error!("Cannot join cluster group");
                Err(ServerError::CannotJoinClusterGroup(e))
            }
        }
    }
    fn load_cluster_clients (
        opt: &ServerOptions,
        rpc_server: &Arc<rpc::Server>
    ) -> Result<Arc<MemberService>, ServerError> {
        let raft_client =
            RaftClient::new(&opt.meta_members, raft::DEFAULT_SERVICE_ID);
        match raft_client {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(rpc_server);
                Ok(Self::join_group(opt, &raft_client)?)
            },
            Err(e) => {
                error!("Cannot load meta client: {:?}", e);
                Err(ServerError::CannotLoadMetaClient)
            }
        }
    }
}