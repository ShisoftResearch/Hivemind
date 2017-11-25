use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use std::sync::Arc;

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
}

pub struct HMServer {
    pub rpc: Arc<rpc::Server>,
    pub consh: Arc<ConsistentHashing>,
    pub member_pool: rpc::ClientPool,
    pub server_id: u64
}

impl HMServer {
    pub fn new(
        opts: &ServerOptions,
        rpc: &Arc<rpc::Server>,
    ) -> Result<Arc<HMServer>, ServerError> {
        let conshash = HMServer::load_cluster_clients(&opts, &rpc)?;
        Ok(Arc::new(
            HMServer {
                rpc: rpc.clone(),
                consh: conshash,
                member_pool: rpc::ClientPool::new(),
                server_id: rpc.server_id
            }
        ))
    }

    pub fn join_group(opt: &ServerOptions, raft_client: &Arc<RaftClient>) -> Result<(), ServerError> {
        let member_service = MemberService::new(&opt.address, raft_client);
        match member_service.join_group(&opt.group_name) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Cannot join cluster group");
                Err(ServerError::CannotJoinClusterGroup(e))
            }
        }
    }

    fn init_conshash(opt: &ServerOptions, raft_client: &Arc<RaftClient>)
                     -> Result<Arc<ConsistentHashing>, ServerError> {
        match ConsistentHashing::new(&opt.group_name, raft_client) {
            Ok(ch) => {
                ch.set_weight(&opt.address, opt.processors as u64);
                if !ch.init_table().is_ok() {
                    error!("Cannot initialize member table");
                    return Err(ServerError::CannotInitMemberTable);
                }
                return Ok(ch);
            },
            _ => {
                error!("Cannot initialize consistent hash table");
                return Err(ServerError::CannotInitConsistentHashTable);
            }
        }
    }
    fn load_cluster_clients (
        opt: &ServerOptions,
        rpc_server: &Arc<rpc::Server>
    ) -> Result<Arc<ConsistentHashing>, ServerError> {
        let raft_client =
            RaftClient::new(&opt.meta_members, raft::DEFAULT_SERVICE_ID);
        match raft_client {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(rpc_server);
                Self::join_group(opt, &raft_client)?;
                let conshash = Self::init_conshash(opt, &raft_client)?;
                Ok(conshash)
            },
            Err(e) => {
                error!("Cannot load meta client: {:?}", e);
                Err(ServerError::CannotLoadMetaClient)
            }
        }
    }
}