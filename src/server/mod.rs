use bifrost::rpc;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft;
use bifrost::raft::client::{RaftClient, ClientError as RaftClientError};
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use std::sync::Arc;
use server::members::{LiveMembers, InitLiveMembersError};
use storage::StorageManagers;
use storage::block::server::{BlockOwnerServer, BLOCK_OWNER_DEFAULT_SERVICE_ID};
use storage::global::state_machine::GlobalStore;
use storage::immutable::state_machine::ImmutableStorageRegistry;
use parking_lot::RwLock;
use futures::Future;

pub mod resources;
pub mod members;

lazy_static! {
    static ref DEFAULT: RwLock<Option<Arc<HMServer>>> = RwLock::new(None);
}

#[derive(Debug)]
pub enum ServerError {
    CannotJoinCluster,
    CannotJoinClusterGroup(sm_master::ExecError),
    CannotInitLiveMemberTable(InitLiveMembersError),
    CannotSetServerWeight,
    CannotInitConsistentHashTable,
    CannotLoadMetaClient(RaftClientError),
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
    pub live_members: Arc<LiveMembers>,
    pub storage_managers: Arc<StorageManagers>,
    pub block_server: Arc<BlockOwnerServer>,
    pub server_id: u64
}

impl HMServer {
    pub fn new(
        opts: &ServerOptions,
        rpc: &Arc<rpc::Server>,
        raft: Option<&Arc<raft::RaftService>>
    ) -> Result<Arc<HMServer>, ServerError> {
        let raft_client =
            RaftClient::new(&opts.meta_members, raft::DEFAULT_SERVICE_ID)
                .map_err(|e| ServerError::CannotLoadMetaClient(e))?;
        let (member_service, live_members) =
            HMServer::load_cluster_clients(&raft_client, &opts, &rpc)?;
        let server_id = rpc.server_id;
        let storage_managers = StorageManagers::new(&live_members, &raft_client, server_id);
        let block_server = Arc::new(BlockOwnerServer::new(opts.storage.to_owned()));
        rpc.register_service(BLOCK_OWNER_DEFAULT_SERVICE_ID, &block_server);
        if let Some(ref raft) = raft {
            let global_store = GlobalStore::new(raft);
            let immutable_store_registry = ImmutableStorageRegistry::new();
            raft.register_state_machine(box global_store);
            raft.register_state_machine(box immutable_store_registry);
        }
        let refer = Arc::new(
            HMServer {
                rpc: rpc.clone(),
                member_pool: rpc::ClientPool::new(),
                member_service,
                live_members,
                storage_managers,
                block_server,
                server_id
            }
        );
        let mut def_val = DEFAULT.write();
        *def_val = Some(refer.clone());
        Ok(refer)
    }
    pub fn default() -> Arc<HMServer> {
        let def = DEFAULT.read();
        return def.clone().unwrap();
    }
    fn join_group(opt: &ServerOptions, raft_client: &Arc<RaftClient>) -> Result<Arc<MemberService>, ServerError> {
        let member_service = MemberService::new(&opt.address, raft_client);
        match member_service.join_group(&opt.group_name).wait() {
            Ok(_) => Ok(member_service),
            Err(e) => {
                error!("Cannot join cluster group");
                Err(ServerError::CannotJoinClusterGroup(e))
            }
        }
    }
    fn load_cluster_clients (
        raft_client: &Arc<RaftClient>,
        opts: &ServerOptions,
        rpc_server: &Arc<rpc::Server>
    ) -> Result<(Arc<MemberService>, Arc<LiveMembers>), ServerError> {

        let member_service = Self::join_group(opts, raft_client)?;
        let live_members =
            LiveMembers::new(&opts.group_name, raft_client)
                .map_err(|e| ServerError::CannotInitLiveMemberTable(e))?;
        RaftClient::prepare_subscription(rpc_server);
        Ok((member_service, live_members))
    }
}