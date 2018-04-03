use std::sync::Arc;
use std::env;
use hivemind::server::*;
use bifrost::rpc;
use bifrost::raft;
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use bifrost::raft::client::RaftClient;
use futures::prelude::*;

pub fn get_server(mut opts: ServerOptions, port: u32) -> Arc<HMServer> {
    if let Ok(env_store) = env::var("HM_CACHE_STORE") {
        opts.storage = format!("{}/{}", env_store, opts.storage);
    }
    let address = format!("127.0.0.1:{}", port);
    let rpc_server = rpc::Server::new(&address);
    let group_name = "test".to_string();
    let meta_members = &vec![address.to_owned()];
    let raft_service = raft::RaftService::new(raft::Options {
        storage: raft::Storage::MEMORY,
        address: address.to_owned(),
        service_id: raft::DEFAULT_SERVICE_ID
    });
    rpc::Server::listen_and_resume(&rpc_server);
    rpc_server.register_service(raft::DEFAULT_SERVICE_ID, &raft_service);
    raft::RaftService::start(&raft_service);
    raft_service.bootstrap();
    Membership::new(&rpc_server, &raft_service);
    let raft_client =
        RaftClient::new(meta_members, raft::DEFAULT_SERVICE_ID).unwrap();
    RaftClient::prepare_subscription(&rpc_server);
    let member_service = MemberService::new(&address, &raft_client);
    member_service.join_group(&group_name).wait().unwrap();
    HMServer::new(
        &opts,
        &rpc_server,
        Some(&raft_service),
        &raft_client,
        &group_name,
        &address).unwrap()
}

#[test]
pub fn server_startup() {
    get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5200);
}