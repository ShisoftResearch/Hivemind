use hivemind::resource;
use hivemind::server::*;
use hivemind::utils::uuid::UUID;
use hivemind::storage::block::Service;
use server::get_server;
use futures::prelude::*;

#[test]
pub fn primary() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5201);
    let task = UUID::rand();
    let block_manager = server.storage_managers.block.to_owned();
    // block_manager.new_task(task);
}