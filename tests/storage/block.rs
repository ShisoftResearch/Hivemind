use hivemind::resource;
use hivemind::server::*;
use hivemind::utils::uuid::UUID;
use hivemind::storage::block::*;
use server::get_server;
use futures::prelude::*;

#[test]
pub fn primary() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5201);
    let task = UUID::rand();
    let k1 = UUID::rand();
    let k2 = UUID::rand();
    let k3 = UUID::rand();
    let k4 = UUID::rand();
    let server_id = server.server_id;
    let block_manager = server.storage_managers.block.to_owned();
    block_manager.new_task(server_id, task);
    block_manager.write(server_id, task, k1, vec![vec![1u8, 2u8, 3u8]]);
    block_manager.write(server_id, task, k1, vec![vec![4u8, 5u8, 6u8]]);
    let cursor = BlockCursor::new(task, k1, ReadLimitBy::Items(10));
    let (data, new_cursor) = block_manager.read(server_id, cursor).wait().unwrap();
}