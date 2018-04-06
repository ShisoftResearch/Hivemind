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
    assert_eq!(data, vec![vec![1u8, 2u8, 3u8], vec![4u8, 5u8, 6u8]]);
    assert_eq!(&new_cursor.pos, &22);
    let test_iter = 1024 * 1024;
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        block_manager.write(server_id, task, k1, vec![vec![n, n]]).wait().unwrap();
    }
    let mut bulk_cursor = BlockCursor::new(task, k1, ReadLimitBy::Items(1));
    bulk_cursor.pos = new_cursor.pos;
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        let (data, new_cursor) = block_manager.read(server_id, bulk_cursor).wait().unwrap();
        bulk_cursor = new_cursor;
        assert_eq!(data, vec![vec![n, n]]);
    }
}