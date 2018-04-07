use hivemind::resource;
use hivemind::server::*;
use hivemind::utils::uuid::UUID;
use hivemind::storage::block::*;
use server::get_server;
use futures::prelude::*;
use futures::future;

#[test]
pub fn streaming() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5201);
    let task = UUID::rand();
    let id = UUID::rand();
    let server_id = server.server_id;
    let block_manager = server.storage_managers.block.to_owned();
    block_manager.new_task(server_id, task);
    block_manager.write(server_id, task, id, vec![vec![1u8, 2u8, 3u8]]);
    block_manager.write(server_id, task, id, vec![vec![4u8, 5u8, 6u8]]);
    let cursor = BlockCursor::new(task, id, ReadLimitBy::Items(10));
    let (data, new_cursor) = block_manager.read(server_id, cursor).wait().unwrap();
    assert_eq!(data, vec![vec![1u8, 2u8, 3u8], vec![4u8, 5u8, 6u8]]);
    assert_eq!(&new_cursor.pos, &22);
    let test_iter = server::BUFFER_CAP / 8;// ensure exceeds in-memory buffer
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        block_manager.write(server_id, task, id, vec![vec![n, n]]).wait().unwrap();
    }
    let mut bulk_cursor = BlockCursor::new(task, id, ReadLimitBy::Items(1));
    bulk_cursor.pos = new_cursor.pos;
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        let (data, new_cursor) = block_manager.read(server_id, bulk_cursor).wait().unwrap();
        bulk_cursor = new_cursor;
        assert_eq!(data, vec![vec![n, n]], "at iter: {}, cursor {}, MEM_CAP {}", i, bulk_cursor.pos, server::BUFFER_CAP);
    }
    block_manager.remove_task(server_id, task);
}

#[test]
pub fn key_value() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5202);
    let task = UUID::rand();
    let id = UUID::rand();
    let server_id = server.server_id;
    let block_manager = server.storage_managers.block.to_owned();
    let test_iter = server::BUFFER_CAP / 8;// ensure exceeds in-memory buffer
    block_manager.new_task(server_id, task);
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        let key = UUID::new(i as u64, i as u64);
        block_manager.set(server_id, task, id, key, vec![n, n]).wait().unwrap();
    }
    for i in 0..test_iter {
        let n: u8 = (i % 255) as u8;
        let key = UUID::new(i as u64, i as u64);
        let data = block_manager.get(server_id, task, id, key).wait().unwrap().unwrap();
        assert_eq!(data, vec![n, n], "at iter: {}", i);
    }
    block_manager.remove_task(server_id, task);
}

#[test]
pub fn parallel() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5203);
    let task = UUID::rand();
    let id = UUID::rand();
    let server_id = server.server_id;
    let block_manager = server.storage_managers.block.to_owned();
    let test_iter = server::BUFFER_CAP / 8;// ensure exceeds in-memory buffer
    block_manager.new_task(server_id, task);
    let set_futs =
        (0..test_iter)
            .map(|i| {
                let n: u8 = (i % 255) as u8;
                let key = UUID::new(i as u64, i as u64);
                block_manager.set(server_id, task, id, key, vec![n, n])
            });
    future::join_all(set_futs).wait().unwrap();

    let get_futs =
        (0..test_iter)
            .map(|i| {
                let n: u8 = (i % 255) as u8;
                let key = UUID::new(i as u64, i as u64);
                block_manager.get(server_id, task, id, key)
                    .map(|x| x.unwrap())
                    .map(move |x| assert_eq!(x, vec![n, n], "at iter: {}", i))
            });
    future::join_all(get_futs).wait().unwrap();

    block_manager.remove_task(server_id, task);
}