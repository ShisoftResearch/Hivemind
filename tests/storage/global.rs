use hivemind::resource;
use hivemind::server::*;
use hivemind::utils::uuid::UUID;
use hivemind::storage::global::*;
use server::get_server;
use futures::prelude::*;
use futures::future;
use futures_cpupool::CpuPool;

#[test]
pub fn cached() {
    let server = get_server(ServerOptions {
        processors: 4,
        storage: "test_data".to_owned()
    }, 5211);
    let task = UUID::rand();
    let server_id = server.server_id;
    let global_manager = server.storage_managers.global.to_owned();
    global_manager.new_task(task, true).unwrap().unwrap();
    global_manager.set(task, vec![1, 2, 3], Some(vec![4, 5, 6])).wait().unwrap();
    global_manager.set(task, vec![7, 8, 9], Some(vec![10, 11, 12])).wait().unwrap();

    assert_eq!(global_manager.get_cached(task, &vec![1, 2, 3]).unwrap().unwrap(), vec![4, 5, 6]);
    assert_eq!(global_manager.get_cached(task, &vec![7, 8, 9]).unwrap().unwrap(), vec![10, 11, 12]);

    global_manager.set(task, vec![1, 2, 3], None).wait().unwrap();
    assert!(global_manager.get_cached(task, &vec![1, 2, 3]).unwrap().is_none());
    global_manager.set(task, vec![1, 2, 3], Some(vec![13, 14, 15, 16])).wait().unwrap();
    assert_eq!(global_manager.get_cached(task, &vec![1, 2, 3]).unwrap().unwrap(), vec![13, 14, 15, 16]);

    global_manager.remove_task(task).unwrap().unwrap();
}
// TODO: more tests