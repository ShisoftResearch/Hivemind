use super::*;
use std::collections::BTreeMap;
use std::fs;
use futures::prelude::*;
use futures_cpupool::CpuPool;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use futures::prelude::*;
use futures::future;

type TaskBlocks = Arc<RwLock<BTreeMap<UUID, Arc<RwLock<LocalOwnedBlock>>>>>;
pub static BLOCK_OWNER_DEFAULT_SERVICE_ID: u64 = hash_ident!(HIVEMIND_BLOCK_SERVICE) as u64;
pub static BUFFER_CAP: usize = 5 * 1024 * 1024;
pub const DATA_LEN_SIZE: usize = 8;

pub struct BlockOwnerServer {
    inner: Arc<BlockOwnerServerInner>,
    pool: CpuPool
}

struct BlockOwnerServerInner {
    blocks: RwLock<BTreeMap<UUID, TaskBlocks>>,
    block_store: String
}

impl BlockOwnerServer {
    pub fn new(store_path: String) -> Self {
        fs::create_dir_all(&store_path);
        BlockOwnerServer {
            inner: Arc::new(BlockOwnerServerInner {
                blocks: RwLock::new(BTreeMap::new()),
                block_store: store_path,
            }),
            pool: CpuPool::new_num_cpus()
        }
    }
}

impl Service for BlockOwnerServer {
    fn read(&self, task: UUID, id: UUID, pos: u64, limit: ReadLimitBy)
        -> Box<Future<Item = (Vec<Vec<u8>>, u64), Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::read(inner, task, id, pos, limit))
    }
    fn write(&self, task: UUID, id: UUID, items: Vec<Vec<u8>>)
        -> Box<Future<Item = Vec<u64>, Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::write(inner, task, id, items))
    }
    fn remove(&self, task: UUID, id: UUID)
        ->Box<Future<Item = (), Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::remove(inner, task, id))
    }
    fn get(&self, task: UUID, id: UUID, key: UUID)
        -> Box<Future<Item = Option<Vec<u8>>, Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::get(inner, task, id, key))
    }
    fn set(&self, task: UUID, id: UUID, key: UUID, value: Vec<u8>)
        -> Box<Future<Item = (), Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::set(inner, task, id, key, value))
    }
    fn unset(&self, task: UUID, id: UUID, key: UUID)
        -> Box<Future<Item = Option<()>, Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::unset(inner, task, id, key))
    }
    fn remove_task(&self, task: UUID)
        -> Box<Future<Item = (), Error = String>>
    {
        let inner = self.inner.clone();
        box self.pool.spawn_fn(move || BlockOwnerServerInner::remove_task(inner, task))
    }
    fn new_task(&self, task: UUID)
        -> Box<Future<Item = (), Error = ()>>
    {
        self.inner.new_task(task);
        box future::ok(())
    }
    fn exists(&self, task: UUID, id: UUID)
        -> Box<Future<Item = bool, Error = String>>
    {
        box future::result(self.inner.exists(task, id))
    }
}

impl BlockOwnerServerInner {
    fn task_blocks(&self, task: &UUID) -> Result<TaskBlocks, String> {
        Ok(self.blocks
            .read()
            .get(task)
            .ok_or("NO TASK")?
            .clone())
    }
    fn read_block(&self, task: &UUID, id: &UUID)
        -> Result<Arc<RwLock<LocalOwnedBlock>>, String>
    {
        let task_blocks = self.task_blocks(task)?;
        let block = task_blocks
            .read()
            .get(id)
            .ok_or("NO BLOCK")?
            .clone();
        Ok(block)
    }
    fn write_block(&self, task: UUID, id: UUID)
        -> Result<Arc<RwLock<LocalOwnedBlock>>, String>
    {
        let task_blocks = self.task_blocks(&task)?;
        let block = task_blocks
            .write()
            .entry(id)
            .or_insert_with(||
                Arc::new(
                    RwLock::new(
                        LocalOwnedBlock::new(
                            id, &self.block_store, BUFFER_CAP))))
            .clone();
        Ok(block)
    }
    fn read(this: Arc<Self>, task: UUID, id: UUID, pos: u64, limit: ReadLimitBy)
            -> Result<(Vec<Vec<u8>>, u64), String>
    {
        let block_lock = this.read_block(&task, &id)?;
        let block = block_lock.read();
        block.read(id, pos, limit)
    }
    fn write(this: Arc<Self>, task: UUID, id: UUID, items: Vec<Vec<u8>>) -> Result<Vec<u64>, String> {
        let block = this.write_block(task, id)?;
        let mut owned = block.write();
        owned.write(id, items)
    }
    fn remove(this: Arc<Self>, task: UUID, id: UUID) -> Result<(), String> {
        let task_blocks_lock = this.task_blocks(&task)?;
        let mut blocks = task_blocks_lock.write();
        blocks
            .remove(&id)
            .ok_or_else(|| "NO BLOCK".to_string())
            .map(|_| ())
    }
    fn get(this: Arc<Self>, task: UUID, id: UUID, key: UUID) -> Result<Option<Vec<u8>>, String> {
        let block_lock = this.read_block(&task, &id)?;
        let block = block_lock.read();
        let pos = block.kv_map
            .get(&key)
            .ok_or("NO KEY")?;
        block.read(id, *pos, ReadLimitBy::Items(1)).map(|d| d.0.into_iter().next())
    }
    fn set(this: Arc<Self>, task: UUID, id: UUID, key: UUID, value: Vec<u8>) -> Result<(), String> {
        let block_lock = this.write_block(task, id)?;
        let mut block = block_lock.write();
        let loc = block.write(id, vec![value])?[0];
        block.kv_map.insert(key, loc);
        Ok(())
    }
    // only remove from index
    fn unset(this: Arc<Self>, task: UUID, id: UUID, key: UUID) -> Result<Option<()>, String> {
        let block_lock = this.write_block(task, id)?;
        let mut block = block_lock.write();
        Ok(block.kv_map.remove(&key).map(|_| ()))
    }
    fn remove_task(this: Arc<Self>, task: UUID) -> Result<(), String> {
        let retained_task = this.blocks.write().remove(&task);
        drop(retained_task); // drop/delete block files after master rwlock released
        return Ok(())
    }
    fn new_task(&self, task: UUID) {
        self.blocks
            .write()
            .entry(task)
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
    }
    fn exists(&self, task: UUID, id: UUID) -> Result<bool, String> {
        let task_blocks = self.task_blocks(&task)?;
        return Ok(task_blocks.read().contains_key(&id));
    }
}

dispatch_rpc_service_functions!(BlockOwnerServer);

pub struct LocalOwnedBlock {
    id: UUID,
    buffer: Vec<u8>,
    buffer_pos: u64,
    buffer_cap: usize,
    local_file_buf: Option<BufWriter<File>>,
    local_file_path: String,
    kv_map: HashMap<UUID, u64>,
    size: u64
}

impl LocalOwnedBlock {
    pub fn new<'a>(id: UUID, block_dir: &'a str, buffer_cap: usize) -> LocalOwnedBlock {
        let file_name = format!("{}.bin", id);
        let file_path = format!("{}/{}", block_dir, file_name);
        let mut mem_buff = Vec::with_capacity(buffer_cap);
        mem_buff.reserve(buffer_cap);
        info!("new block with path: {}", file_path);
        LocalOwnedBlock {
            id,
            buffer: mem_buff,
            buffer_pos: 0,
            buffer_cap,
            local_file_buf: None,
            local_file_path: file_path,
            kv_map: HashMap::new(),
            size: 0
        }
    }

    pub fn append_data(&mut self, data: &[u8]) -> io::Result<()> {
        let buf_cap = self.buffer_cap;
        let buf_size = self.size as usize;
        let data_len = data.len();
        let mut len_bytes = [0u8; DATA_LEN_SIZE];
        LittleEndian::write_u64(&mut len_bytes, data_len as u64);
        if buf_size + data_len > buf_cap { // write to disk
            let ensured = self.ensured_file()?;
            match self.local_file_buf {
                Some(ref mut writer) => {
                    if ensured {
                        // flush in-memory buffer
                        writer.write(self.buffer.as_slice())?;
                        self.buffer.clear();
                    }
                    writer.write(&len_bytes)?;
                    writer.write(data)?;
                    writer.flush()?;
                },
                None => return Err(io::Error::from(io::ErrorKind::NotFound))
            };
        } else { // in-memory
            self.buffer.extend_from_slice(&len_bytes);
            self.buffer.extend_from_slice(data);
        }
        self.size += (DATA_LEN_SIZE + data_len) as u64;
        Ok(())
    }

    pub fn read_data(&self, pos: usize, buf: &mut [u8]) -> io::Result<usize> {
        if self.local_file_buf.is_some() {
            let mut file = File::open(&self.local_file_path)?;
            file.seek(SeekFrom::Start(pos as u64))?;
            return file.read(buf);
        } else {
            if self.buffer.len() < pos {
                return Err(io::Error::from(io::ErrorKind::InvalidInput))
            } else {
                let bytes_to_read = if pos + buf.len() > self.buffer.len() {
                    self.buffer.len() - pos
                } else { buf.len() };
                let data: Vec<_> = self.buffer.iter()
                    .skip(pos)
                    .take(bytes_to_read)
                    .cloned()
                    .collect();
                buf.copy_from_slice(data.as_slice());
                return Ok(bytes_to_read);
            }
        }
    }

    fn ensured_file(&mut self) -> io::Result<bool> {
        if !self.local_file_buf.is_some() {
            self.local_file_buf = Some(BufWriter::new(File::create(&self.local_file_path)?));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn read (&self, id: UUID, pos: u64, limit: ReadLimitBy) -> Result<(Vec<Vec<u8>>, u64), String> {
        let mut res: Vec<Vec<u8>> = Vec::new();
        let mut read_items = 0;
        let mut read_bytes = 0 as usize;
        let mut cursor = pos as usize;
        while match limit {
            ReadLimitBy::Size(size) => read_bytes < size as usize,
            ReadLimitBy::Items(num) => read_items < num as usize,
        } && cursor < self.size as usize {
            let mut len_buf = [0u8; DATA_LEN_SIZE];
            if self
                .read_data(cursor, &mut len_buf)
                .map_err(|e| format!("{}", e))? < 1 {
                break;
            }
            let data_len = LittleEndian::read_u64(&len_buf) as usize;
            cursor += DATA_LEN_SIZE;
            let mut data_vec = vec![0u8; data_len];
            self
                .read_data(cursor, &mut data_vec)
                .map_err(|e| format!("{}", e))?;
            res.push(data_vec);
            cursor += data_len;
            read_bytes += data_len + DATA_LEN_SIZE;
            read_items += 1;
        }
        return Ok((res, cursor as u64))
    }
    fn write(&mut self, id: UUID, items: Vec<Vec<u8>>)
        -> Result<Vec<u64>, String>
    {
        let mut poses = Vec::new();
        for item in items {
            poses.push(self.buffer.len() as u64);
            self
                .append_data(item.as_slice())
                .map_err(|e| format!("{}", e))?
        }
        Ok(poses)

    }
}

impl Drop for LocalOwnedBlock {
    fn drop(&mut self) {
        if self.local_file_buf.is_some() {
            self.local_file_buf = None; // drop the writer
            remove_file(&self.local_file_path); // remove block file
        }
    }
}