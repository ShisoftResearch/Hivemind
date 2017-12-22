// Block manager for shuffle and data sharing
// Blocks will be saved at where it will be needed. It's id will be registered on a raft state machine
// When shuffling, the block will be available on other nodes by fetching it from the node that generated it
// Block is like a blob container without addressing table. It can be appended and read in sequel, lookup is impossible
// Blocks also support lazy loading and streaming, which means it have a cursor so a request can fetch partial of it.

use std::fs::{File, remove_file};
use std::io;
use std::io::{BufWriter, Seek, SeekFrom};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::{Write, Read};
use bifrost::raft::RaftService;
use parking_lot::RwLock;
use utils::uuid::UUID;
use byteorder::{ByteOrder, LittleEndian};

pub mod client;
pub mod registry;

pub struct BlockManager {
    owned_blocks: RwLock<HashMap<UUID, Arc<RwLock<LocalOwnedBlock>>>>
}

pub struct LocalOwnedBlock {
    id: UUID,
    buffer: Vec<u8>,
    buffer_pos: u64,
    local_file_buf: Option<BufWriter<File>>,
    local_file_path: String
}

impl LocalOwnedBlock {
    pub fn new<'a>(block_dir: &'a str, buffer_cap: usize) -> LocalOwnedBlock {
        let id = UUID::rand();
        let file_name = format!("{}.bin", id);
        let file_path = format!("{}/{}", block_dir, file_name);
        LocalOwnedBlock {
            id,
            buffer: Vec::with_capacity(buffer_cap),
            buffer_pos: 0,
            local_file_buf: None,
            local_file_path: file_path
        }
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        let buf_cap = self.buffer.capacity();
        let buf_size = self.buffer.len();
        let data_len = data.len();
        if buf_size + data_len > buf_cap { // write to disk
            let ensured = self.ensured_file()?;
            match self.local_file_buf {
                Some(ref mut writer) => {
                    if ensured {
                        // flush in-memory buffer
                        writer.write(self.buffer.as_slice())?;
                        self.buffer.clear();
                    }
                    let mut len_bytes = [0u8; 8];
                    LittleEndian::write_u64(&mut len_bytes, data_len as u64);
                    writer.write(&len_bytes)?;
                    writer.write(data)?;
                },
                None => return Err(io::Error::from(io::ErrorKind::NotFound))
            };
        } else { // in-memory
            self.buffer.extend_from_slice(data);
        }
        Ok(())
    }

    pub fn read(&self, pos: usize, buf: &mut [u8], len: usize) -> io::Result<usize> {
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
                let read_data: Vec<_> = self.buffer.iter()
                    .skip(pos)
                    .take(bytes_to_read)
                    .cloned()
                    .collect();
                buf.copy_from_slice(read_data.as_slice());
                return Ok(bytes_to_read);
            }
        }
    }

    fn ensured_file(&mut self) -> io::Result<bool> {
        let has_file = self.local_file_buf.is_some();
        if !has_file {
            self.local_file_buf = Some(BufWriter::new(File::create(&self.local_file_path)?));
            Ok(true)
        } else {
            Ok(false)
        }
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

impl BlockManager {
    pub fn new(raft: Arc<RaftService>) -> Arc<BlockManager> {
        let registry = registry::BlockRegistry::new();
        let manager = BlockManager {
            owned_blocks: RwLock::new(HashMap::new())
        };
        raft.register_state_machine(Box::new(registry));
        Arc::new(manager)
    }
}