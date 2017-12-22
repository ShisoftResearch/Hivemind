use super::*;

static BUFFER_CAP: usize = 5 * 1027 * 1024;

service! {
    rpc read(id: UUID, pos: u64, limit: ReadLimitBy) -> Vec<Vec<u8>> | String;
    rpc write(id: UUID, items: Vec<Vec<u8>>) | String;
}

pub struct BlockOwnerServer {
    blocks: RwLock<HashMap<UUID, Arc<RwLock<LocalOwnedBlock>>>>,
    block_store: String
}

impl Service for BlockOwnerServer {
    fn read(&self, id: &UUID, pos: &u64, limit: &ReadLimitBy) -> Result<Vec<Vec<u8>>, String> {
        let block = self.blocks
            .read()
            .get(id)
            .ok_or("NO BLOCK")?
            .clone();
        let owned = block.read();
        let mut res: Vec<Vec<u8>> = Vec::new();
        let mut read_items = 0;
        let mut read_bytes = 0 as usize;
        let mut cursor = *pos as usize;
        while match limit {
            &ReadLimitBy::Size(size) => read_bytes < size as usize,
            &ReadLimitBy::Items(num) => read_items < num as usize
        } {
            let mut len_buf = [0u8; 8];
            if owned
                .read_data(cursor, &mut len_buf)
                .map_err(|e| format!("{}", e))? < 1 {
                break;
            }
            let data_len = LittleEndian::read_u64(&len_buf) as usize;
            cursor += 8;
            let mut data_vec = vec![0u8; data_len];
            owned
                .read_data(cursor, &mut data_vec)
                .map_err(|e| format!("{}", e))?;
            res.push(data_vec);
            cursor += data_len;
            read_bytes += data_len + 8;
            read_items += 1;
        }
        return Ok(res)
    }
    fn write(&self, id: &UUID, items: &Vec<Vec<u8>>) -> Result<(), String> {
        let block = self.blocks
            .write()
            .entry(*id)
            .or_insert_with(||
                Arc::new(
                    RwLock::new(
                        LocalOwnedBlock::new(
                            *id, &self.block_store, BUFFER_CAP))))
            .clone();
        let mut owned = block.write();
        for item in items {
            owned
                .append_data(item.as_slice())
                .map_err(|e| format!("{}", e))?
        }
        Ok(())
    }
}

dispatch_rpc_service_functions!(BlockOwnerServer);


pub struct LocalOwnedBlock {
    id: UUID,
    buffer: Vec<u8>,
    buffer_pos: u64,
    local_file_buf: Option<BufWriter<File>>,
    local_file_path: String
}

impl LocalOwnedBlock {
    pub fn new<'a>(id: UUID, block_dir: &'a str, buffer_cap: usize) -> LocalOwnedBlock {
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

    pub fn append_data(&mut self, data: &[u8]) -> io::Result<()> {
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