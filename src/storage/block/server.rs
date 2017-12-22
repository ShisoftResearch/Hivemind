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
                            *id, &self.block_store, BUFFER_CAP
                        ))))
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