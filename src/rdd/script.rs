use rdd::{RDDID, RDD};
use rdd::transformers::REGISTRY;

// only for RDD transport
#[derive(Serialize, Deserialize)]
pub struct RDDScript {
    pub trans_id: u64,
    pub trans_data: Vec<u8>,
    pub deps: Vec<RDDID>,
}

impl RDDScript {
    pub fn compile(&self) -> Result<Box<RDD>, String> {
        let reg_trans = REGISTRY.get(self.trans_id).ok_or("cannot find rdd")?;
        let args = (reg_trans.construct_args)(&self.trans_data);
        (reg_trans.construct)(args)
    }
}