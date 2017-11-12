use super::{RDDID, RDD};

// only for RDD transport
#[derive(Serialize, Deserialize)]
pub struct RDDScript {
    pub func_id: u64,
    pub trans: u64,
    pub deps: Vec<RDDID>,
    pub closure: Vec<u8>
}

impl RDDScript {
    pub fn compile(&self) -> Box<RDD> {
        unimplemented!()
    }
}