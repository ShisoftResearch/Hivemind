use std::rc::Rc;
use rdd::{RDDID, RDD};
use rdd::transformers::REGISTRY;

// only for RDD transport
#[derive(Serialize, Deserialize)]
pub struct RDDScript {
    pub rdd_id: RDDID,
    pub ctx: RDDScriptCtx,
    pub deps: Vec<RDDID>,
}

#[derive(Serialize, Deserialize)]
pub enum RDDScriptCtx {
    Transformer {
        id: u64,
        data: Vec<u8>,
    }
}

impl RDDScript {
    pub fn compile(&self) -> Result<Rc<RDD>, String> {
        match self.ctx {
            RDDScriptCtx::Transformer {id, ref data} => {
                let reg_trans = REGISTRY.get(id).ok_or("cannot find rdd transformer")?;
                let args = (reg_trans.construct_args)(data);
                (reg_trans.construct)(args)
            }
        }
    }
}