use std::collections::BTreeMap;
use rdd::script::RDDScript;
use rdd::{RDDID, RDDTracker};
use rdd::funcs::RDDFunc;
use rdd::{transformers as trans};
use bifrost::utils::bincode;

// only for context transport
#[derive(Serialize, Deserialize)]
pub struct ContextScript {
    pub dag: BTreeMap<RDDID, RDDScript>
}

pub struct RDDPlaceholder <'a> {
    id: RDDID,
    ctx: &'a mut ContextScript
}

impl <'a> RDDPlaceholder <'a>  {
    pub fn map <F> (&mut self, closure: F) -> RDDPlaceholder
        where F: RDDFunc
    {
        let rdd_id = RDDID::rand();
        let func_id = F::id();
        let closure_data = bincode::serialize(&closure);
        self.ctx.dag.insert(rdd_id, RDDScript {
            func_id,
            trans: trans::map::Map::trans_id(),
            deps: vec![self.id],
            closure: closure_data,
        });
        RDDPlaceholder {
            id: rdd_id,
            ctx: self.ctx
        }
    }
}