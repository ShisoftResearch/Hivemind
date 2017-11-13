use std::collections::BTreeMap;
use rdd::script::RDDScript;
use rdd::{RDDID, RDDTracker};
use rdd::funcs::RDDFunc;
use rdd::{transformers as trans};
use super::TaskContext;
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
            trans_id: trans::map::Map::trans_id(),
            trans_data: bincode::serialize(&(func_id, closure_data)),
            deps: vec![self.id],
        });
        RDDPlaceholder {
            id: rdd_id,
            ctx: self.ctx
        }
    }
}

impl ContextScript {
    pub fn compile(&self) -> Result<TaskContext, String> {
        let mut runtime_context = TaskContext::new();
        for (id, script) in &self.dag {
            let compiled_scr = script.compile()?;
            runtime_context.rdds.insert(*id, compiled_scr);
        }
        return Ok(runtime_context);
    }
}