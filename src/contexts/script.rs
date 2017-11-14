use std::collections::BTreeMap;
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use rdd::script::{RDDScript, RDDScriptCtx};
use rdd::{RDDID, RDDTracker, UNIT_RDDID};
use rdd::funcs::RDDFunc;
use rdd::{transformers as trans};
use super::TaskContext;
use bifrost::utils::bincode;

trait Composer {
    fn map<F>(&self, closure: F) -> RDDPlaceholder where F: RDDFunc;
    fn filter<F>(&self, closure: F) -> RDDPlaceholder where F: RDDFunc;
}

pub trait Script {
    fn new_context() -> Self;
}

pub type ContextScript = Rc<RefCell<InnerContextScript>>;

// only for context transport
#[derive(Serialize, Deserialize)]
pub struct InnerContextScript {
    dag: BTreeMap<RDDID, RDDScript>
}

pub struct RDDPlaceholder {
    id: RDDID,
    ctx: Rc<RefCell<InnerContextScript>>
}

impl RDDPlaceholder {
    fn transform<F>(&self, closure: F, trans_id: u64) -> RDDPlaceholder
        where F: RDDFunc
    {
        let rdd_id = RDDID::rand();
        let func_id = F::id();
        let closure_data = bincode::serialize(&closure);
        {
            let mut ctx = self.ctx.borrow_mut();
            ctx.dag.insert(rdd_id, RDDScript {
                rdd_id,
                ctx: RDDScriptCtx::Transformer {
                    id: trans_id,
                    data: bincode::serialize(&(func_id, closure_data)),
                },
                deps: vec![self.id],
            });
        }
        RDDPlaceholder {
            id: rdd_id,
            ctx: self.ctx.clone()
        }
    }
}

impl  Composer for RDDPlaceholder {
    fn map<F>(&self, closure: F) -> RDDPlaceholder
        where F: RDDFunc
    {
        self.transform(closure, trans::map::Map::trans_id())
    }
    fn filter<F>(&self, closure: F) -> RDDPlaceholder
        where F: RDDFunc
    {
        self.transform(closure, trans::filter::Filter::trans_id())
    }
}

impl InnerContextScript {
    pub fn new() -> Rc<RefCell<InnerContextScript>> {
        Rc::new(RefCell::new(
            InnerContextScript {
                dag: BTreeMap::new()
            }
        ))
    }
    pub fn compile(&self) -> Result<TaskContext, String> {
        let mut runtime_context = TaskContext::new();
        for (id, script) in &self.dag {
            let compiled_scr = script.compile()?;
            runtime_context.rdds.insert(*id, compiled_scr);
        }
        return Ok(runtime_context);
    }
    fn header(this: &Rc<RefCell<InnerContextScript>>) -> RDDPlaceholder {
        RDDPlaceholder {
            id: UNIT_RDDID,
            ctx: this.clone()
        }
    }
}

impl Composer for ContextScript {
    fn map<F>(&self, closure: F) -> RDDPlaceholder
        where F: RDDFunc
    {
        InnerContextScript::header(self).map(closure)
    }
    fn filter<F>(&self, closure: F) -> RDDPlaceholder
        where F: RDDFunc
    {
        InnerContextScript::header(self).filter(closure)
    }
}

impl Script for ContextScript {
    fn new_context() -> ContextScript {
        InnerContextScript::new()
    }
}

mod test {
    use super::*;



    #[test]
    fn build_placeholder() {
        let ctx = ContextScript::new_context();
        // let mut ctx = ContextScript::new();
    }
}