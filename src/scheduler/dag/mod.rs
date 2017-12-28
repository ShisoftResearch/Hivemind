// DAG scheduler is a static scheduler which is used at the task manager to generate stages.
// Stages are generated and connected at task manager then distributed to executors for execution

use contexts::script::ScriptContext;
use rdd::script::RDDScriptCtx;
use rdd::RDDID;

#[derive(Serialize, Deserialize)]
pub struct StagePlan {
    id: u32,
    rdds: Vec<RDDID>,
    deps: Vec<u32>
}

impl StagePlan {

    fn check_rdd_deps(rdd_script: &RDDScriptCtx, ctx: &mut ScriptContext) {
        let mut rdd_scr = rdd_script;
        match rdd_scr {
            &RDDScriptCtx::Transformer { id, ref data } => {

            }
        }
    }

    pub fn compile(ctx: &mut ScriptContext) {
        let start = ctx.start;

    }

}