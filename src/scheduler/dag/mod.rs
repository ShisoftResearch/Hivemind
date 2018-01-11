// DAG scheduler is a static scheduler which is used at the task manager to generate stages.
// Stages are generated and connected at task manager then distributed to executors for execution

use contexts::script::ScriptContext;
use rdd::script::RDDScript;
use rdd::RDDID;
use rdd::dependency::DependencyScript;

use std::collections::VecDeque;

#[derive(Serialize, Deserialize)]
pub enum GenerateStagesError {
    StartRDDNotFound,
    Unexpected
}

#[derive(Serialize, Deserialize)]
pub struct DAGScheduler {
    stages: Vec<StageScript>
}

// this is going to send to the executors
#[derive(Serialize, Deserialize)]
pub struct StageScript {
    id: u64,
    rdds: Vec<RDDID>,
    deps: Vec<u64>
}

impl DAGScheduler {
    pub fn compile(ctx: &mut ScriptContext) -> Result<(), GenerateStagesError> {
        if !ctx.dag.contains_key(&ctx.start) {
            return Err(GenerateStagesError::StartRDDNotFound)
        }
        let mut rdd_scr_opt =
            ctx.dag.get(&ctx.start);
        let mut stages: Vec<DAGScheduler> = vec![];
        let mut current_stage = StageScript {
            id: 0, rdds: vec![], deps: vec![]
        };
        let bfs_queue: VecDeque<RDDID> = VecDeque::new();
        while let Some(rdd_scr) = rdd_scr_opt  {
            let all_narrow = rdd_scr.deps
                .iter()
                .all(|scr| {
                    match scr {
                        &DependencyScript::Narrow(_) => true,
                        &DependencyScript::Shuffle(_) => false,
                    }
                });
            if all_narrow {
                // keep current stage, add current rdd id into it and continue bfs

            }
        }
        unimplemented!()
    }

}