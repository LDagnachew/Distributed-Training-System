use common::{
    distributed_trainer::{NoOp, StartTraining},
    orchestrator_command::CommandType,
    OrchestratorCommand,
    WorkerPhase,
    WorkerStateSnapshot,
};
use crate::config::JobConfiguration;
use std::collections::HashSet;

pub struct JobController {
	job_config: JobConfiguration,
    next_command_id: u64,
    started_workers: HashSet<String>,
}

impl JobController {

	pub fn new(config: JobConfiguration) -> Self {
        Self {
            job_config: config,
            next_command_id: 1,
            started_workers: HashSet::new(),
        }
    }

	pub fn compute_action(&mut self, status: WorkerStateSnapshot) -> OrchestratorCommand {
        let command_type = self.reconcile(status);
        let command_id = self.next_command_id;
        self.next_command_id += 1;

		OrchestratorCommand {
            command_id,
			issued_at: None,
            command_type: Some(command_type),
        }
	} 

    fn reconcile(&mut self, status: WorkerStateSnapshot) -> CommandType {
        let worker_id = status.worker_id;
        let phase = WorkerPhase::try_from(status.phase).unwrap_or(WorkerPhase::Unspecified);

        // Start each worker once when it first reports a non-training phase.
        if !self.started_workers.contains(&worker_id)
            && matches!(phase, WorkerPhase::Starting | WorkerPhase::Idle | WorkerPhase::Unspecified)
        {
            self.started_workers.insert(worker_id.clone());
            return CommandType::StartTraining(StartTraining {
                worker_id,
                world_size: self.job_config.world_size(),
                seed: self.job_config.seed(),
                target_step: None,
            });
        }

        CommandType::NoOp(NoOp {})
    }

	fn load_checkpoint() {
        todo!("Implement when making checkpointing")
	}

}
