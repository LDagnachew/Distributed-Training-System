use common::{
    // Proto-generated types
    WorkerStateSnapshot,
    OrchestratorCommand,
    WorkerPhase,
    CheckpointStatus,
    CheckpointInfo,
    orchestrator_command::CommandType,
    
    // gRPC service (for server side)
    distributed_trainer::orchestrator_service_server::{
        OrchestratorService,
        OrchestratorServiceServer,
    },

	distributed_trainer::{
		NoOp,
	},


};
use crate::config::JobConfiguration;

pub struct JobController {
	job_config: JobConfiguration,
}

impl JobController {

	pub fn new(config: JobConfiguration) -> Self {
        Self {
            job_config: config,
        }
    }

	pub fn compute_action(_status: WorkerStateSnapshot) -> OrchestratorCommand {
		let reply = OrchestratorCommand {
            command_id: 1,
			issued_at: None,
            command_type: Some(CommandType::NoOp(NoOp {})),
        };
		reply
	} 

	fn load_checkpoint() {
        todo!("Implement when making checkpointing")
	}

}