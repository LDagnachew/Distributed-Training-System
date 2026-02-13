use tonic::{transport::Server, Request, Response, Status};

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

#[derive(Debug, Default)]
pub struct MyOrchestratorService {}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestratorService {
    async fn report_state(
        &self,
        request: tonic::Request<WorkerStateSnapshot>,
    ) -> Result<tonic::Response<OrchestratorCommand>, tonic::Status> {
        let reply: OrchestratorCommand = crate::JobController::compute_action(request.into_inner());
        println!("I actually managed to recieve a command!");
        Ok(Response::new(reply))
    }
}

pub async fn boot_coordinator() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let agent_service: MyOrchestratorService = MyOrchestratorService::default();

    println!("OrchestratorService listening on {}", addr);

    Server::builder()
        .add_service(OrchestratorServiceServer::new(agent_service))
        .serve(addr)
        .await?;
    Ok(())
}