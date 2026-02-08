use tonic::{transport::Server, Request, Response, Status};

pub mod distributed_trainer {
    tonic::include_proto!("distributed_trainer");
}
use distributed_trainer::orchestrator_service_server::{OrchestratorService, OrchestratorServiceServer};
use distributed_trainer::{OrchestratorCommand, NoOp, WorkerStateSnapshot};
use distributed_trainer::orchestrator_command::CommandType;

#[derive(Debug, Default)]
pub struct MyOrchestratorService {}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestratorService {
    async fn report_state(
        &self,
        _request: Request<WorkerStateSnapshot>,
    ) -> Result<Response<OrchestratorCommand>, Status> {
        let reply = OrchestratorCommand {
            command_id: 1,
            issued_at: None,
            command_type: Some(CommandType::NoOp(NoOp {})),
        };
        println!("I actually managed to recieve a command!");
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let agent_service = MyOrchestratorService::default();

    println!("OrchestratorService listening on {}", addr);

    Server::builder()
        .add_service(OrchestratorServiceServer::new(agent_service))
        .serve(addr)
        .await?;

    Ok(())
}