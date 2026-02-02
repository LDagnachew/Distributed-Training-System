use tonic::{transport::Server, Request, Response, Status};

pub mod orchestrator {
    tonic::include_proto!("orchestrator");
}
use orchestrator::orchestrator_service_server::{OrchestratorService, OrchestratorServiceServer};
use orchestrator::{OrchestratorCommand, GetOrchestratorCommand, NoOp};
use orchestrator::orchestrator_command::CommandType;

#[derive(Debug, Default)]
pub struct MyOrchestratorService {}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestratorService {
    async fn send_command(
        &self,
        _request: Request<GetOrchestratorCommand>,
    ) -> Result<Response<OrchestratorCommand>, Status> {
        let reply = OrchestratorCommand {
            command_id: 1,
            issued_at: None,
            command_type: Some(CommandType::NoOp(NoOp {})),
        };
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