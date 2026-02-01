
use tonic::{transport::Server, Request, Response, Status};

pub mod agent {
    tonic::include_proto!("agent");
}
use agent::agent_service_server::{AgentService, AgentServiceServer};
use agent::{WorkerStateSnapshot, GetWorkerStateRequest};

#[derive(Debug, Default)]
pub struct MyAgentService {}

#[tonic::async_trait]
impl AgentService for MyAgentService {
    async fn send_worker_state(
        &self,
        _request: Request<GetWorkerStateRequest>,
    ) -> Result<Response<WorkerStateSnapshot>, Status> {
        // Return a dummy WorkerStateSnapshot for now
        let reply = WorkerStateSnapshot {
            run_id: "dummy_run".to_string(),
            worker_id: "dummy_worker".to_string(),
            rank: 0,
            world_size: 1,
            phase: 0,
            last_checkpoint_id: 0,
            checkpoint_in_progress: None,
            last_executed_command_id: None,
            current_command_id: None,
            timestamp: None,
            err: None,
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let agent_service = MyAgentService::default();

    println!("AgentService listening on {}", addr);

    Server::builder()
        .add_service(AgentServiceServer::new(agent_service))
        .serve(addr)
        .await?;

    Ok(())
}
