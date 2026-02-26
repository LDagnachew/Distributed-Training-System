use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Response};

use common::{
    WorkerStateSnapshot,
    OrchestratorCommand,
    distributed_trainer::orchestrator_service_server::{
        OrchestratorService,
        OrchestratorServiceServer,
    },
};
use crate::controller::JobController;

pub struct MyOrchestratorService {
    controller: Arc<Mutex<JobController>>,
}

impl MyOrchestratorService {
    pub fn new(controller: Arc<Mutex<JobController>>) -> Self {
        Self { controller }
    }
}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestratorService {
    async fn report_state(
        &self,
        request: tonic::Request<WorkerStateSnapshot>,
    ) -> Result<tonic::Response<OrchestratorCommand>, tonic::Status> {
        let mut controller = self.controller.lock().await;
        let reply = controller.compute_action(request.into_inner());
        println!("I actually managed to recieve a command!");
        Ok(Response::new(reply))
    }
}

pub async fn boot_coordinator(controller: Arc<Mutex<JobController>>) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let agent_service = MyOrchestratorService::new(controller);

    println!("OrchestratorService listening on {}", addr);

    Server::builder()
        .add_service(OrchestratorServiceServer::new(agent_service))
        .serve(addr)
        .await?;
    Ok(())
}
