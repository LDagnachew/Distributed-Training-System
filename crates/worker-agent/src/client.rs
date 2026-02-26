use tonic::transport::Channel;
pub mod distributed_trainer {
    tonic::include_proto!("distributed_trainer");
}
use distributed_trainer::orchestrator_service_client::OrchestratorServiceClient;
use distributed_trainer::{WorkerStateSnapshot, WorkerPhase};
use std::time::Duration;

pub struct WorkerAgent {
    worker_id: String,
    rank: u32,
    world_size: u32,
    coordinator_client: OrchestratorServiceClient<Channel>,
}

impl WorkerAgent {
    /// One-time setup: creates worker and connects to coordinator
    pub async fn new(
        worker_id: String,
        rank: u32,
        world_size: u32,
        coordinator_addr: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        
        println!("Worker {} connecting to coordinator at {}", worker_id, coordinator_addr);
        
        let coordinator_client = OrchestratorServiceClient::connect(
            coordinator_addr.to_string()
        ).await?;
        
        println!("Worker {} connected successfully", worker_id);
        
        Ok(Self {
            worker_id,
            rank,
            world_size,
            coordinator_client,
        })
    }
    
    /// Main worker loop - call this after setup
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            // Build snapshot
            let snapshot = WorkerStateSnapshot {
                run_id: "0".to_string(),
                worker_id: self.worker_id.clone(),
                rank: self.rank,
                world_size: self.world_size,
                phase: WorkerPhase::Idle as i32,
                current_step: 0,
                last_checkpoint_id: None,
                checkpoint_in_progress: None,
                last_executed_command_id: None,
                current_command_id: None,
                timestamp: Some(std::time::SystemTime::now().into()),
                error_message: None,
                progress: None,
            };
            
            // Call coordinator
            match self.coordinator_client.report_state(snapshot).await {
                Ok(response) => {
                    let command = response.into_inner();
                    println!("Received command: {:?}", command);
                    // Handle command TODO
                }
                Err(e) => {
                    eprintln!("Failed to report state: {}", e);
                }
            }
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One-time setup
    let worker = WorkerAgent::new(
        "worker-1".to_string(),
        0,  // rank
        3,  // world_size
        "http://localhost:50051"
    ).await?;
    
    // Run the worker loop
    worker.run().await?;
    
    Ok(())
}
