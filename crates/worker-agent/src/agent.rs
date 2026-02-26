use common::{
    orchestrator_service_client::OrchestratorServiceClient,
    WorkerStateSnapshot,
    OrchestratorCommand,
    WorkerPhase,
    orchestrator_command::CommandType,
};
use std::time::{SystemTime, Duration};
use tokio::time;

pub struct WorkerAgent {
    worker_id: String,
    rank: u32,
    world_size: u32,
    client: OrchestratorServiceClient<tonic::transport::Channel>,
    
    // Worker state
    phase: WorkerPhase,
    current_step: u64,
    last_executed_command_id: Option<u64>,
}

impl WorkerAgent {
    pub async fn new(
        worker_id: String,
        rank: u32,
        world_size: u32,
        orchestrator_addr: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        println!("Worker {} connecting to orchestrator at {}", worker_id, orchestrator_addr);
        
        let client = OrchestratorServiceClient::connect(orchestrator_addr.to_string()).await?;
        
        println!("Worker {} connected successfully", worker_id);
        
        Ok(Self {
            worker_id,
            rank,
            world_size,
            client,
            phase: WorkerPhase::Idle,
            current_step: 0,
            last_executed_command_id: None,
        })
    }
    
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = time::interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            // Build state snapshot
            let snapshot = self.build_snapshot();
            
            // Send to orchestrator
            match self.client.report_state(snapshot).await {
                Ok(response) => {
                    let command = response.into_inner();
                    self.handle_command(command)?;
                }
                Err(e) => {
                    eprintln!("Worker {} failed to report state: {}", self.worker_id, e);
                    if e.to_string().contains("h2 protocol error") {
                        eprintln!(
                            "Hint: verify --orchestrator points to the coordinator gRPC endpoint (e.g. http://[::1]:50051) and that coordinator is running."
                        );
                    }
                }
            }
            
            // Simulate training progress
            self.simulate_work();
        }
    }
    
    fn build_snapshot(&self) -> WorkerStateSnapshot {
        WorkerStateSnapshot {
            worker_id: self.worker_id.clone(),
            rank: self.rank,
            world_size: self.world_size,
            phase: self.phase as i32,
            current_step: self.current_step,
            last_checkpoint_id: None,
            checkpoint_in_progress: None,
            last_executed_command_id: self.last_executed_command_id,
            current_command_id: None,
            timestamp: Some(SystemTime::now().into()),
            error_message: None,
            progress: None,
            run_id: "test-run-1".to_string(),
        }
    }
    
    fn handle_command(&mut self, command: OrchestratorCommand) -> Result<(), Box<dyn std::error::Error>> {
        // Check if already executed (idempotency)
        if let Some(last_executed) = self.last_executed_command_id {
            if command.command_id <= last_executed {
                return Ok(());
            }
        }
        
        match command.command_type {
            Some(CommandType::StartTraining(cmd)) => {
                println!("Worker {}: Received StartTraining command", self.worker_id);
                self.phase = WorkerPhase::Training;
            }
            Some(CommandType::BeginCheckpoint(cmd)) => {
                println!("Worker {}: Received BeginCheckpoint command (id: {})", 
                         self.worker_id, cmd.checkpoint_id);
                self.phase = WorkerPhase::Checkpointing;
                // Simulate checkpoint delay
                // (In real version, this would be async)
            }
            Some(CommandType::ResumeTraining(_)) => {
                println!("Worker {}: Received ResumeTraining command", self.worker_id);
                self.phase = WorkerPhase::Training;
            }
            Some(CommandType::NoOp(_)) => {
                // Nothing to do
            }
            _ => {
                println!("Worker {}: Received unknown command", self.worker_id);
            }
        }
        
        self.last_executed_command_id = Some(command.command_id);
        
        Ok(())
    }
    
    fn simulate_work(&mut self) {
        match self.phase {
            WorkerPhase::Training => {
                // Simulate training progress
                self.current_step += 10;
                if self.current_step % 100 == 0 {
                    println!("Worker {} training: step {}", self.worker_id, self.current_step);
                }
            }
            WorkerPhase::Checkpointing => {
                // Simulate checkpoint completion
                println!("Worker {} completed checkpoint", self.worker_id);
                self.phase = WorkerPhase::Training;
            }
            _ => {}
        }
    }
}
