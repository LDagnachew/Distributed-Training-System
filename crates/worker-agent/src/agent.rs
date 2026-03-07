use common::{
    orchestrator_service_client::OrchestratorServiceClient,
    orchestrator_command::CommandType,
    OrchestratorCommand,
    WorkerPhase,
    WorkerStateSnapshot,
};
use std::process::Stdio;
use std::time::{Duration, SystemTime};
use tokio::process::Child;
use tokio::time;
use storage::S3Uri;

pub struct WorkerAgent {
    worker_id: String,
    rank: u32,
    world_size: u32,
    client: OrchestratorServiceClient<tonic::transport::Channel>,
    
    // Worker state
    phase: WorkerPhase,
    current_step: u64,
    last_executed_command_id: Option<u64>,
    training_child: Option<Child>,
    last_checkpoint_id: Option<u64>,
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
            training_child: None,
            last_checkpoint_id: None,
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
                            "Hint: verify --orchestrator points to the coordinator gRPC endpoint 
                            (e.g. http://[::1]:50051) and that coordinator is running."
                        );
                    }
                }
            }

            // Update local view of training process + simulated progress
            self.poll_training_process();
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
            last_checkpoint_id: self.last_checkpoint_id,
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
                let s3_info = S3Uri::parse(&cmd.dataset_uri)
                    .map(|u| format!("bucket=`{}`, key_prefix=`{}`", u.bucket, u.key))
                    .unwrap_or_else(|e| format!("(failed to parse dataset_uri `{}`: {})", cmd.dataset_uri, e));

                println!(
                    "Worker {}: StartTraining run_id={} model={} lr={} batch_size={} epochs={} dataset={} checkpoint_prefix={} interval_steps={}",
                    self.worker_id,
                    cmd.run_id,
                    cmd.model,
                    cmd.learning_rate,
                    cmd.batch_size,
                    cmd.epochs,
                    s3_info,
                    cmd.checkpoint_storage_prefix,
                    cmd.checkpoint_interval_steps,
                );

                //  Just note that this is a minimal MVP integration; in a real deployment
                // you'd likely configure the Python binary/location.
                let mut command = tokio::process::Command::new("python");
                command
                    .arg("python-trainer/main.py")
                    .arg("--run-id")
                    .arg(&cmd.run_id)
                    .arg("--worker-id")
                    .arg(&self.worker_id)
                    .arg("--rank")
                    .arg(self.rank.to_string())
                    .arg("--world-size")
                    .arg(self.world_size.to_string())
                    .arg("--model")
                    .arg(&cmd.model)
                    .arg("--learning-rate")
                    .arg(cmd.learning_rate.to_string())
                    .arg("--batch-size")
                    .arg(cmd.batch_size.to_string())
                    .arg("--epochs")
                    .arg(cmd.epochs.to_string())
                    .arg("--dataset-uri")
                    .arg(&cmd.dataset_uri)
                    .arg("--checkpoint-prefix")
                    .arg(&cmd.checkpoint_storage_prefix)
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit());

                match command.spawn() {
                    Ok(child) => {
                        println!(
                            "Worker {}: launched python trainer process for run_id={} (pid={})",
                            self.worker_id,
                            cmd.run_id,
                            child.id().unwrap_or(0),
                        );
                        self.training_child = Some(child);
                        self.phase = WorkerPhase::Training;
                        self.current_step = 0;
                    }
                    Err(e) => {
                        eprintln!(
                            "Worker {}: failed to launch python trainer: {}",
                            self.worker_id, e
                        );
                        self.phase = WorkerPhase::Error;
                    }
                }
            }
            Some(CommandType::BeginCheckpoint(cmd)) => {
                println!("Worker {}: Received BeginCheckpoint command (id: {})", 
                         self.worker_id, cmd.checkpoint_id);
                self.phase = WorkerPhase::Checkpointing;
                // Simulate checkpoint delay
                // (In real version, this would be async)
                // TODO: Async + Call wait().
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

    /// Check whether the training child process has exited and update phase.
    fn poll_training_process(&mut self) {
        if let Some(child) = &mut self.training_child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if status.success() {
                        println!(
                            "Worker {}: python trainer exited successfully",
                            self.worker_id
                        );
                        // Treat a successful training run as having produced
                        // a fresh checkpoint (e.g. final model state).
                        let next_id = self.last_checkpoint_id.unwrap_or(0) + 1;
                        self.last_checkpoint_id = Some(next_id);
                        self.phase = WorkerPhase::Idle;
                    } else {
                        println!(
                            "Worker {}: python trainer exited with status {:?}",
                            self.worker_id, status
                        );
                        self.phase = WorkerPhase::Error;
                    }
                    self.training_child = None;
                }
                Ok(None) => {
                    // Still running; nothing to do.
                }
                Err(e) => {
                    eprintln!(
                        "Worker {}: error checking python trainer status: {}",
                        self.worker_id, e
                    );
                    self.training_child = None;
                    self.phase = WorkerPhase::Error;
                }
            }
        }
    }
}
