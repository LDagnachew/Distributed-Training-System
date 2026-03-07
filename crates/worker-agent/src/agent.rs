use common::{
    orchestrator_service_client::OrchestratorServiceClient,
    orchestrator_command::CommandType,
    distributed_trainer::StartTraining,
    OrchestratorCommand,
    WorkerPhase,
    WorkerStateSnapshot,
};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, SystemTime};
use tokio::process::Child;
use tokio::time;
use storage::S3Uri;

/// Training hyperparameters cached from the last StartTraining command so that
/// a RestoreCheckpoint can re-launch the Python trainer with the same config.
struct CachedTrainingConfig {
    run_id: String,
    model: String,
    learning_rate: f64,
    batch_size: u64,
    epochs: u64,
    dataset_uri: String,
    checkpoint_storage_prefix: String,
    checkpoint_interval_steps: u64,
}

/// Checkpoint marker persisted by the Python trainer in `$TMPDIR/{worker_id}.ckpt`.
#[derive(Debug, Clone)]
struct PersistedCheckpointSignal {
    run_id: Option<String>,
    step: u64,
}

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
    last_checkpoint_run_id: Option<String>,

    /// Path to the signal file the Python trainer writes after each checkpoint.
    /// Preferred format: JSON `{"run_id":"...","step":123}`.
    /// Legacy format still accepted: plain integer step.
    checkpoint_signal_path: PathBuf,

    /// Saved from the most recent StartTraining command.
    cached_config: Option<CachedTrainingConfig>,
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

        let checkpoint_signal_path =
            std::env::temp_dir().join(format!("{}.ckpt", worker_id));

        let persisted_checkpoint = Self::read_signal_file(&checkpoint_signal_path);
        let (last_checkpoint_id, last_checkpoint_run_id) = persisted_checkpoint
            .map(|s| (Some(s.step), s.run_id))
            .unwrap_or((None, None));
        if let Some(step) = last_checkpoint_id {
            let run_scope = last_checkpoint_run_id
                .as_deref()
                .map(|r| format!(" run_id={r}"))
                .unwrap_or_else(|| " run_id=<legacy-unknown>".to_string());
            println!(
                "Worker {}: found persisted checkpoint at step {}{} (from {})",
                worker_id, step, run_scope, checkpoint_signal_path.display()
            );
        }

        Ok(Self {
            worker_id,
            rank,
            world_size,
            client,
            phase: WorkerPhase::Idle,
            current_step: 0,
            last_executed_command_id: None,
            training_child: None,
            last_checkpoint_id,
            last_checkpoint_run_id,
            checkpoint_signal_path,
            cached_config: None,
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let snapshot = self.build_snapshot();

            match self.client.report_state(snapshot).await {
                Ok(response) => {
                    let command = response.into_inner();
                    self.handle_command(command)?;
                }
                Err(e) => {
                    eprintln!("Worker {} failed to report state: {}", self.worker_id, e);
                    if e.to_string().contains("h2 protocol error") {
                        eprintln!(
                            "Hint: verify --orchestrator points to the coordinator gRPC endpoint \
                             (e.g. http://[::1]:50051) and that coordinator is running."
                        );
                    }
                }
            }

            self.poll_training_process();
            self.poll_checkpoint_signal();
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
            run_id: self
                .cached_config
                .as_ref()
                .map(|cfg| cfg.run_id.clone())
                .or_else(|| self.last_checkpoint_run_id.clone())
                .unwrap_or_else(|| "unknown".to_string()),
        }
    }

    fn handle_command(&mut self, command: OrchestratorCommand) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(last_executed) = self.last_executed_command_id {
            if command.command_id <= last_executed {
                return Ok(());
            }
        }

        match command.command_type {
            Some(CommandType::StartTraining(cmd)) => {
                self.handle_start_training(cmd);
            }
            Some(CommandType::RestoreCheckpoint(cmd)) => {
                let checkpoint_path = cmd.checkpoint_path.as_deref().unwrap_or("").to_string();
                println!(
                    "Worker {}: RestoreCheckpoint checkpoint_id={} path={}",
                    self.worker_id, cmd.checkpoint_id, checkpoint_path
                );

                if self.cached_config.is_none() {
                    eprintln!(
                        "Worker {}: received RestoreCheckpoint but have no cached training config \
                         — staying Idle so coordinator falls back to StartTraining.",
                        self.worker_id
                    );
                    return Ok(());
                } else {
                    self.spawn_trainer(Some(&checkpoint_path));
                }
            }
            Some(CommandType::BeginCheckpoint(cmd)) => {
                println!("Worker {}: BeginCheckpoint id={}", self.worker_id, cmd.checkpoint_id);
                self.phase = WorkerPhase::Checkpointing;
            }
            Some(CommandType::ResumeTraining(_)) => {
                println!("Worker {}: ResumeTraining", self.worker_id);
                self.phase = WorkerPhase::Training;
            }
            Some(CommandType::NoOp(_)) => {}
            _ => {
                println!("Worker {}: unknown command", self.worker_id);
            }
        }

        self.last_executed_command_id = Some(command.command_id);
        Ok(())
    }

    fn handle_start_training(&mut self, cmd: StartTraining) {
        let s3_info = S3Uri::parse(&cmd.dataset_uri)
            .map(|u| format!("bucket=`{}`, key_prefix=`{}`", u.bucket, u.key))
            .unwrap_or_else(|e| format!("(parse error: {})", e));

        println!(
            "Worker {}: StartTraining run_id={} model={} lr={} batch={} epochs={} data={} ckpt_prefix={} interval={}",
            self.worker_id, cmd.run_id, cmd.model, cmd.learning_rate, cmd.batch_size,
            cmd.epochs, s3_info, cmd.checkpoint_storage_prefix, cmd.checkpoint_interval_steps,
        );

        // If we have a persisted checkpoint from a previous run, resume from it
        // even when the coordinator sends StartTraining as a recovery fallback.
        let resume_path = if self.last_checkpoint_run_id.as_deref() == Some(cmd.run_id.as_str()) {
            self.last_checkpoint_id.map(|step| {
                format!(
                    "{}/{}/checkpoint-{}",
                    cmd.checkpoint_storage_prefix.trim_end_matches('/'),
                    cmd.run_id,
                    step,
                )
            })
        } else {
            if self.last_checkpoint_id.is_some() {
                println!(
                    "Worker {}: ignoring persisted checkpoint because run_id mismatch \
                     (persisted={:?}, incoming={})",
                    self.worker_id,
                    self.last_checkpoint_run_id,
                    cmd.run_id
                );
            }
            // Drop stale checkpoint markers from other runs so we do not keep
            // advertising an unusable checkpoint to coordinator.
            self.last_checkpoint_id = None;
            self.last_checkpoint_run_id = None;
            None
        };

        self.cached_config = Some(CachedTrainingConfig {
            run_id: cmd.run_id,
            model: cmd.model,
            learning_rate: cmd.learning_rate,
            batch_size: cmd.batch_size,
            epochs: cmd.epochs,
            dataset_uri: cmd.dataset_uri,
            checkpoint_storage_prefix: cmd.checkpoint_storage_prefix,
            checkpoint_interval_steps: cmd.checkpoint_interval_steps,
        });

        self.spawn_trainer(resume_path.as_deref());
    }

    /// Spawn the Python trainer subprocess.
    ///
    /// Pass `resume_from_checkpoint = Some(s3_path)` to resume from a
    /// previously saved checkpoint instead of starting from step 0.
    fn spawn_trainer(&mut self, resume_from_checkpoint: Option<&str>) {
        let cfg = match &self.cached_config {
            Some(c) => c,
            None => {
                eprintln!("Worker {}: spawn_trainer called with no cached config", self.worker_id);
                self.phase = WorkerPhase::Error;
                return;
            }
        };

        let mut py_cmd = tokio::process::Command::new("python");
        py_cmd
            .arg("python-trainer/main.py")
            .arg("--run-id").arg(&cfg.run_id)
            .arg("--worker-id").arg(&self.worker_id)
            .arg("--rank").arg(self.rank.to_string())
            .arg("--world-size").arg(self.world_size.to_string())
            .arg("--model").arg(&cfg.model)
            .arg("--learning-rate").arg(cfg.learning_rate.to_string())
            .arg("--batch-size").arg(cfg.batch_size.to_string())
            .arg("--epochs").arg(cfg.epochs.to_string())
            .arg("--dataset-uri").arg(&cfg.dataset_uri)
            .arg("--checkpoint-prefix").arg(&cfg.checkpoint_storage_prefix)
            .arg("--checkpoint-interval-steps").arg(cfg.checkpoint_interval_steps.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        if let Some(ckpt_path) = resume_from_checkpoint {
            py_cmd.arg("--resume-from-checkpoint").arg(ckpt_path);
        }

        let mode = if resume_from_checkpoint.is_some() { "restore" } else { "fresh" };

        match py_cmd.spawn() {
            Ok(child) => {
                println!(
                    "Worker {}: launched python trainer [{}] (pid={})",
                    self.worker_id, mode, child.id().unwrap_or(0),
                );
                self.training_child = Some(child);
                self.phase = if resume_from_checkpoint.is_some() {
                    WorkerPhase::Restoring
                } else {
                    WorkerPhase::Training
                };
                self.current_step = 0;
            }
            Err(e) => {
                eprintln!("Worker {}: failed to launch python trainer: {}", self.worker_id, e);
                self.phase = WorkerPhase::Error;
            }
        }
    }

    /// Read the checkpoint signal file written by the Python trainer.
    /// Returns persisted checkpoint info, or None if absent/unreadable.
    fn read_signal_file(path: &PathBuf) -> Option<PersistedCheckpointSignal> {
        let raw = std::fs::read_to_string(path).ok()?;
        let trimmed = raw.trim();

        if trimmed.starts_with('{') {
            let step = Self::extract_json_u64(trimmed, "step")?;
            let run_id = Self::extract_json_string(trimmed, "run_id");
            return Some(PersistedCheckpointSignal { run_id, step });
        }

        // Backward compatibility with old plain-step format.
        let step = trimmed.parse::<u64>().ok()?;
        Some(PersistedCheckpointSignal {
            run_id: None,
            step,
        })
    }

    fn extract_json_string(input: &str, key: &str) -> Option<String> {
        let needle = format!("\"{key}\"");
        let start = input.find(&needle)?;
        let after_key = &input[start + needle.len()..];
        let colon = after_key.find(':')?;
        let after_colon = after_key[colon + 1..].trim_start();
        if !after_colon.starts_with('"') {
            return None;
        }
        let rest = &after_colon[1..];
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    }

    fn extract_json_u64(input: &str, key: &str) -> Option<u64> {
        let needle = format!("\"{key}\"");
        let start = input.find(&needle)?;
        let after_key = &input[start + needle.len()..];
        let colon = after_key.find(':')?;
        let after_colon = after_key[colon + 1..].trim_start();
        let digits: String = after_colon
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if digits.is_empty() {
            return None;
        }
        digits.parse::<u64>().ok()
    }

    /// Poll the signal file every tick so that `last_checkpoint_id` stays
    /// up-to-date even while the Python trainer is still running.
    fn poll_checkpoint_signal(&mut self) {
        if let Some(signal) = Self::read_signal_file(&self.checkpoint_signal_path) {
            let signal_step = signal.step;
            let signal_run_id = signal.run_id;
            if self.last_checkpoint_id != Some(signal_step)
                || self.last_checkpoint_run_id.as_ref() != signal_run_id.as_ref()
            {
                println!(
                    "Worker {}: checkpoint signal updated → run_id={:?} step={}",
                    self.worker_id, signal_run_id, signal_step
                );
                self.last_checkpoint_id = Some(signal_step);
                self.last_checkpoint_run_id = signal_run_id;
            }
        }
    }

    fn simulate_work(&mut self) {
        match self.phase {
            WorkerPhase::Training => {
                self.current_step += 10;
                if self.current_step % 100 == 0 {
                    println!("Worker {} training: step {}", self.worker_id, self.current_step);
                }
            }
            WorkerPhase::Checkpointing => {
                println!("Worker {} completed checkpoint", self.worker_id);
                self.phase = WorkerPhase::Training;
            }
            _ => {}
        }
    }

    fn poll_training_process(&mut self) {
        if let Some(child) = &mut self.training_child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if status.success() {
                        println!("Worker {}: python trainer exited successfully", self.worker_id);
                        // last_checkpoint_id is already kept current by poll_checkpoint_signal;
                        // no need to increment a counter here.
                        // Use Paused as a terminal "done" state. If we report Idle here,
                        // coordinator interprets active->fresh as crash-recovery and can
                        // repeatedly issue RestoreCheckpoint after normal completion.
                        self.phase = WorkerPhase::Paused;
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
                    // Still running.
                    // Transition Restoring → Training once the process is confirmed alive.
                    if matches!(self.phase, WorkerPhase::Restoring) {
                        self.phase = WorkerPhase::Training;
                        println!("Worker {}: checkpoint loaded, resuming training", self.worker_id);
                    }
                }
                Err(e) => {
                    eprintln!("Worker {}: error checking python trainer: {}", self.worker_id, e);
                    self.training_child = None;
                    self.phase = WorkerPhase::Error;
                }
            }
        }
    }
}
