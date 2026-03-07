use common::{
    distributed_trainer::{NoOp, RestoreCheckpoint, StartTraining},
    orchestrator_command::CommandType,
    OrchestratorCommand,
    WorkerPhase,
    WorkerStateSnapshot,
};
use crate::config::JobConfiguration;
use std::collections::HashMap;

/// Per-worker bookkeeping held by the coordinator.
struct WorkerRecord {
    /// Phase the worker was in when we last heard from it.
    last_phase: WorkerPhase,
    /// Most recent checkpoint id reported by the worker.
    last_checkpoint_id: Option<u64>,
    /// True while we have issued a RestoreCheckpoint that the worker has not
    /// yet acknowledged by re-entering Training.
    restore_issued: bool,
}

pub struct JobController {
    job_config: JobConfiguration,
    next_command_id: u64,
    worker_records: HashMap<String, WorkerRecord>,
}

impl JobController {
    pub fn new(config: JobConfiguration) -> Self {
        Self {
            job_config: config,
            next_command_id: 1,
            worker_records: HashMap::new(),
        }
    }

    pub fn compute_action(&mut self, status: WorkerStateSnapshot) -> OrchestratorCommand {
        let command_type = self.reconcile(status);
        let command_id = self.next_command_id;
        self.next_command_id += 1;

        OrchestratorCommand {
            command_id,
            issued_at: None,
            command_type: Some(command_type),
        }
    }

    fn reconcile(&mut self, status: WorkerStateSnapshot) -> CommandType {
        let worker_id = status.worker_id.clone();
        let phase = WorkerPhase::try_from(status.phase).unwrap_or(WorkerPhase::Unspecified);

        // A "fresh" phase means the worker has (re)started and is waiting for work.
        let is_fresh = matches!(
            phase,
            WorkerPhase::Starting | WorkerPhase::Idle | WorkerPhase::Unspecified
        );

        // ── First contact: start training from scratch. ──────────────────────
        if !self.worker_records.contains_key(&worker_id) {
            self.worker_records.insert(
                worker_id.clone(),
                WorkerRecord {
                    last_phase: phase,
                    last_checkpoint_id: status.last_checkpoint_id,
                    restore_issued: false,
                },
            );
            println!("Coordinator: new worker {worker_id}, issuing StartTraining");
            return CommandType::StartTraining(self.build_start_training(worker_id));
        }

        // ── Existing worker: decide what to do. ──────────────────────────────
        enum Action {
            Restore(u64),
            StartFresh,
            NoOp,
        }

        let action = {
            let record = self.worker_records.get_mut(&worker_id).unwrap();

            // Keep the most recent checkpoint id the worker has reported.
            if status.last_checkpoint_id.is_some() {
                record.last_checkpoint_id = status.last_checkpoint_id;
            }

            // Phases that indicate the worker was actively doing something
            // before it went silent / crashed.
            let was_active = matches!(
                record.last_phase,
                WorkerPhase::Training
                    | WorkerPhase::Checkpointing
                    | WorkerPhase::Restoring
                    | WorkerPhase::Error
            );

            let action = if was_active && is_fresh && !record.restore_issued {
                // Crash-and-recovery detected: the worker was active, it has
                // come back in a fresh state.
                if let Some(ckpt_id) = record.last_checkpoint_id {
                    record.restore_issued = true;
                    Action::Restore(ckpt_id)
                } else {
                    // No checkpoint to restore from – restart from scratch.
                    Action::StartFresh
                }
            } else if record.restore_issued
                && matches!(phase, WorkerPhase::Training | WorkerPhase::Paused)
            {
                // Restore is complete — worker made it back to Training,
                // or immediately finished and moved to Paused.
                record.restore_issued = false;
                Action::NoOp
            } else if record.restore_issued && is_fresh {
                // We issued a RestoreCheckpoint but the worker is still in a
                // fresh state, meaning it couldn't process the restore (most
                // likely it had no cached training config after restarting).
                // Fall back to StartTraining so the worker gets its config.
                record.restore_issued = false;
                Action::StartFresh
            } else {
                Action::NoOp
            };

            record.last_phase = phase;
            action
        };

        // Borrow on worker_records is released; safe to use job_config now.
        match action {
            Action::Restore(checkpoint_id) => {
                let checkpoint_path = self.build_checkpoint_path(checkpoint_id);
                println!(
                    "Coordinator: crash recovery for worker {worker_id} — \
                     restoring checkpoint {checkpoint_id} from {checkpoint_path}"
                );
                CommandType::RestoreCheckpoint(RestoreCheckpoint {
                    checkpoint_id,
                    checkpoint_path: Some(checkpoint_path),
                })
            }
            Action::StartFresh => {
                println!(
                    "Coordinator: crash recovery for worker {worker_id} — \
                     no checkpoint available, re-issuing StartTraining"
                );
                CommandType::StartTraining(self.build_start_training(worker_id))
            }
            Action::NoOp => CommandType::NoOp(NoOp {}),
        }
    }

    fn build_start_training(&self, worker_id: String) -> StartTraining {
        StartTraining {
            worker_id,
            world_size: self.job_config.world_size(),
            seed: self.job_config.seed(),
            target_step: None,
            run_id: self.job_config.run_id().to_string(),
            model: self.job_config.model().to_string(),
            learning_rate: self.job_config.learning_rate(),
            batch_size: self.job_config.batch_size(),
            epochs: self.job_config.epochs(),
            dataset_uri: self.job_config.dataset_uri().to_string(),
            checkpoint_storage_prefix: self.job_config.checkpoint_storage_prefix().to_string(),
            checkpoint_interval_steps: self.job_config.checkpoint_interval_steps(),
        }
    }

    /// Constructs the S3 path where a checkpoint for `checkpoint_id` lives.
    /// Layout: `{storage_prefix}/{run_id}/checkpoint-{checkpoint_id}`
    fn build_checkpoint_path(&self, checkpoint_id: u64) -> String {
        format!(
            "{}/{}/checkpoint-{}",
            self.job_config.checkpoint_storage_prefix().trim_end_matches('/'),
            self.job_config.run_id(),
            checkpoint_id,
        )
    }
}
