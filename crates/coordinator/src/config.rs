use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct JobConfig {
	run_id: String,
	world_size: i64,
	training: Training,
	data: Data,
	checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Training {
    model: String,
    learning_rate: f64,
	batch_size: i64,
	epochs: i64,
	seed: i64
}

#[derive(Debug, Clone, Deserialize)]
pub struct Data {
	dataset_url: String,
	sharding: String,
	shuffle: bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct Checkpoint {
	interval_steps: i64,
	storage_prefix: String,
	retain_last: i64,
}

pub struct JobConfiguration {
	pub config: JobConfig
}

impl JobConfiguration {
	pub fn parse_job(job_name: &str) -> Result<JobConfiguration, Box<dyn std::error::Error>> {
		let config: JobConfig = toml::from_str(job_name)?;
		println!("run_id: {}", config.run_id);
		Ok(JobConfiguration { config })
	}

	pub fn run_id(&self) -> &str {
		&self.config.run_id
	}

	pub fn world_size(&self) -> u32 {
		self.config.world_size as u32
	}

	pub fn seed(&self) -> u64 {
		self.config.training.seed as u64
	}

    pub fn model(&self) -> &str {
        &self.config.training.model
    }

    pub fn learning_rate(&self) -> f64 {
        self.config.training.learning_rate
    }

    pub fn batch_size(&self) -> u64 {
        self.config.training.batch_size as u64
    }

    pub fn epochs(&self) -> u64 {
        self.config.training.epochs as u64
    }

    /// Returns the dataset URI (currently stored as `dataset_url` in the job file).
    pub fn dataset_uri(&self) -> &str {
        &self.config.data.dataset_url
    }

    pub fn checkpoint_storage_prefix(&self) -> &str {
        &self.config.checkpoint.storage_prefix
    }

    pub fn checkpoint_interval_steps(&self) -> u64 {
        self.config.checkpoint.interval_steps as u64
    }
}
