use core::str;
use serde::{Deserialize, Serialize};

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
		println!("run_id")
		Ok(JobConfiguration { config })
	}
}