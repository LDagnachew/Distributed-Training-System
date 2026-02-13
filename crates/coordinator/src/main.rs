mod config;
mod client;
mod controller;

use std::fs;
use std::env;
use crate::config::JobConfiguration;
use crate::controller::JobController;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let args: Vec<String> = env::args().collect();
	println!("{}", args.join(" "));
    let config_str = fs::read_to_string("sample_job.toml")?;	
	let job_config: JobConfiguration = config::JobConfiguration::parse_job(&config_str).expect("something happened");
	let _job_controller: JobController = controller::JobController::new(job_config);
	// Boot gRPC Server
	client::boot_coordinator().await?;
    Ok(())
}