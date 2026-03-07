mod config;
mod client;
mod controller;

use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::config::JobConfiguration;
use crate::controller::JobController;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "coordinator")]
struct Args {
    /// Path to the job configuration TOML file.
    #[arg(long, default_value = "sample_job.toml")]
    job: String,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Starting coordinator with job config: {}", args.job);

    let config_str = fs::read_to_string(&args.job)?;	
	let job_config: JobConfiguration = config::JobConfiguration::parse_job(&config_str).expect("something happened");
	let job_controller = Arc::new(Mutex::new(JobController::new(job_config)));
	// Boot gRPC Server
	client::boot_coordinator(job_controller).await?;
    Ok(())
}
