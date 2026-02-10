mod config;
mod client;

use std::fs;
use std::env;
use crate::config::JobConfiguration;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let args: Vec<String> = env::args().collect();
	println!("{}", args.join(" "));
    let config_str = fs::read_to_string("sample_job.toml")?;	
	let job_config: JobConfiguration = config::JobConfiguration::parse_job(&config_str).expect("something happened");
	
	// JobController Needs to get set
	todo!("Implement the Job Controller");
	// Boot gRPC Server
	client::boot_coordinator().await?;
	
    Ok(())
}