mod agent;

use agent::WorkerAgent;
use clap::Parser;

#[derive(Parser)]
#[command(name = "worker-agent")]
struct Args {
    #[arg(long)]
    id: String,
    
    #[arg(long)]
    rank: u32,
    
    #[arg(long, default_value = "3")]
    world_size: u32,
    
    #[arg(long, default_value = "http://[::1]:50051")]
    orchestrator: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    println!("Starting worker: {} (rank {}/{})", args.id, args.rank, args.world_size);
    
    let mut worker = WorkerAgent::new(
        args.id,
        args.rank,
        args.world_size,
        &args.orchestrator,
    ).await?;
    
    worker.run().await?;
    
    Ok(())
}
