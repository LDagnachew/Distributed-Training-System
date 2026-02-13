// Include the generated proto code
// This generates a module named after your package in the proto file
pub mod distributed_trainer {
    tonic::include_proto!("distributed_trainer");
}

// Re-export everything from the generated module for convenience
pub use distributed_trainer::*;