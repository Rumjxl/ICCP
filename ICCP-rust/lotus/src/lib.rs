//! Lotus - High-Performance Concurrent Congestion Control Framework
//!
//! Lotus is a next-generation congestion control framework designed to overcome
//! the limitations of single-threaded architectures when handling compute-intensive
//! algorithms under high concurrent traffic loads.
//!
//! ## Key Features
//!
//! - **Async-first design**: All operations are asynchronous by default
//! - **Multi-threaded execution**: Parallel processing of flows and algorithms
//! - **Compute-intensive algorithm support**: Background task execution with work-stealing
//! - **Backward compatibility**: Maintains Portus-compatible interfaces
//! - **High throughput**: Optimized for large-scale concurrent flows
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
//! │   IPC Layer     │    │  Message Router  │    │ Algorithm Pool  │
//! │  (Async I/O)    │◄──►│   (Load Balancer)│◄──►│ (Work Stealing) │
//! └─────────────────┘    └──────────────────┘    └─────────────────┘
//!           │                       │                       │
//!           ▼                       ▼                       ▼
//! ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
//! │ Connection Pool │    │   Flow Manager   │    │  Compute Tasks  │
//! │   (Per-DP)      │    │  (Concurrent)    │    │   (Parallel)    │
//! └─────────────────┘    └──────────────────┘    └─────────────────┘
//! ```

pub mod algorithm;
pub mod compute;
pub mod datapath_listener;
pub mod flow;
pub mod ipc;
pub mod ipc_netlink;
pub mod manager;
pub mod portus_datapath;
pub mod runtime;
pub mod scheduler;
pub mod serialize;

// Re-export core types for compatibility
pub use algorithm::{AsyncCongAlg, CongAlg, LotusAlgorithm};
pub use flow::{AsyncFlow, Flow, FlowContext, FlowId, FlowManager, FlowState};
pub use ipc::{AsyncIpc, IpcMessage, MessageRouter};
pub use manager::AlgorithmManager;
pub use runtime::{LotusRuntime, RuntimeConfig, RuntimeHandle};

// Compatibility layer for Portus
pub mod compat;
pub use compat::*;

use tracing::info;

/// Core error type for Lotus framework
#[derive(Debug, thiserror::Error)]
pub enum LotusError {
    #[error("IPC error: {0}")]
    Ipc(String),
    #[error("Algorithm error: {0}")]
    Algorithm(String),
    #[error("Flow error: {0}")]
    Flow(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, LotusError>;

/// Global configuration for the Lotus runtime
#[derive(Debug, Clone)]
pub struct LotusConfig {
    /// Number of worker threads for algorithm execution
    pub worker_threads: usize,
    /// Size of the message buffer per connection
    pub message_buffer_size: usize,
    /// Maximum number of concurrent flows per worker
    pub max_flows_per_worker: usize,
    /// Enable work-stealing between workers
    pub enable_work_stealing: bool,
    /// Timeout for algorithm execution (milliseconds)
    pub algorithm_timeout_ms: u64,
    /// Enable flow affinity (pin flows to specific workers)
    pub enable_flow_affinity: bool,
}

impl Default for LotusConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus::get(),
            message_buffer_size: 8192,
            max_flows_per_worker: 1000,
            enable_work_stealing: true,
            algorithm_timeout_ms: 100,
            enable_flow_affinity: true,
        }
    }
}

/// Main entry point for creating a Lotus runtime
pub async fn create_runtime(config: LotusConfig) -> Result<LotusRuntime> {
    info!("Initializing Lotus runtime with config: {:?}", config);
    LotusRuntime::new(config).await
}

/// Convenience macro for defining algorithms with both sync and async support
#[macro_export]
macro_rules! lotus_algorithm {
    ($name:ident, $sync_impl:ty, $async_impl:ty) => {
        pub struct $name;

        impl $crate::LotusAlgorithm for $name {
            type SyncImpl = $sync_impl;
            type AsyncImpl = $async_impl;

            fn name(&self) -> &'static str {
                stringify!($name)
            }
        }
    };
}
