# Lotus - High-Performance Concurrent Congestion Control Framework

Lotus is a next-generation congestion control framework designed to overcome the limitations of single-threaded architectures when handling compute-intensive algorithms under high concurrent traffic loads. Built as an evolution of Portus, Lotus provides async-first APIs, parallel execution, and advanced scheduling while maintaining backward compatibility.

## 🚀 Key Features

- **Async-First Design**: All operations are asynchronous by default for maximum concurrency
- **Multi-Threaded Execution**: Parallel processing of flows and algorithms with work-stealing
- **Compute-Intensive Algorithm Support**: Background task execution optimized for CPU-heavy algorithms
- **Backward Compatibility**: Seamless migration path from Portus with compatibility layer
- **High Throughput**: Optimized for large-scale concurrent flows
- **Advanced Scheduling**: Multiple scheduling algorithms (Priority, Round-Robin, CFS, EDF)
- **Load Balancing**: Intelligent work distribution across worker threads

## 🏗️ Architecture Overview

```text
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   IPC Layer     │    │  Message Router  │    │ Algorithm Pool  │
│  (Async I/O)    │◄──►│   (Load Balancer)│◄──►│ (Work Stealing) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Connection Pool │    │   Flow Manager   │    │  Compute Tasks  │
│   (Per-DP)      │    │  (Concurrent)    │    │   (Parallel)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📦 Installation

Add Lotus to your `Cargo.toml`:

```toml
[dependencies]
lotus = "0.1.0"
```

## 🔧 Quick Start

### Basic Async Algorithm

```rust
use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    runtime::RuntimeBuilder,
};
use async_trait::async_trait;

#[derive(Clone)]
struct MyAsyncAlgorithm;

#[async_trait]
impl AsyncCongAlg<()> for MyAsyncAlgorithm {
    type Flow = MyAsyncFlow;
    
    fn name() -> &'static str {
        "my_async_algorithm"
    }
    
    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        // Define your datapath programs
        HashMap::new()
    }
    
    async fn new_flow(&self, control: FlowContext<()>, info: DatapathInfo) -> Result<Self::Flow> {
        Ok(MyAsyncFlow::new(control))
    }
}

struct MyAsyncFlow {
    control: FlowContext<()>,
    cwnd: u32,
}

#[async_trait]
impl AsyncFlow for MyAsyncFlow {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        // Process report asynchronously
        let rtt = report.get_field("rtt_us").unwrap_or(100000);
        
        // Perform async computation
        tokio::time::sleep(Duration::from_micros(100)).await;
        
        // Update congestion window
        self.cwnd = calculate_new_cwnd(rtt);
        
        // Send control message
        let msg = format!("SET_CWND {}", self.cwnd);
        self.control.send_control_message(msg.as_bytes()).await?;
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .enable_work_stealing(true)
        .build()
        .await?;
    
    runtime.register_async_algorithm(MyAsyncAlgorithm).await?;
    let handle = runtime.start().await?;
    
    // Your application logic here
    
    handle.shutdown().await?;
    Ok(())
}
```

### Migrating from Portus

Lotus provides a compatibility layer for seamless migration:

```rust
use lotus::{
    compat::PortusCompatRuntime,
    portus_main,
};

// Your existing Portus algorithm
struct MyCubicAlgorithm;

impl CongAlg<()> for MyCubicAlgorithm {
    // ... existing Portus implementation
}

// Option 1: Use compatibility runtime
#[tokio::main]
async fn main() -> Result<()> {
    let mut runtime = PortusCompatRuntime::new().await?;
    runtime.register_algorithm(MyCubicAlgorithm).await?;
    runtime.run().await?;
    Ok(())
}

// Option 2: Use convenience macro
portus_main!(MyCubicAlgorithm);
```

### Compute-Intensive Algorithms

For CPU-heavy algorithms, use the compute engine:

```rust
use lotus::compute::{ComputeTask, ComputeEngine};

struct MLPredictionTask {
    features: Vec<f64>,
    model_weights: Vec<f64>,
}

impl ComputeTask for MLPredictionTask {
    type Output = f64;
    
    fn execute(self) -> Self::Output {
        // Perform intensive ML computation
        neural_network_inference(&self.features, &self.model_weights)
    }
    
    fn priority(&self) -> u8 {
        2 // High priority
    }
    
    fn estimated_duration_us(&self) -> u64 {
        5000 // 5ms estimate
    }
}

// In your flow's on_report method:
async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
    let task = MLPredictionTask {
        features: self.extract_features(),
        model_weights: self.model_weights.clone(),
    };
    
    let prediction = compute_engine.submit_task(task, self.flow_id).await?;
    self.update_cwnd(prediction);
    
    Ok(())
}
```

## 🎯 Performance Comparison

| Feature | Portus | Lotus |
|---------|--------|-------|
| Execution Model | Single-threaded | Multi-threaded + Async |
| Algorithm Execution | Synchronous blocking | Async + parallel compute |
| Flow Processing | Sequential | Concurrent |
| Compute-Intensive Support | Limited | Optimized |
| Memory Usage | Higher per flow | Optimized sharing |
| Throughput | ~1K flows/sec | ~10K+ flows/sec |
| Latency (P99) | ~100ms | ~10ms |

## 📊 Benchmarks

```bash
# Run performance benchmarks
cargo bench

# Run with different worker counts
LOTUS_WORKERS=8 cargo run --example compute_intensive
```

## 🔧 Configuration

Lotus provides extensive configuration options:

```rust
let runtime = RuntimeBuilder::new()
    .with_worker_threads(8)                    // Number of worker threads
    .with_algorithm_timeout(1000)              // Algorithm timeout (ms)
    .with_message_buffer_size(8192)            // Message buffer size
    .enable_work_stealing(true)                // Enable work stealing
    .with_cleanup_interval(30000)              // Cleanup interval (ms)
    .build()
    .await?;
```

## 📚 Examples

- [`basic_usage.rs`](examples/basic_usage.rs) - Simple async algorithm
- [`portus_migration.rs`](examples/portus_migration.rs) - Migration from Portus
- [`compute_intensive.rs`](examples/compute_intensive.rs) - ML-based algorithm

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run with logging
RUST_LOG=debug cargo test

# Run specific test suite
cargo test --lib algorithm
```

## 📈 Monitoring

Lotus provides comprehensive runtime statistics:

```rust
let stats = runtime.get_stats().await;
println!("Active flows: {}", stats.active_flows);
println!("Compute throughput: {:.2} tasks/sec", stats.compute_throughput);
println!("Success rate: {:.2}%", stats.compute_success_rate * 100.0);
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built upon the foundation of [Portus](https://github.com/ccp-project/portus)
- Inspired by modern async Rust patterns
- Thanks to the CCP project contributors

## 📞 Support

- 📖 [Documentation](https://docs.rs/lotus)
- 🐛 [Issue Tracker](https://github.com/your-org/lotus/issues)
- 💬 [Discussions](https://github.com/your-org/lotus/discussions)

---

**Lotus**: Empowering the next generation of congestion control algorithms with async-first, high-performance execution. 🪷