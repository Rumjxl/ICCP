#!/usr/bin/env cargo +nightly -Zscript
//! Lotus Framework Demo
//! 
//! This script demonstrates the key features and performance improvements
//! of the Lotus congestion control framework compared to Portus.
//! 
//! Run with: cargo run --bin demo

use lotus::{
    algorithm::{AsyncCongAlg, CongAlg, DatapathInfo, Report},
    compat::{CompatDatapath, PortusCompatRuntime},
    flow::{AsyncFlow, Flow, FlowContext},
    runtime::RuntimeBuilder,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Demo async algorithm that simulates compute-intensive processing
#[derive(Clone)]
struct DemoAsyncAlgorithm {
    name: String,
    processing_time_us: u64,
    reports_processed: Arc<AtomicU64>,
}

impl DemoAsyncAlgorithm {
    fn new(name: &str, processing_time_us: u64) -> Self {
        Self {
            name: name.to_string(),
            processing_time_us,
            reports_processed: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn get_reports_processed(&self) -> u64 {
        self.reports_processed.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AsyncCongAlg<()> for DemoAsyncAlgorithm {
    type Flow = DemoAsyncFlow;
    
    fn name() -> &'static str {
        "demo_async"
    }
    
    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }
    
    async fn new_flow(&self, control: FlowContext<()>, info: DatapathInfo) -> lotus::Result<Self::Flow> {
        info!(
            algorithm = %self.name,
            sock_id = info.sock_id,
            "Creating async flow"
        );
        Ok(DemoAsyncFlow::new(control, self.processing_time_us, self.reports_processed.clone()))
    }
}

struct DemoAsyncFlow {
    _control: FlowContext<()>,
    processing_time_us: u64,
    reports_processed: Arc<AtomicU64>,
    cwnd: u32,
}

impl DemoAsyncFlow {
    fn new(control: FlowContext<()>, processing_time_us: u64, reports_processed: Arc<AtomicU64>) -> Self {
        Self {
            _control: control,
            processing_time_us,
            reports_processed,
            cwnd: 10,
        }
    }
}

#[async_trait]
impl AsyncFlow for DemoAsyncFlow {
    async fn on_report(&mut self, _sock_id: u32, report: Report) -> lotus::Result<()> {
        // Simulate compute-intensive processing
        if self.processing_time_us > 0 {
            tokio::time::sleep(Duration::from_micros(self.processing_time_us)).await;
        }
        
        // Simple congestion control logic
        let rtt = report.get_field("rtt_us").unwrap_or(100000);
        if rtt > 200000 {
            self.cwnd = (self.cwnd / 2).max(1);
        } else {
            self.cwnd += 1;
        }
        
        self.reports_processed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Demo sync algorithm for comparison
#[derive(Clone)]
struct DemoSyncAlgorithm {
    name: String,
    processing_time_us: u64,
    reports_processed: Arc<AtomicU64>,
}

impl DemoSyncAlgorithm {
    fn new(name: &str, processing_time_us: u64) -> Self {
        Self {
            name: name.to_string(),
            processing_time_us,
            reports_processed: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn get_reports_processed(&self) -> u64 {
        self.reports_processed.load(Ordering::Relaxed)
    }
}

impl CongAlg<()> for DemoSyncAlgorithm {
    type Flow = DemoSyncFlow;
    
    fn name() -> &'static str {
        "demo_sync"
    }
    
    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }
    
    fn new_flow(&self, _control: CompatDatapath<()>, info: DatapathInfo) -> Self::Flow {
        info!(
            algorithm = %self.name,
            sock_id = info.sock_id,
            "Creating sync flow"
        );
        DemoSyncFlow::new(self.processing_time_us, self.reports_processed.clone())
    }
}

struct DemoSyncFlow {
    processing_time_us: u64,
    reports_processed: Arc<AtomicU64>,
    cwnd: u32,
}

impl DemoSyncFlow {
    fn new(processing_time_us: u64, reports_processed: Arc<AtomicU64>) -> Self {
        Self {
            processing_time_us,
            reports_processed,
            cwnd: 10,
        }
    }
}

impl Flow for DemoSyncFlow {
    fn on_report(&mut self, _sock_id: u32, report: Report) {
        // Simulate compute-intensive processing (blocking)
        if self.processing_time_us > 0 {
            std::thread::sleep(Duration::from_micros(self.processing_time_us));
        }
        
        // Simple congestion control logic
        let rtt = report.get_field("rtt_us").unwrap_or(100000);
        if rtt > 200000 {
            self.cwnd = (self.cwnd / 2).max(1);
        } else {
            self.cwnd += 1;
        }
        
        self.reports_processed.fetch_add(1, Ordering::Relaxed);
    }
}

fn create_demo_report(rtt_us: u64) -> Report {
    let mut report = Report {
        fields: HashMap::new(),
        timestamp: Instant::now(),
    };
    report.set_field("rtt_us".to_string(), rtt_us);
    report.set_field("cwnd".to_string(), 10);
    report
}

fn create_demo_datapath_info(sock_id: u32) -> DatapathInfo {
    DatapathInfo {
        sock_id,
        init_cwnd: 10,
        mss: 1460,
        src_ip: 0x7F000001,
        src_port: 8080 + sock_id as u16,
        dst_ip: 0x7F000001,
        dst_port: 80,
        programs: std::collections::HashMap::new(),
        scopes: std::collections::HashMap::new(),
        report_fields: std::collections::HashMap::new(),
    }
}

async fn demo_async_performance() -> lotus::Result<()> {
    println!("\n🚀 === Lotus Async Algorithm Demo ===");
    
    let algorithm = DemoAsyncAlgorithm::new("FastAsync", 1000); // 1ms processing time
    
    let mut runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .enable_work_stealing(true)
        .build()
        .await?;
    
    runtime.register_async_algorithm(algorithm.clone()).await?;
    
    let start_time = Instant::now();
    let mut flow_ids = Vec::new();
    
    // Create multiple concurrent flows
    for i in 0..10 {
        let datapath_info = create_demo_datapath_info(i + 1);
        let flow_id = runtime.create_flow(i + 1, "demo_async", datapath_info).await?;
        flow_ids.push(flow_id);
    }
    
    // Send reports to all flows concurrently
    let mut tasks = Vec::new();
    for (i, &flow_id) in flow_ids.iter().enumerate() {
        let runtime_ref = &runtime;
        let task = async move {
            for j in 0..20 {
                let rtt = 50000 + (j % 5) * 10000; // Varying RTT
                let report = create_demo_report(rtt);
                if let Err(e) = runtime_ref.handle_report(flow_id, i as u32 + 1, report).await {
                    warn!(flow = i, error = %e, "Failed to handle report");
                }
            }
        };
        tasks.push(task);
    }
    
    futures::future::join_all(tasks).await;
    
    // Close flows
    for flow_id in flow_ids {
        runtime.close_flow(flow_id).await?;
    }
    
    let duration = start_time.elapsed();
    let total_reports = algorithm.get_reports_processed();
    let throughput = total_reports as f64 / duration.as_secs_f64();
    
    println!("✅ Async Demo Results:");
    println!("   • Flows: 10");
    println!("   • Reports per flow: 20");
    println!("   • Total reports: {}", total_reports);
    println!("   • Duration: {:.2}s", duration.as_secs_f64());
    println!("   • Throughput: {:.0} reports/sec", throughput);
    
    let stats = runtime.get_stats().await;
    println!("   • Compute success rate: {:.1}%", stats.compute_success_rate * 100.0);
    
    Ok(())
}

async fn demo_sync_performance() -> lotus::Result<()> {
    println!("\n📊 === Portus Sync Algorithm Demo (for comparison) ===");
    
    let algorithm = DemoSyncAlgorithm::new("SlowSync", 1000); // 1ms processing time
    
    let mut compat_runtime = PortusCompatRuntime::new().await?;
    compat_runtime.register_algorithm(algorithm.clone()).await?;
    
    let start_time = Instant::now();
    let mut flow_ids = Vec::new();
    
    // Create flows (but they'll be processed sequentially due to sync nature)
    for i in 0..10 {
        let datapath_info = create_demo_datapath_info(i + 1);
        let flow_id = compat_runtime.lotus_runtime()
            .create_flow(i + 1, "demo_sync", datapath_info)
            .await?;
        flow_ids.push(flow_id);
    }
    
    // Send reports (will be processed sequentially)
    for (i, &flow_id) in flow_ids.iter().enumerate() {
        for j in 0..20 {
            let rtt = 50000 + (j % 5) * 10000;
            let report = create_demo_report(rtt);
            if let Err(e) = compat_runtime.lotus_runtime()
                .handle_report(flow_id, i as u32 + 1, report)
                .await {
                warn!(flow = i, error = %e, "Failed to handle report");
            }
        }
    }
    
    // Close flows
    for flow_id in flow_ids {
        compat_runtime.lotus_runtime().close_flow(flow_id).await?;
    }
    
    let duration = start_time.elapsed();
    let total_reports = algorithm.get_reports_processed();
    let throughput = total_reports as f64 / duration.as_secs_f64();
    
    println!("✅ Sync Demo Results:");
    println!("   • Flows: 10");
    println!("   • Reports per flow: 20");
    println!("   • Total reports: {}", total_reports);
    println!("   • Duration: {:.2}s", duration.as_secs_f64());
    println!("   • Throughput: {:.0} reports/sec", throughput);
    
    Ok(())
}

async fn demo_scalability() -> lotus::Result<()> {
    println!("\n📈 === Scalability Demo ===");
    
    let flow_counts = [1, 10, 50, 100];
    
    for &flow_count in &flow_counts {
        let algorithm = DemoAsyncAlgorithm::new("ScalabilityTest", 500); // 0.5ms processing
        
        let mut runtime = RuntimeBuilder::new()
            .with_worker_threads(8)
            .enable_work_stealing(true)
            .build()
            .await?;
        
        runtime.register_async_algorithm(algorithm.clone()).await?;
        
        let start_time = Instant::now();
        let mut flow_ids = Vec::new();
        
        // Create flows
        for i in 0..flow_count {
            let datapath_info = create_demo_datapath_info(i + 1);
            let flow_id = runtime.create_flow(i + 1, "demo_async", datapath_info).await?;
            flow_ids.push(flow_id);
        }
        
        // Process reports concurrently
        let mut tasks = Vec::new();
        for (i, &flow_id) in flow_ids.iter().enumerate() {
            let runtime_ref = &runtime;
            let task = async move {
                for j in 0..10 {
                    let rtt = 50000 + (j % 3) * 20000;
                    let report = create_demo_report(rtt);
                    runtime_ref.handle_report(flow_id, i as u32 + 1, report).await.ok();
                }
            };
            tasks.push(task);
        }
        
        futures::future::join_all(tasks).await;
        
        // Close flows
        for flow_id in flow_ids {
            runtime.close_flow(flow_id).await?;
        }
        
        let duration = start_time.elapsed();
        let total_reports = algorithm.get_reports_processed();
        let throughput = total_reports as f64 / duration.as_secs_f64();
        
        println!("   📊 {} flows: {:.0} reports/sec ({:.2}s)", 
                 flow_count, throughput, duration.as_secs_f64());
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    println!("🪷 Welcome to the Lotus Congestion Control Framework Demo!");
    println!("This demo showcases the performance improvements over traditional approaches.");
    
    // Run async performance demo
    if let Err(e) = demo_async_performance().await {
        eprintln!("Async demo failed: {}", e);
    }
    
    // Run sync performance demo for comparison
    if let Err(e) = demo_sync_performance().await {
        eprintln!("Sync demo failed: {}", e);
    }
    
    // Run scalability demo
    if let Err(e) = demo_scalability().await {
        eprintln!("Scalability demo failed: {}", e);
    }
    
    println!("\n🎉 Demo completed! Key takeaways:");
    println!("   • Lotus async algorithms can process reports concurrently");
    println!("   • Work-stealing enables better CPU utilization");
    println!("   • Backward compatibility allows gradual migration from Portus");
    println!("   • Scalability improves significantly with concurrent flows");
    println!("\n📚 Check out the examples/ directory for more detailed usage patterns!");
    
    Ok(())
}