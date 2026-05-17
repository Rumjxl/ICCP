//! Basic usage example of the Lotus framework
//!
//! This example demonstrates how to create a simple congestion control
//! algorithm using Lotus's async-first API.

use async_trait::async_trait;
use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    runtime::RuntimeBuilder,
    Result,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

/// A simple async congestion control algorithm
#[derive(Clone)]
struct SimpleAsyncAlgorithm {
    initial_cwnd: u32,
}

impl SimpleAsyncAlgorithm {
    fn new(initial_cwnd: u32) -> Self {
        Self { initial_cwnd }
    }
}

#[async_trait]
impl AsyncCongAlg<()> for SimpleAsyncAlgorithm {
    fn name(&self) -> &'static str {
        "simple_async"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut programs = HashMap::new();
        programs.insert(
            "simple_program",
            r#"
            (def (Report
                (volatile cwnd 0)
                (volatile rtt_us 0)
            ))
            (when true
                (:= Report.cwnd Flow.cwnd)
                (:= Report.rtt_us Flow.rtt_sample_us)
            )
            (when (> Micros 10000)
                (report)
                (reset)
            )
        "#
            .to_string(),
        );
        programs
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        info: DatapathInfo,
    ) -> Result<Box<dyn AsyncFlow>> {
        info!(
            sock_id = info.sock_id,
            src_ip = info.src_ip,
            src_port = info.src_port,
            dst_ip = info.dst_ip,
            dst_port = info.dst_port,
            "Creating new async flow"
        );

        Ok(Box::new(SimpleAsyncFlow::new(control, self.initial_cwnd)))
    }

    async fn initialize(&mut self) -> Result<()> {
        info!("Initializing SimpleAsyncAlgorithm");
        Ok(())
    }
}

/// A simple async flow implementation
struct SimpleAsyncFlow {
    control: FlowContext<()>,
    cwnd: u32,
    ssthresh: u32,
    rtt_samples: Vec<u64>,
}

impl SimpleAsyncFlow {
    fn new(control: FlowContext<()>, initial_cwnd: u32) -> Self {
        Self {
            control,
            cwnd: initial_cwnd,
            ssthresh: 65535,
            rtt_samples: Vec::new(),
        }
    }

    async fn update_cwnd(&mut self, rtt_us: u64) -> Result<()> {
        // Simple AIMD algorithm
        if self.cwnd < self.ssthresh {
            // Slow start: exponential increase
            self.cwnd *= 2;
            debug!(cwnd = self.cwnd, "Slow start: doubled cwnd");
        } else {
            // Congestion avoidance: linear increase
            self.cwnd += 1;
            debug!(cwnd = self.cwnd, "Congestion avoidance: incremented cwnd");
        }

        // Track RTT samples for future use
        self.rtt_samples.push(rtt_us);
        if self.rtt_samples.len() > 100 {
            self.rtt_samples.remove(0);
        }

        // Send control message to datapath (simulated)
        let control_msg = format!("SET_CWND {}", self.cwnd);
        self.control
            .send_control_message(control_msg.as_bytes())
            .await?;

        Ok(())
    }

    async fn handle_congestion(&mut self) -> Result<()> {
        // Multiplicative decrease
        self.ssthresh = self.cwnd / 2;
        self.cwnd = self.ssthresh;

        info!(
            cwnd = self.cwnd,
            ssthresh = self.ssthresh,
            "Congestion detected: reduced cwnd"
        );

        Ok(())
    }
}

#[async_trait]
impl AsyncFlow for SimpleAsyncFlow {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        debug!(sock_id = sock_id, "Received measurement report");

        // Extract measurements from report
        let cwnd = report.get_field("cwnd").unwrap_or(self.cwnd as u64) as u32;
        let rtt_us = report.get_field("rtt_us").unwrap_or(100000); // Default 100ms

        debug!(
            current_cwnd = self.cwnd,
            reported_cwnd = cwnd,
            rtt_us = rtt_us,
            "Processing report"
        );

        // Simulate some async computation (e.g., ML inference, complex calculations)
        tokio::time::sleep(Duration::from_micros(100)).await;

        // Update congestion window based on RTT
        if rtt_us > 200000 {
            // > 200ms indicates congestion
            self.handle_congestion().await?;
        } else {
            self.update_cwnd(rtt_us).await?;
        }

        // Update flow state
        self.control
            .update_state(|state| {
                state.report_count += 1;
                state.last_report_at = Some(std::time::Instant::now());
            })
            .await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        info!(
            final_cwnd = self.cwnd,
            total_rtt_samples = self.rtt_samples.len(),
            "Closing async flow"
        );

        // Cleanup resources
        self.rtt_samples.clear();
        Ok(())
    }

    async fn initialize(&mut self) -> Result<()> {
        info!(initial_cwnd = self.cwnd, "Initializing async flow");

        // Send initial control message
        let init_msg = format!("INIT_CWND {}", self.cwnd);
        self.control
            .send_control_message(init_msg.as_bytes())
            .await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting Lotus basic usage example");

    // Create and configure the runtime
    let mut runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .with_algorithm_timeout(1000) // 1 second timeout
        .with_message_buffer_size(8192)
        .enable_work_stealing(true)
        .build()
        .await?;

    // Register our async algorithm
    let algorithm = SimpleAsyncAlgorithm::new(10);
    runtime.register_async_algorithm(algorithm).await?;

    // Start the runtime
    let handle = runtime.start().await?;

    // Simulate creating some flows and processing reports
    tokio::spawn(async move {
        // Wait a bit for runtime to fully start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a test flow
        let datapath_info = DatapathInfo {
            sock_id: 1,
            init_cwnd: 10,
            mss: 1460,
            src_ip: 0x7F000001, // 127.0.0.1
            src_port: 8080,
            dst_ip: 0x7F000001, // 127.0.0.1
            dst_port: 80,
            programs: std::collections::HashMap::new(),
            scopes: std::collections::HashMap::new(),
            report_fields: std::collections::HashMap::new(),
        };

        match runtime.create_flow(1, "simple_async", datapath_info).await {
            Ok(flow_id) => {
                info!(flow_id = %flow_id, "Created test flow");

                // Simulate some measurement reports
                for i in 0..10 {
                    let mut report = Report {
                        fields: std::collections::HashMap::new(),
                        timestamp: std::time::Instant::now(),
                    };

                    report.set_field("cwnd".to_string(), 10 + i);
                    report.set_field("rtt_us".to_string(), 50000 + i * 10000); // Increasing RTT

                    if let Err(e) = runtime.handle_report(flow_id, 1, report).await {
                        eprintln!("Failed to handle report: {}", e);
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Close the flow
                if let Err(e) = runtime.close_flow(flow_id).await {
                    eprintln!("Failed to close flow: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to create flow: {}", e);
            }
        }

        // Print runtime statistics
        let stats = runtime.get_stats().await;
        info!(
            active_flows = stats.active_flows,
            registered_algorithms = stats.registered_algorithms,
            compute_throughput = stats.compute_throughput,
            "Runtime statistics"
        );
    });

    // Run for a few seconds then shutdown
    tokio::time::sleep(Duration::from_secs(3)).await;

    info!("Shutting down runtime");
    handle.shutdown().await?;

    info!("Example completed successfully");
    Ok(())
}
