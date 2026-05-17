//! Example showing how to migrate from Portus to Lotus
//!
//! This example demonstrates the compatibility layer that allows
//! existing Portus algorithms to work with Lotus.

use lotus::{
    algorithm::{CongAlg, DatapathInfo, Report},
    compat::{helpers, PortusCompatRuntime},
    flow::{Flow, FlowContext},
};
use std::collections::HashMap;
use tracing::{debug, info};

/// A traditional Portus-style algorithm
#[derive(Clone, Default)]
struct TraditionalCubic {
    beta: f64,
    c: f64,
}

impl TraditionalCubic {
    fn new() -> Self {
        Self {
            beta: 0.7, // Multiplicative decrease factor
            c: 0.4,    // Cubic scaling constant
        }
    }
}

impl CongAlg<()> for TraditionalCubic {
    fn name(&self) -> &'static str {
        "traditional_cubic"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut programs = HashMap::new();
        programs.insert(
            "cubic_program",
            r#"
            (def (Report
                (volatile cwnd 0)
                (volatile rtt_us 0)
                (volatile loss_detected 0)
                (volatile bytes_acked 0)
            ))
            (when true
                (:= Report.cwnd Flow.cwnd)
                (:= Report.rtt_us Flow.rtt_sample_us)
                (:= Report.bytes_acked Flow.bytes_acked)
            )
            (when Flow.loss_detected
                (:= Report.loss_detected 1)
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

    fn new_flow(&self, _control: FlowContext<()>, info: DatapathInfo) -> Box<dyn Flow> {
        info!(sock_id = info.sock_id, "Creating traditional Cubic flow");

        Box::new(CubicFlow::new(self.beta, self.c))
    }
}

/// Traditional Portus-style flow implementation
struct CubicFlow {
    // Cubic algorithm state
    cwnd: f64,
    ssthresh: f64,
    beta: f64,
    c: f64,
    w_max: f64,
    k: f64,
    w_tcp: f64,
    origin_point: std::time::Instant,

    // Statistics
    reports_received: u32,
    losses_detected: u32,
}

impl CubicFlow {
    fn new(beta: f64, c: f64) -> Self {
        let now = std::time::Instant::now();
        Self {
            cwnd: 10.0,
            ssthresh: f64::INFINITY,
            beta,
            c,
            w_max: 0.0,
            k: 0.0,
            w_tcp: 0.0,
            origin_point: now,
            reports_received: 0,
            losses_detected: 0,
        }
    }

    fn handle_loss(&mut self) {
        debug!(
            old_cwnd = self.cwnd,
            old_ssthresh = self.ssthresh,
            "Handling loss event"
        );

        self.losses_detected += 1;

        // Cubic multiplicative decrease
        self.w_max = self.cwnd;
        self.cwnd = self.cwnd * self.beta;
        self.ssthresh = self.cwnd;

        // Calculate K (time to reach w_max again)
        self.k = ((self.w_max - self.cwnd) / self.c).powf(1.0 / 3.0);
        self.origin_point = std::time::Instant::now();

        info!(
            new_cwnd = self.cwnd,
            new_ssthresh = self.ssthresh,
            w_max = self.w_max,
            k = self.k,
            "Loss handled"
        );
    }

    fn handle_ack(&mut self, rtt_us: u64, bytes_acked: u64) {
        let rtt_s = rtt_us as f64 / 1_000_000.0;
        let t = self.origin_point.elapsed().as_secs_f64();

        // Cubic function: W_cubic(t) = C * (t - K)^3 + W_max
        let w_cubic = self.c * (t - self.k).powi(3) + self.w_max;

        // TCP-friendly rate
        self.w_tcp += (bytes_acked as f64) / (self.cwnd * rtt_s);

        // Use the more aggressive of cubic or TCP-friendly
        let target_cwnd = if w_cubic > self.w_tcp {
            w_cubic
        } else {
            self.w_tcp
        };

        // Gradual increase towards target
        if target_cwnd > self.cwnd {
            let increase = (target_cwnd - self.cwnd) / self.cwnd;
            self.cwnd += increase.min(1.0); // Cap increase per RTT
        }

        // Ensure minimum cwnd
        self.cwnd = self.cwnd.max(1.0);

        debug!(
            cwnd = self.cwnd,
            w_cubic = w_cubic,
            w_tcp = self.w_tcp,
            target = target_cwnd,
            t = t,
            "Updated cwnd"
        );
    }
}

impl Flow for CubicFlow {
    fn on_report(&mut self, sock_id: u32, report: Report) {
        self.reports_received += 1;

        debug!(
            sock_id = sock_id,
            report_num = self.reports_received,
            "Received report"
        );

        // Extract measurements
        let reported_cwnd = report.get_field("cwnd").unwrap_or(self.cwnd as u64);
        let rtt_us = report.get_field("rtt_us").unwrap_or(100000); // Default 100ms
        let loss_detected = report.get_field("loss_detected").unwrap_or(0) > 0;
        let bytes_acked = report.get_field("bytes_acked").unwrap_or(1460);

        debug!(
            reported_cwnd = reported_cwnd,
            current_cwnd = self.cwnd,
            rtt_us = rtt_us,
            loss_detected = loss_detected,
            bytes_acked = bytes_acked,
            "Report details"
        );

        // Handle loss if detected
        if loss_detected {
            self.handle_loss();
        } else {
            // Normal ACK processing
            self.handle_ack(rtt_us, bytes_acked);
        }

        // Periodic logging
        if self.reports_received % 10 == 0 {
            info!(
                reports = self.reports_received,
                losses = self.losses_detected,
                cwnd = self.cwnd,
                ssthresh = self.ssthresh,
                "Cubic flow statistics"
            );
        }
    }

    fn close(&mut self) {
        info!(
            total_reports = self.reports_received,
            total_losses = self.losses_detected,
            final_cwnd = self.cwnd,
            "Closing Cubic flow"
        );
    }
}

/// Alternative main function using the compatibility runtime
async fn run_with_compat_runtime() -> lotus::Result<()> {
    info!("Starting Portus migration example with compatibility runtime");

    let mut runtime = PortusCompatRuntime::new().await?;
    runtime.register_algorithm(TraditionalCubic::new()).await?;

    // Get access to the underlying Lotus runtime for advanced features
    let lotus_runtime = runtime.lotus_runtime();

    // Create a test flow to demonstrate the algorithm
    let datapath_info = helpers::create_datapath_info(
        1,
        helpers::ipv4_to_u32("192.168.1.100").unwrap(),
        8080,
        helpers::ipv4_to_u32("192.168.1.1").unwrap(),
        80,
    );

    match lotus_runtime
        .create_flow(1, "traditional_cubic", datapath_info)
        .await
    {
        Ok(flow_id) => {
            info!(flow_id = %flow_id, "Created test flow");

            // Simulate normal operation
            for i in 0..20u64 {
                let report = helpers::create_report(&[
                    ("cwnd", 10 + i),
                    ("rtt_us", 50000 + (i % 5) * 10000),
                    ("bytes_acked", 1460),
                    ("loss_detected", if i == 10 { 1 } else { 0 }), // Simulate loss at report 10
                ]);

                if let Err(e) = lotus_runtime.handle_report(flow_id, 1, report).await {
                    eprintln!("Failed to handle report: {}", e);
                }

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }

            // Close the flow
            if let Err(e) = lotus_runtime.close_flow(flow_id).await {
                eprintln!("Failed to close flow: {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to create flow: {}", e);
        }
    }

    // Print final statistics
    let stats = lotus_runtime.get_stats().await;
    info!(
        active_flows = stats.active_flows,
        compute_throughput = stats.compute_throughput,
        "Final runtime statistics"
    );

    info!("Migration example completed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Run the compatibility example
    run_with_compat_runtime().await?;

    Ok(())
}

// Alternative: Use the portus_main! macro for even easier migration
// Uncomment the following to use the macro instead:

/*
// This would be the entire main function for a simple Portus migration:
portus_main!(TraditionalCubic::new());
*/
