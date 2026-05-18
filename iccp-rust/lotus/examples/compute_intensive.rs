//! Example demonstrating compute-intensive algorithm execution
//!
//! This example shows how Lotus handles CPU-intensive congestion control
//! algorithms using parallel execution and work-stealing.

use async_trait::async_trait;
use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    compute::{ComputeTask, ParallelAlgorithm},
    flow::{AsyncFlow, FlowContext, FlowId},
    runtime::RuntimeBuilder,
    Result,
};
use rayon::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// A compute-intensive machine learning-based congestion control algorithm
#[derive(Clone)]
struct MLCongestionControl {
    model_weights: Vec<f64>,
    learning_rate: f64,
}

impl MLCongestionControl {
    fn new() -> Self {
        // Initialize with random weights for a simple neural network
        let model_weights = (0..100).map(|_| rand::random::<f64>() - 0.5).collect();

        Self {
            model_weights,
            learning_rate: 0.01,
        }
    }
}

#[async_trait]
impl AsyncCongAlg<()> for MLCongestionControl {
    fn name(&self) -> &'static str {
        "ml_congestion_control"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut programs = HashMap::new();
        programs.insert(
            "ml_program",
            r#"
            (def (Report
                (volatile cwnd 0)
                (volatile rtt_us 0)
                (volatile loss_rate 0)
                (volatile throughput_mbps 0)
                (volatile queue_delay_us 0)
                (volatile bytes_in_flight 0)
            ))
            (when true
                (:= Report.cwnd Flow.cwnd)
                (:= Report.rtt_us Flow.rtt_sample_us)
                (:= Report.throughput_mbps (/ (* Flow.rate_outgoing 8) 1000000))
                (:= Report.bytes_in_flight Flow.bytes_in_flight)
            )
            (when (> Micros 5000)  ; Report every 5ms for ML algorithm
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
        info!(sock_id = info.sock_id, "Creating ML-based flow");

        Ok(Box::new(MLFlow::new(
            control,
            self.model_weights.clone(),
            self.learning_rate,
        )))
    }
}

/// ML-based flow that performs intensive computations
struct MLFlow {
    control: FlowContext<()>,
    model_weights: Vec<f64>,
    learning_rate: f64,

    // Network state history for ML features
    rtt_history: Vec<u64>,
    cwnd_history: Vec<u32>,
    loss_history: Vec<bool>,
    throughput_history: Vec<f64>,

    // Current state
    current_cwnd: u32,
    prediction_cache: Option<(Instant, f64)>,

    // Statistics
    predictions_made: u64,
    training_iterations: u64,
    total_compute_time: Duration,
}

impl MLFlow {
    fn new(control: FlowContext<()>, model_weights: Vec<f64>, learning_rate: f64) -> Self {
        Self {
            control,
            model_weights,
            learning_rate,
            rtt_history: Vec::with_capacity(1000),
            cwnd_history: Vec::with_capacity(1000),
            loss_history: Vec::with_capacity(1000),
            throughput_history: Vec::with_capacity(1000),
            current_cwnd: 10,
            prediction_cache: None,
            predictions_made: 0,
            training_iterations: 0,
            total_compute_time: Duration::ZERO,
        }
    }

    /// Extract features from network state history
    fn extract_features(&self) -> Vec<f64> {
        let mut features = Vec::with_capacity(20);

        // RTT statistics
        if !self.rtt_history.is_empty() {
            let recent_rtt: Vec<_> = self.rtt_history.iter().rev().take(10).collect();
            let avg_rtt =
                recent_rtt.iter().map(|&&x| x as f64).sum::<f64>() / recent_rtt.len() as f64;
            let min_rtt = recent_rtt
                .iter()
                .map(|&&x| x as f64)
                .fold(f64::INFINITY, f64::min);
            let max_rtt = recent_rtt.iter().map(|&&x| x as f64).fold(0.0, f64::max);
            let rtt_variance = recent_rtt
                .iter()
                .map(|&&x| (x as f64 - avg_rtt).powi(2))
                .sum::<f64>()
                / recent_rtt.len() as f64;

            features.extend_from_slice(&[avg_rtt, min_rtt, max_rtt, rtt_variance]);
        } else {
            features.extend_from_slice(&[100000.0, 100000.0, 100000.0, 0.0]);
        }

        // Throughput statistics
        if !self.throughput_history.is_empty() {
            let recent_throughput: Vec<_> = self.throughput_history.iter().rev().take(10).collect();
            let avg_throughput =
                recent_throughput.iter().map(|&&x| x).sum::<f64>() / recent_throughput.len() as f64;
            let throughput_trend = if recent_throughput.len() >= 2 {
                recent_throughput[0] - recent_throughput[recent_throughput.len() - 1]
            } else {
                0.0
            };

            features.extend_from_slice(&[avg_throughput, throughput_trend]);
        } else {
            features.extend_from_slice(&[0.0, 0.0]);
        }

        // Loss rate
        let recent_losses = self.loss_history.iter().rev().take(20).count();
        let loss_rate = recent_losses as f64 / 20.0;
        features.push(loss_rate);

        // Current cwnd
        features.push(self.current_cwnd as f64);

        // Pad to fixed size
        while features.len() < 20 {
            features.push(0.0);
        }

        features.truncate(20);
        features
    }

    /// Compute-intensive neural network prediction
    async fn predict_optimal_cwnd(&mut self) -> Result<f64> {
        let start_time = Instant::now();

        // Check cache first
        if let Some((cache_time, cached_prediction)) = self.prediction_cache {
            if cache_time.elapsed() < Duration::from_millis(10) {
                return Ok(cached_prediction);
            }
        }

        let features = self.extract_features();

        // Create a compute task for the prediction
        let prediction_task = MLPredictionTask {
            features: features.clone(),
            weights: self.model_weights.clone(),
            flow_id: self.control.flow_id,
        };

        // This would normally use the compute engine, but for the example we'll simulate it
        let prediction = tokio::task::spawn_blocking(move || Box::new(prediction_task).execute())
            .await
            .map_err(|e| lotus::LotusError::Runtime(format!("Prediction task failed: {}", e)))?;

        let compute_time = start_time.elapsed();
        self.total_compute_time += compute_time;
        self.predictions_made += 1;

        // Cache the result
        self.prediction_cache = Some((Instant::now(), prediction));

        debug!(
            prediction = prediction,
            compute_time_us = compute_time.as_micros(),
            "ML prediction completed"
        );

        Ok(prediction)
    }

    /// Train the model using recent experience
    async fn train_model(&mut self, target_cwnd: f64) -> Result<()> {
        if self.rtt_history.len() < 10 {
            return Ok(());
        }

        let start_time = Instant::now();

        let features = self.extract_features();

        // Create training task
        let training_task = MLTrainingTask {
            features,
            target: target_cwnd,
            weights: self.model_weights.clone(),
            learning_rate: self.learning_rate,
            flow_id: self.control.flow_id,
        };

        // Execute training in background
        let updated_weights =
            tokio::task::spawn_blocking(move || Box::new(training_task).execute())
                .await
                .map_err(|e| lotus::LotusError::Runtime(format!("Training task failed: {}", e)))?;

        self.model_weights = updated_weights;
        self.training_iterations += 1;

        let training_time = start_time.elapsed();
        self.total_compute_time += training_time;

        debug!(
            training_time_us = training_time.as_micros(),
            iterations = self.training_iterations,
            "Model training completed"
        );

        Ok(())
    }

    /// Update network state history
    fn update_history(&mut self, rtt_us: u64, throughput_mbps: f64, loss_detected: bool) {
        // Maintain sliding windows
        self.rtt_history.push(rtt_us);
        if self.rtt_history.len() > 1000 {
            self.rtt_history.remove(0);
        }

        self.cwnd_history.push(self.current_cwnd);
        if self.cwnd_history.len() > 1000 {
            self.cwnd_history.remove(0);
        }

        self.throughput_history.push(throughput_mbps);
        if self.throughput_history.len() > 1000 {
            self.throughput_history.remove(0);
        }

        self.loss_history.push(loss_detected);
        if self.loss_history.len() > 1000 {
            self.loss_history.remove(0);
        }
    }
}

#[async_trait]
impl AsyncFlow for MLFlow {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        debug!(sock_id = sock_id, "Processing ML report");

        // Extract measurements
        let rtt_us = report.get_field("rtt_us").unwrap_or(100000);
        let throughput_mbps = report.get_field("throughput_mbps").unwrap_or(0) as f64;
        let loss_rate = report.get_field("loss_rate").unwrap_or(0) as f64;
        let loss_detected = loss_rate > 0.01; // 1% loss threshold

        // Update history
        self.update_history(rtt_us, throughput_mbps, loss_detected);

        // Predict optimal congestion window
        let predicted_cwnd = self.predict_optimal_cwnd().await?;

        // Apply prediction with bounds checking
        let new_cwnd = if loss_detected {
            // Conservative decrease on loss
            (self.current_cwnd as f64 * 0.7).max(1.0)
        } else {
            // Use ML prediction but bound it reasonably
            predicted_cwnd.max(1.0).min(self.current_cwnd as f64 * 2.0)
        };

        let old_cwnd = self.current_cwnd;
        self.current_cwnd = new_cwnd as u32;

        // Train model with the actual outcome
        if self.predictions_made > 10 {
            // Use throughput as training signal
            let target_cwnd = if throughput_mbps > 0.0 {
                // If we got good throughput, the cwnd was probably good
                old_cwnd as f64
            } else {
                // Poor throughput, maybe we should have used different cwnd
                (old_cwnd as f64 * 0.9).max(1.0)
            };

            self.train_model(target_cwnd).await?;
        }

        // Send control message
        let control_msg = format!("SET_CWND {}", self.current_cwnd);
        self.control
            .send_control_message(control_msg.as_bytes())
            .await?;

        // Periodic statistics
        if self.predictions_made % 100 == 0 {
            info!(
                predictions = self.predictions_made,
                training_iterations = self.training_iterations,
                avg_compute_time_us =
                    self.total_compute_time.as_micros() / self.predictions_made.max(1) as u128,
                current_cwnd = self.current_cwnd,
                "ML flow statistics"
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        info!(
            total_predictions = self.predictions_made,
            total_training = self.training_iterations,
            total_compute_time_ms = self.total_compute_time.as_millis(),
            final_cwnd = self.current_cwnd,
            "Closing ML flow"
        );
        Ok(())
    }
}

/// Compute task for ML prediction
struct MLPredictionTask {
    features: Vec<f64>,
    weights: Vec<f64>,
    flow_id: FlowId,
}

impl ComputeTask for MLPredictionTask {
    type Output = f64;

    fn execute(self: Box<Self>) -> Self::Output {
        // Simulate a neural network forward pass
        // This is computationally intensive with matrix operations

        // Simple feedforward network: input -> hidden -> output
        let input_size = self.features.len();
        let hidden_size = 50;
        let output_size = 1;

        // Input to hidden layer
        let mut hidden: Vec<f64> = vec![0.0; hidden_size];
        for i in 0..hidden_size {
            let mut sum = 0.0;
            for j in 0..input_size {
                let weight_idx = i * input_size + j;
                if weight_idx < self.weights.len() {
                    sum += self.features[j] * self.weights[weight_idx];
                }
            }
            hidden[i] = sum.tanh(); // Activation function
        }

        // Hidden to output layer
        let mut output = 0.0;
        let output_weights_start = input_size * hidden_size;
        for i in 0..hidden_size {
            let weight_idx = output_weights_start + i;
            if weight_idx < self.weights.len() {
                output += hidden[i] * self.weights[weight_idx];
            }
        }

        // Apply sigmoid to get positive output
        let prediction = 1.0 / (1.0 + (-output).exp());

        // Scale to reasonable cwnd range (1-1000)
        prediction * 999.0 + 1.0
    }

    fn priority(&self) -> u8 {
        2 // High priority for predictions
    }

    fn estimated_duration_us(&self) -> u64 {
        5000 // Estimate 5ms for neural network inference
    }
}

/// Compute task for ML training
struct MLTrainingTask {
    features: Vec<f64>,
    target: f64,
    weights: Vec<f64>,
    learning_rate: f64,
    flow_id: FlowId,
}

impl ComputeTask for MLTrainingTask {
    type Output = Vec<f64>;

    fn execute(mut self: Box<Self>) -> Self::Output {
        // Simulate backpropagation training
        // This is very compute-intensive

        let input_size = self.features.len();
        let hidden_size = 50;

        // Forward pass (same as prediction)
        let mut hidden: Vec<f64> = vec![0.0; hidden_size];
        for i in 0..hidden_size {
            let mut sum = 0.0;
            for j in 0..input_size {
                let weight_idx = i * input_size + j;
                if weight_idx < self.weights.len() {
                    sum += self.features[j] * self.weights[weight_idx];
                }
            }
            hidden[i] = sum.tanh();
        }

        let mut output = 0.0;
        let output_weights_start = input_size * hidden_size;
        for i in 0..hidden_size {
            let weight_idx = output_weights_start + i;
            if weight_idx < self.weights.len() {
                output += hidden[i] * self.weights[weight_idx];
            }
        }

        let prediction = 1.0 / (1.0 + (-output).exp()) * 999.0 + 1.0;

        // Backward pass (gradient descent)
        let error = self.target - prediction;

        // Update weights (simplified gradient descent)
        for i in 0..self.weights.len() {
            let gradient = error * self.learning_rate * 0.001; // Simplified gradient
            self.weights[i] += gradient;
        }

        self.weights
    }

    fn priority(&self) -> u8 {
        1 // Normal priority for training
    }

    fn estimated_duration_us(&self) -> u64 {
        10000 // Estimate 10ms for training
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting compute-intensive algorithm example");

    // Create runtime optimized for compute-intensive workloads
    let mut runtime = RuntimeBuilder::new()
        .with_worker_threads(8) // More workers for parallel computation
        .with_algorithm_timeout(5000) // Longer timeout for ML algorithms
        .enable_work_stealing(true) // Enable work stealing for load balancing
        .build()
        .await?;

    // Register the ML algorithm
    let ml_algorithm = MLCongestionControl::new();
    runtime.register_async_algorithm(ml_algorithm).await?;

    // Start the runtime
    let _handle = runtime.start().await?;

    // Simulate multiple concurrent flows with compute-intensive processing sequentially
    for flow_num in 0u64..5 {
        let datapath_info = DatapathInfo {
            sock_id: flow_num as u32 + 1,
            init_cwnd: 10,
            mss: 1460,
            src_ip: 0xC0A80100 + flow_num as u32, // 192.168.1.x
            src_port: 8080 + flow_num as u16,
            dst_ip: 0xC0A80101,
            dst_port: 80,
            programs: std::collections::HashMap::new(),
            scopes: std::collections::HashMap::new(),
            report_fields: std::collections::HashMap::new(),
        };

        match runtime
            .create_flow(flow_num as u32 + 1, "ml_congestion_control", datapath_info)
            .await
        {
            Ok(flow_id) => {
                info!(flow_num = flow_num, flow_id = %flow_id, "Created ML flow");

                // Simulate varying network conditions
                for report_num in 0..50u64 {
                    let mut report = Report {
                        fields: HashMap::new(),
                        timestamp: Instant::now(),
                    };

                    // Simulate varying RTT and throughput
                    let base_rtt: u64 = 50000 + (report_num % 10) * 5000;
                    let rtt_us = base_rtt;

                    let throughput = 100.0 + (report_num as f64 * 2.0) % 50.0;
                    let loss_rate: f64 = if report_num % 20 == 0 { 0.02 } else { 0.0 }; // Occasional loss

                    report.set_field("rtt_us".to_string(), rtt_us);
                    report.set_field("throughput_mbps".to_string(), throughput as u64);
                    report.set_field("loss_rate".to_string(), (loss_rate * 1000.0) as u64);
                    report.set_field("cwnd".to_string(), 10 + report_num);

                    if let Err(e) = runtime
                        .handle_report(flow_id, flow_num as u32 + 1, report)
                        .await
                    {
                        warn!(flow_num = flow_num, error = %e, "Failed to handle report");
                    }
                }

                // Close the flow
                if let Err(e) = runtime.close_flow(flow_id).await {
                    warn!(flow_num = flow_num, error = %e, "Failed to close flow");
                }

                info!(flow_num = flow_num, "Completed ML flow simulation");
            }
            Err(e) => {
                warn!(flow_num = flow_num, error = %e, "Failed to create ML flow");
            }
        }
    }

    // Print final statistics
    let stats = runtime.get_stats().await;
    info!(
        active_flows = stats.active_flows,
        compute_throughput = stats.compute_throughput,
        compute_success_rate = stats.compute_success_rate,
        total_tasks_submitted = stats.total_tasks_submitted,
        total_tasks_completed = stats.total_tasks_completed,
        "Final compute-intensive example statistics"
    );

    // Shutdown
    info!("Shutting down compute-intensive example");

    info!("Compute-intensive example completed successfully");
    Ok(())
}
