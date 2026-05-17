use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use lotus::{
    algorithm::{AsyncCongAlg, CongAlg, DatapathInfo, Report},
    compat::PortusCompatRuntime,
    flow::{AsyncFlow, Flow, FlowContext},
    runtime::RuntimeBuilder,
};
use std::collections::HashMap;
use tokio::runtime::Runtime;

// Simple sync algorithm for comparison
#[derive(Clone, Default)]
struct SimpleSyncAlgorithm;

impl CongAlg<()> for SimpleSyncAlgorithm {
    fn name(&self) -> &'static str {
        "simple_sync"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }

    fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn Flow> {
        Box::new(SimpleSyncFlow {
            cwnd: 10,
            reports: 0,
        })
    }
}

struct SimpleSyncFlow {
    cwnd: u32,
    reports: u32,
}

impl Flow for SimpleSyncFlow {
    fn on_report(&mut self, _sock_id: u32, report: Report) {
        self.reports += 1;
        let rtt = report.get_field("rtt_us").unwrap_or(100000);

        // Simple AIMD
        if rtt > 200000 {
            self.cwnd = (self.cwnd / 2).max(1);
        } else {
            self.cwnd += 1;
        }

        // Simulate some computation
        for _ in 0..1000 {
            black_box(self.cwnd * 2);
        }
    }
}

// Simple async algorithm
#[derive(Clone, Default)]
struct SimpleAsyncAlgorithm;

#[async_trait]
impl AsyncCongAlg<()> for SimpleAsyncAlgorithm {
    fn name(&self) -> &'static str {
        "simple_async"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        _info: DatapathInfo,
    ) -> lotus::Result<Box<dyn AsyncFlow>> {
        Ok(Box::new(SimpleAsyncFlow {
            _control: control,
            cwnd: 10,
            reports: 0,
        }))
    }
}

struct SimpleAsyncFlow {
    _control: FlowContext<()>,
    cwnd: u32,
    reports: u32,
}

#[async_trait]
impl AsyncFlow for SimpleAsyncFlow {
    async fn on_report(&mut self, _sock_id: u32, report: Report) -> lotus::Result<()> {
        self.reports += 1;
        let rtt = report.get_field("rtt_us").unwrap_or(100000);

        // Simple AIMD
        if rtt > 200000 {
            self.cwnd = (self.cwnd / 2).max(1);
        } else {
            self.cwnd += 1;
        }

        // Simulate async computation
        tokio::task::yield_now().await;

        // Simulate some computation
        for _ in 0..1000 {
            black_box(self.cwnd * 2);
        }

        Ok(())
    }
}

fn create_test_report(rtt_us: u64) -> Report {
    let mut report = Report {
        fields: HashMap::new(),
        timestamp: std::time::Instant::now(),
    };
    report.set_field("rtt_us".to_string(), rtt_us);
    report.set_field("cwnd".to_string(), 10);
    report
}

fn create_test_datapath_info(sock_id: u32) -> DatapathInfo {
    DatapathInfo {
        sock_id,
        init_cwnd: 10,
        mss: 1460,
        src_ip: 0x7F000001,
        src_port: 8080,
        dst_ip: 0x7F000001,
        dst_port: 80,
        programs: std::collections::HashMap::new(),
        scopes: std::collections::HashMap::new(),
        report_fields: std::collections::HashMap::new(),
    }
}

fn bench_sync_algorithm(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("sync_algorithm_single_flow", |b| {
        b.to_async(&rt).iter(|| async {
            let mut compat_runtime = PortusCompatRuntime::new().await.unwrap();
            compat_runtime
                .register_algorithm(SimpleSyncAlgorithm)
                .await
                .unwrap();

            let datapath_info = create_test_datapath_info(1);
            let flow_id = compat_runtime
                .lotus_runtime()
                .create_flow(1, "simple_sync", datapath_info)
                .await
                .unwrap();

            // Process 100 reports
            for i in 0..100 {
                let report = create_test_report(50000 + i * 1000);
                compat_runtime
                    .lotus_runtime()
                    .handle_report(flow_id, 1, report)
                    .await
                    .unwrap();
            }

            compat_runtime
                .lotus_runtime()
                .close_flow(flow_id)
                .await
                .unwrap();
        });
    });
}

fn bench_async_algorithm(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("async_algorithm_single_flow", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = RuntimeBuilder::new()
                .with_worker_threads(1)
                .build()
                .await
                .unwrap();

            runtime
                .register_async_algorithm(SimpleAsyncAlgorithm)
                .await
                .unwrap();

            let datapath_info = create_test_datapath_info(1);
            let flow_id = runtime
                .create_flow(1, "simple_async", datapath_info)
                .await
                .unwrap();

            // Process 100 reports
            for i in 0..100 {
                let report = create_test_report(50000 + i * 1000);
                runtime.handle_report(flow_id, 1, report).await.unwrap();
            }

            runtime.close_flow(flow_id).await.unwrap();
        });
    });
}

fn bench_concurrent_flows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_flows");

    for flow_count in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("async", flow_count),
            flow_count,
            |b, &flow_count| {
                b.to_async(&rt).iter(|| async move {
                    let runtime = RuntimeBuilder::new()
                        .with_worker_threads(4)
                        .build()
                        .await
                        .unwrap();

                    runtime
                        .register_async_algorithm(SimpleAsyncAlgorithm)
                        .await
                        .unwrap();

                    let mut flow_ids = Vec::new();

                    // Create flows
                    for i in 0..flow_count {
                        let datapath_info = create_test_datapath_info(i as u32 + 1);
                        let flow_id = runtime
                            .create_flow(i as u32 + 1, "simple_async", datapath_info)
                            .await
                            .unwrap();
                        flow_ids.push(flow_id);
                    }

                    // Process reports concurrently
                    let mut tasks = Vec::new();
                    for (i, &flow_id) in flow_ids.iter().enumerate() {
                        let runtime_ref = &runtime;
                        let task = async move {
                            for j in 0..10 {
                                let report = create_test_report(50000 + j * 1000);
                                runtime_ref
                                    .handle_report(flow_id, i as u32 + 1, report)
                                    .await
                                    .unwrap();
                            }
                        };
                        tasks.push(task);
                    }

                    futures::future::join_all(tasks).await;

                    // Close flows
                    for flow_id in flow_ids {
                        runtime.close_flow(flow_id).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_compute_intensive(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("compute_intensive_algorithm", |b| {
        b.to_async(&rt).iter(|| async {
            let runtime = RuntimeBuilder::new()
                .with_worker_threads(4)
                .enable_work_stealing(true)
                .build()
                .await
                .unwrap();

            runtime
                .register_async_algorithm(ComputeIntensiveAlgorithm)
                .await
                .unwrap();

            let datapath_info = create_test_datapath_info(1);
            let flow_id = runtime
                .create_flow(1, "compute_intensive", datapath_info)
                .await
                .unwrap();

            // Process reports with heavy computation
            for i in 0..20 {
                let report = create_test_report(50000 + i * 1000);
                runtime.handle_report(flow_id, 1, report).await.unwrap();
            }

            runtime.close_flow(flow_id).await.unwrap();
        });
    });
}

// Compute-intensive algorithm for benchmarking
#[derive(Clone, Default)]
struct ComputeIntensiveAlgorithm;

#[async_trait]
impl AsyncCongAlg<()> for ComputeIntensiveAlgorithm {
    fn name(&self) -> &'static str {
        "compute_intensive"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        _info: DatapathInfo,
    ) -> lotus::Result<Box<dyn AsyncFlow>> {
        Ok(Box::new(ComputeIntensiveFlow {
            _control: control,
            cwnd: 10,
        }))
    }
}

struct ComputeIntensiveFlow {
    _control: FlowContext<()>,
    cwnd: u32,
}

#[async_trait]
impl AsyncFlow for ComputeIntensiveFlow {
    async fn on_report(&mut self, _sock_id: u32, report: Report) -> lotus::Result<()> {
        let rtt = report.get_field("rtt_us").unwrap_or(100000);

        // Simulate heavy computation in background task
        let computation_result = tokio::task::spawn_blocking(move || {
            // Simulate complex mathematical computation
            let mut result = 0.0f64;
            for i in 0..10000 {
                result += (i as f64).sin().cos().tan();
            }

            // Simple congestion control based on RTT
            if rtt > 200000 {
                result * 0.7 // Decrease
            } else {
                result + 1.0 // Increase
            }
        })
        .await
        .unwrap();

        self.cwnd = (computation_result as u32).max(1).min(1000);

        Ok(())
    }
}

criterion_group!(
    benches,
    bench_sync_algorithm,
    bench_async_algorithm,
    bench_concurrent_flows,
    bench_compute_intensive
);
criterion_main!(benches);
