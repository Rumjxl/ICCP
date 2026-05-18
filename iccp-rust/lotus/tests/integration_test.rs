use async_trait::async_trait;
use lotus::{
    algorithm::{AsyncCongAlg, CongAlg, DatapathInfo, Report},
    compat::PortusCompatRuntime,
    flow::{AsyncFlow, Flow, FlowContext},
    runtime::RuntimeBuilder,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct TestAsyncAlgorithm {
    counter: Arc<AtomicU32>,
}

impl TestAsyncAlgorithm {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }
    fn get_count(&self) -> u32 {
        self.counter.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AsyncCongAlg<()> for TestAsyncAlgorithm {
    fn name(&self) -> &'static str {
        "test_async"
    }
    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut programs = HashMap::new();
        programs.insert("test_program", "test program".to_string());
        programs
    }
    async fn new_flow(
        &self,
        control: FlowContext<()>,
        _info: DatapathInfo,
    ) -> lotus::Result<Box<dyn AsyncFlow>> {
        Ok(Box::new(TestAsyncFlow::new(control, self.counter.clone())))
    }
}

struct TestAsyncFlow {
    _control: FlowContext<()>,
    counter: Arc<AtomicU32>,
    reports_received: u32,
}

impl TestAsyncFlow {
    fn new(control: FlowContext<()>, counter: Arc<AtomicU32>) -> Self {
        Self {
            _control: control,
            counter,
            reports_received: 0,
        }
    }
}

#[async_trait]
impl AsyncFlow for TestAsyncFlow {
    async fn on_report(&mut self, _sock_id: u32, _report: Report) -> lotus::Result<()> {
        self.reports_received += 1;
        self.counter.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }
    async fn close(&mut self) -> lotus::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct TestSyncAlgorithm {
    counter: Arc<AtomicU32>,
}

impl TestSyncAlgorithm {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }
    fn get_count(&self) -> u32 {
        self.counter.load(Ordering::Relaxed)
    }
}

impl CongAlg<()> for TestSyncAlgorithm {
    fn name(&self) -> &'static str {
        "test_sync"
    }
    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut programs = HashMap::new();
        programs.insert("test_program", "test program".to_string());
        programs
    }
    fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn Flow> {
        Box::new(TestSyncFlow::new(self.counter.clone()))
    }
}

struct TestSyncFlow {
    counter: Arc<AtomicU32>,
    reports_received: u32,
}

impl TestSyncFlow {
    fn new(counter: Arc<AtomicU32>) -> Self {
        Self {
            counter,
            reports_received: 0,
        }
    }
}

impl Flow for TestSyncFlow {
    fn on_report(&mut self, _sock_id: u32, _report: Report) {
        self.reports_received += 1;
        self.counter.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn create_test_report() -> Report {
    let mut report = Report {
        fields: HashMap::new(),
        timestamp: std::time::Instant::now(),
    };
    report.set_field("rtt_us".to_string(), 100000);
    report.set_field("cwnd".to_string(), 10);
    report
}

fn create_test_datapath_info(sock_id: u32) -> DatapathInfo {
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

#[tokio::test]
async fn test_async_algorithm_basic() {
    let algorithm = TestAsyncAlgorithm::new();
    let initial_count = algorithm.get_count();
    let runtime = RuntimeBuilder::new()
        .with_worker_threads(2)
        .build()
        .await
        .unwrap();
    runtime
        .register_async_algorithm(algorithm.clone())
        .await
        .unwrap();
    let datapath_info = create_test_datapath_info(1);
    let flow_id = runtime
        .create_flow(1, "test_async", datapath_info)
        .await
        .unwrap();
    for _ in 0..5 {
        let report = create_test_report();
        runtime.handle_report(flow_id, 1, report).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime.close_flow(flow_id).await.unwrap();
    assert!(algorithm.get_count() > initial_count);
    assert_eq!(algorithm.get_count(), initial_count + 5);
}

#[tokio::test]
async fn test_sync_algorithm_compatibility() {
    let algorithm = TestSyncAlgorithm::new();
    let initial_count = algorithm.get_count();
    let mut compat_runtime = PortusCompatRuntime::new().await.unwrap();
    compat_runtime
        .register_algorithm(algorithm.clone())
        .await
        .unwrap();
    let datapath_info = create_test_datapath_info(1);
    let flow_id = compat_runtime
        .lotus_runtime()
        .create_flow(1, "test_sync", datapath_info)
        .await
        .unwrap();
    for _ in 0..3 {
        let report = create_test_report();
        compat_runtime
            .lotus_runtime()
            .handle_report(flow_id, 1, report)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    compat_runtime
        .lotus_runtime()
        .close_flow(flow_id)
        .await
        .unwrap();
    assert!(algorithm.get_count() > initial_count);
    assert_eq!(algorithm.get_count(), initial_count + 3);
}

#[tokio::test]
async fn test_concurrent_flows() {
    let algorithm = TestAsyncAlgorithm::new();
    let initial_count = algorithm.get_count();
    let runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .build()
        .await
        .unwrap();
    runtime
        .register_async_algorithm(algorithm.clone())
        .await
        .unwrap();
    let mut flow_ids = Vec::new();
    for i in 0..5u32 {
        let datapath_info = create_test_datapath_info(i + 1);
        let flow_id = runtime
            .create_flow(i + 1, "test_async", datapath_info)
            .await
            .unwrap();
        flow_ids.push(flow_id);
    }
    for (i, &flow_id) in flow_ids.iter().enumerate() {
        for _ in 0..3 {
            let report = create_test_report();
            runtime
                .handle_report(flow_id, i as u32 + 1, report)
                .await
                .unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    for flow_id in flow_ids {
        runtime.close_flow(flow_id).await.unwrap();
    }
    assert_eq!(algorithm.get_count(), initial_count + 15);
}

#[tokio::test]
async fn test_runtime_statistics() {
    let algorithm = TestAsyncAlgorithm::new();
    let runtime = RuntimeBuilder::new()
        .with_worker_threads(2)
        .build()
        .await
        .unwrap();
    runtime.register_async_algorithm(algorithm).await.unwrap();
    let initial_stats = runtime.get_stats().await;
    assert_eq!(initial_stats.active_flows, 0);
    assert_eq!(initial_stats.registered_algorithms, 1);
    let datapath_info = create_test_datapath_info(1);
    let flow_id = runtime
        .create_flow(1, "test_async", datapath_info)
        .await
        .unwrap();
    let stats_with_flow = runtime.get_stats().await;
    assert_eq!(stats_with_flow.active_flows, 1);
    runtime.close_flow(flow_id).await.unwrap();
    let final_stats = runtime.get_stats().await;
    assert_eq!(final_stats.active_flows, 0);
}

#[tokio::test]
async fn test_algorithm_timeout() {
    let runtime = RuntimeBuilder::new()
        .with_worker_threads(2)
        .with_algorithm_timeout(50)
        .build()
        .await
        .unwrap();
    let algorithm = TestAsyncAlgorithm::new();
    runtime.register_async_algorithm(algorithm).await.unwrap();
    let stats = runtime.get_stats().await;
    assert_eq!(stats.registered_algorithms, 1);
}
