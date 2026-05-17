//! Lotus runtime - Main orchestration and lifecycle management
//!
//! This module provides the main runtime that coordinates all components
//! of the Lotus framework for high-performance concurrent execution.

use crate::{
    algorithm::{AlgorithmRegistry, AsyncCongAlg},
    compute::{ComputeConfig, ComputeEngine},
    datapath_listener::DatapathListener,
    flow::{FlowId, FlowManager},
    ipc::{MessageRouter, RouterConfig},
    manager::AlgorithmManager,
    LotusConfig, LotusError, Result,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Main Lotus runtime that orchestrates all components
pub struct LotusRuntime {
    config: LotusConfig,
    algorithm_registry: Arc<AlgorithmRegistry<()>>,
    flow_manager: Arc<FlowManager<()>>,
    compute_engine: Arc<tokio::sync::Mutex<ComputeEngine>>,
    message_router: Arc<RwLock<MessageRouter>>,
    algorithm_manager: Arc<AlgorithmManager>,
    runtime_handle: Option<RuntimeHandle>,
    shutdown_sender: Option<mpsc::Sender<()>>,
}

/// Handle for controlling the runtime
#[derive(Debug)]
pub struct RuntimeHandle {
    pub shutdown_sender: mpsc::Sender<()>,
    pub join_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl RuntimeHandle {
    /// Gracefully shutdown the runtime
    pub async fn shutdown(self) -> Result<()> {
        info!("Initiating runtime shutdown");

        // Send shutdown signal
        if let Err(e) = self.shutdown_sender.send(()).await {
            warn!(error = %e, "Failed to send shutdown signal");
        }

        // Wait for all tasks to complete
        for handle in self.join_handles {
            if let Err(e) = handle.await {
                warn!(error = %e, "Task failed during shutdown");
            }
        }

        info!("Runtime shutdown completed");
        Ok(())
    }

    /// Force shutdown the runtime (may lose in-flight work)
    pub fn force_shutdown(self) {
        info!("Force shutting down runtime");
        for handle in self.join_handles {
            handle.abort();
        }
    }
}

/// Runtime configuration
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub lotus_config: LotusConfig,
    pub compute_config: ComputeConfig,
    pub router_config: RouterConfig,
    pub cleanup_interval_ms: u64,
    pub stats_interval_ms: u64,
    pub max_flow_idle_duration_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            lotus_config: LotusConfig::default(),
            compute_config: ComputeConfig::default(),
            router_config: RouterConfig::default(),
            cleanup_interval_ms: 30000,        // 30 seconds
            stats_interval_ms: 10000,          // 10 seconds
            max_flow_idle_duration_ms: 300000, // 5 minutes
        }
    }
}

impl LotusRuntime {
    /// Create a new Lotus runtime
    pub async fn new(config: LotusConfig) -> Result<Self> {
        info!("Initializing Lotus runtime");

        // Create algorithm registry
        let timeout_duration = Duration::from_millis(config.algorithm_timeout_ms);
        let algorithm_registry = Arc::new(AlgorithmRegistry::new(timeout_duration));

        // Create flow manager
        let flow_manager = Arc::new(FlowManager::new(algorithm_registry.clone()));

        // Create compute engine
        let compute_config = ComputeConfig {
            worker_count: config.worker_threads,
            task_timeout_ms: config.algorithm_timeout_ms,
            work_stealing_enabled: config.enable_work_stealing,
            ..Default::default()
        };
        let compute_engine = Arc::new(tokio::sync::Mutex::new(ComputeEngine::new(compute_config)));

        // Create message router
        let router_config = RouterConfig {
            worker_count: config.worker_threads,
            channel_buffer_size: config.message_buffer_size,
            ..Default::default()
        };
        let message_router = Arc::new(RwLock::new(MessageRouter::new(router_config)));

        // Create algorithm manager
        let algorithm_manager = Arc::new(AlgorithmManager::new());

        Ok(Self {
            config,
            algorithm_registry,
            flow_manager,
            compute_engine,
            message_router,
            algorithm_manager,
            runtime_handle: None,
            shutdown_sender: None,
        })
    }

    /// Start the runtime with all components
    pub async fn start(&mut self) -> Result<RuntimeHandle> {
        info!("Starting Lotus runtime");

        let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(1);
        let mut join_handles = Vec::new();

        // Start compute engine
        {
            let mut compute_engine = self.compute_engine.lock().await;
            let compute_handles = compute_engine.start().await?;
            join_handles.extend(compute_handles);
        }

        // Start message router
        {
            let mut message_router = self.message_router.write().await;
            let router_handles = message_router.start().await?;
            join_handles.extend(router_handles);
        }

        // Start background tasks
        let cleanup_handle = self.start_cleanup_task().await;
        let stats_handle = self.start_stats_task().await;
        let main_loop_handle = self.start_main_loop(shutdown_receiver).await;

        join_handles.push(cleanup_handle);
        join_handles.push(stats_handle);
        join_handles.push(main_loop_handle);

        let runtime_handle = RuntimeHandle {
            shutdown_sender: shutdown_sender.clone(),
            join_handles,
        };

        self.shutdown_sender = Some(shutdown_sender);
        self.runtime_handle = Some(RuntimeHandle {
            shutdown_sender: self.shutdown_sender.as_ref().unwrap().clone(),
            join_handles: Vec::new(), // Empty for the stored handle
        });

        info!("Lotus runtime started successfully");
        Ok(runtime_handle)
    }

    /// Start the main event loop
    ///
    /// Phase 2：若传入 `algorithms` 非空，则构造 `DatapathListener<NetlinkBlockingBridge>`
    /// 并 spawn 其 `run()` 任务；否则保留原先的 sleep 占位行为（向后兼容）。
    async fn start_main_loop(
        &self,
        mut shutdown_receiver: mpsc::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let flow_manager = self.flow_manager.clone();
        let algorithm_manager = self.algorithm_manager.clone();

        tokio::spawn(async move {
            info!("Starting main event loop (placeholder - use start_netlink() for real IPC)");

            loop {
                tokio::select! {
                    _ = shutdown_receiver.recv() => {
                        info!("Received shutdown signal in main loop");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // 占位：真实 IPC 通过 start_netlink() 启动
                        let _ = flow_manager.flow_count();
                        let _ = algorithm_manager.get_algorithm(
                            &crate::manager::FlowKey::new(0, 0, 0, 0)
                        );
                    }
                }
            }

            info!("Main event loop stopped");
        })
    }

    /// Start periodic cleanup task
    async fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let flow_manager = self.flow_manager.clone();
        let cleanup_interval = Duration::from_millis(30000); // 30 seconds
        let max_idle_duration = Duration::from_millis(300000); // 5 minutes

        tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                debug!("Running periodic cleanup");
                let cleaned_up = flow_manager.cleanup_inactive_flows(max_idle_duration).await;

                if cleaned_up > 0 {
                    info!(cleaned_up = cleaned_up, "Cleaned up inactive flows");
                }
            }
        })
    }

    /// Start periodic statistics reporting
    async fn start_stats_task(&self) -> tokio::task::JoinHandle<()> {
        let flow_manager = self.flow_manager.clone();
        let compute_stats = {
            let engine = self.compute_engine.lock().await;
            engine.get_stats()
        };
        let message_router = self.message_router.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(10000)); // 10 seconds

            loop {
                interval.tick().await;

                // Collect and log statistics
                let flow_count = flow_manager.flow_count();

                let router_stats = {
                    let router = message_router.read().await;
                    router.get_stats()
                };

                info!(
                    active_flows = flow_count,
                    compute_throughput = compute_stats.get_throughput(),
                    compute_success_rate = compute_stats.get_success_rate(),
                    router_workers = router_stats.worker_count,
                    "Runtime statistics"
                );
            }
        })
    }

    /// Register a synchronous algorithm
    pub async fn register_sync_algorithm<A>(&self, algorithm: A) -> Result<()>
    where
        A: crate::algorithm::CongAlg<()> + 'static,
    {
        let name = algorithm.name().to_string();
        self.algorithm_registry.register_sync(algorithm).await;
        info!(algorithm = %name, "Registered sync algorithm");
        Ok(())
    }

    /// Register an asynchronous algorithm
    pub async fn register_async_algorithm<A>(&self, algorithm: A) -> Result<()>
    where
        A: crate::algorithm::AsyncCongAlg<()> + 'static,
    {
        let name = algorithm.name().to_string();
        self.algorithm_registry.register_async(algorithm).await;
        info!(algorithm = %name, "Registered async algorithm");
        Ok(())
    }

    /// Create a new flow
    pub async fn create_flow(
        &self,
        sock_id: u32,
        algorithm_name: &str,
        datapath_info: crate::algorithm::DatapathInfo,
    ) -> Result<FlowId> {
        // Create a mock sender for now
        struct MockSender;
        #[async_trait::async_trait]
        impl crate::ipc::AsyncIpc<()> for MockSender {
            async fn send(&self, _msg: &[u8], _to: &()) -> Result<()> {
                Ok(())
            }
            async fn recv(&self, _buf: &mut [u8]) -> Result<(usize, ())> {
                Ok((0, ()))
            }
            async fn close(&mut self) -> Result<()> {
                Ok(())
            }
            fn name(&self) -> &'static str {
                "mock"
            }
        }

        let sender = Arc::new(MockSender);
        self.flow_manager
            .create_flow(sock_id, algorithm_name, datapath_info, sender)
            .await
    }

    /// Handle a measurement report
    pub async fn handle_report(
        &self,
        flow_id: FlowId,
        sock_id: u32,
        report: crate::algorithm::Report,
    ) -> Result<()> {
        self.flow_manager
            .handle_report(flow_id, sock_id, report)
            .await
    }

    /// Close a flow
    pub async fn close_flow(&self, flow_id: FlowId) -> Result<()> {
        self.flow_manager.close_flow(flow_id).await
    }

    /// Get runtime statistics
    pub async fn get_stats(&self) -> RuntimeStats {
        let flow_count = self.flow_manager.flow_count();
        let active_algorithms = self.algorithm_registry.list_algorithms().await;

        let compute_stats = {
            let engine = self.compute_engine.lock().await;
            engine.get_stats()
        };

        let router_stats = {
            let router = self.message_router.read().await;
            router.get_stats()
        };

        RuntimeStats {
            active_flows: flow_count,
            registered_algorithms: active_algorithms.len(),
            compute_throughput: compute_stats.get_throughput(),
            compute_success_rate: compute_stats.get_success_rate(),
            total_tasks_submitted: compute_stats
                .tasks_submitted
                .load(std::sync::atomic::Ordering::Relaxed),
            total_tasks_completed: compute_stats
                .tasks_completed
                .load(std::sync::atomic::Ordering::Relaxed),
            router_worker_count: router_stats.worker_count,
        }
    }

    /// Get the runtime handle (if started)
    pub fn get_handle(&self) -> Option<&RuntimeHandle> {
        self.runtime_handle.as_ref()
    }

    // ── Phase 2: Netlink 集成 ─────────────────────────────────────────────────

    /// 将 `datapath_programs` 中的 fold 源码编译成 INSTALL 字节帧列表。
    ///
    /// 返回 `(install_frames, uid_map, scope_map)`：
    /// - `install_frames`：每个程序对应一条 INSTALL(type=2) 二进制帧
    /// - `uid_map`：程序名 → `program_uid`
    /// - `scope_map`：程序名 → `portus::lang::Scope`（包含寄存器名→Reg 映射，用于 get_field/update_field）
    ///
    /// 编译失败的程序记录 warn 并跳过。
    pub fn compile_install_msgs(
        programs: &HashMap<&'static str, String>,
    ) -> (
        Vec<Vec<u8>>,
        HashMap<String, u32>,
        HashMap<String, portus::lang::Scope>,
        HashMap<String, Vec<String>>,
    ) {
        let mut msgs = Vec::new();
        let mut uid_map: HashMap<String, u32> = HashMap::new();
        let mut scope_map: HashMap<String, portus::lang::Scope> = HashMap::new();
        let mut report_fields_map: HashMap<String, Vec<String>> = HashMap::new();
        for (name, src) in programs.iter() {
            match portus::lang::compile(src.as_bytes(), &[]) {
                Ok((bin, sc)) => {
                    let portus_msg = portus::serialize::install::Msg {
                        sid: 0,
                        program_uid: sc.program_uid,
                        num_events: bin.events.len() as u32,
                        num_instrs: bin.instrs.len() as u32,
                        instrs: bin,
                    };
                    match portus::serialize::serialize(&portus_msg) {
                        Ok(buf) => {
                            info!(program = %name, uid = sc.program_uid, bytes = buf.len(), "compiled datapath program");
                            uid_map.insert(name.to_string(), sc.program_uid);
                            // Keep the returned Scope for compatibility / name -> Reg lookup
                            scope_map.insert(name.to_string(), sc.clone());

                            // Extract Report field names (ordered) from source and store for
                            // later mapping of MEASURE fields -> named Report entries.
                            fn extract_report_fields(src: &str) -> Vec<String> {
                                let mut res = Vec::new();
                                if let Some(idx) = src.find("(Report") {
                                    // find matching ')' for the Report form
                                    let mut i = idx;
                                    let bytes = src.as_bytes();
                                    let mut depth: i32 = 0;
                                    let mut end = None;
                                    while i < bytes.len() {
                                        match bytes[i] as char {
                                            '(' => {
                                                depth += 1;
                                            }
                                            ')' => {
                                                depth -= 1;
                                                if depth == 0 {
                                                    end = Some(i);
                                                    break;
                                                }
                                            }
                                            _ => {}
                                        }
                                        i += 1;
                                    }
                                    if let Some(end_idx) = end {
                                        let inner = &src[idx + "(Report".len()..end_idx];
                                        let inner_bytes = inner.as_bytes();
                                        let mut j = 0usize;
                                        while j < inner_bytes.len() {
                                            if inner_bytes[j] as char == '(' {
                                                j += 1; // skip '('
                                                        // skip whitespace
                                                while j < inner_bytes.len()
                                                    && (inner_bytes[j] as char).is_whitespace()
                                                {
                                                    j += 1;
                                                }
                                                let token_start = j;
                                                while j < inner_bytes.len()
                                                    && !(inner_bytes[j] as char).is_whitespace()
                                                    && inner_bytes[j] as char != ')'
                                                {
                                                    j += 1;
                                                }
                                                let token = &inner[token_start..j];
                                                let token_str = token.trim();
                                                let name_slice = if token_str == "volatile" {
                                                    while j < inner_bytes.len()
                                                        && (inner_bytes[j] as char).is_whitespace()
                                                    {
                                                        j += 1;
                                                    }
                                                    let name_start = j;
                                                    while j < inner_bytes.len()
                                                        && !(inner_bytes[j] as char).is_whitespace()
                                                        && inner_bytes[j] as char != ')'
                                                    {
                                                        j += 1;
                                                    }
                                                    &inner[name_start..j]
                                                } else {
                                                    token
                                                };
                                                let name_str = name_slice.trim().to_string();
                                                if !name_str.is_empty() {
                                                    res.push(name_str);
                                                }
                                            } else {
                                                j += 1;
                                            }
                                        }
                                    }
                                }
                                res
                            }

                            let fields = extract_report_fields(src);
                            report_fields_map.insert(name.to_string(), fields);
                            msgs.push(buf);
                        }
                        Err(e) => {
                            warn!(program = %name, err = %e.0, "failed to serialize INSTALL msg, skipping");
                        }
                    }
                }
                Err(e) => {
                    warn!(program = %name, err = ?e, "datapath program compile failed, skipping");
                }
            }
        }
        (msgs, uid_map, scope_map, report_fields_map)
    }

    /// 启动真实 netlink IPC 主循环（Phase 2 核心入口）。
    ///
    /// 调用方需提供：
    /// - `algorithms`: 算法名 → `Box<dyn AsyncCongAlg<()>>`（注意每个算法只能移动一次）
    ///
    /// 方法内部会：
    /// 1. 从每个算法收集 `datapath_programs()` 并编译成 INSTALL 帧
    /// 2. 创建 `NetlinkBlockingBridge`（需要 CAP_NET_ADMIN 权限）
    /// 3. spawn `DatapathListener::run()` 任务
    ///
    /// 返回 `JoinHandle`，调用方可通过 abort() 停止。
    pub async fn start_netlink(
        &self,
        algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        // 1. 收集并编译所有 datapath programs，同时获取 uid 映射
        let mut all_programs: HashMap<&'static str, String> = HashMap::new();
        for alg in algorithms.values() {
            let progs = alg.datapath_programs().await;
            all_programs.extend(progs);
        }
        let (install_msgs, uid_map, scope_map, report_fields_map) =
            Self::compile_install_msgs(&all_programs);
        info!(
            programs = all_programs.len(),
            install_msgs = install_msgs.len(),
            "compiled datapath programs for netlink start"
        );

        // 2. 创建 netlink socket
        let bridge = crate::ipc_netlink::NetlinkBlockingBridge::new()
            .map_err(|e| LotusError::Ipc(format!("failed to create netlink bridge: {e}")))?;

        // 3. 校验默认路由算法。调用方显式设置过的默认算法优先保留；
        //    只有当前默认算法没有注册时，才回退到一个可用算法。
        let alg_manager = self.algorithm_manager.clone();
        self.ensure_default_algorithm_registered(&algorithms);

        // 4. 构造 DatapathListener 并 spawn（uid_map/scope_map 注入）
        let mut listener = DatapathListener::new(
            bridge,
            install_msgs,
            uid_map,
            scope_map,
            report_fields_map,
            alg_manager,
            algorithms,
        );

        let handle = tokio::spawn(async move {
            match listener.run().await {
                Ok(_infallible) => {} // never reached
                Err(e) => {
                    error!(err = %e, "DatapathListener exited with error");
                }
            }
        });

        info!("DatapathListener (netlink) started");
        Ok(handle)
    }

    /// 使用自定义 IPC 启动主循环（用于测试 / 非 netlink 场景）。
    pub async fn start_with_ipc<I: crate::ipc::AsyncIpc<()>>(
        &self,
        ipc: I,
        algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>>,
        install_msgs: Vec<Vec<u8>>,
    ) -> tokio::task::JoinHandle<()> {
        let alg_manager = self.algorithm_manager.clone();
        // start_with_ipc 用于测试，scope_map/uid_map 传空
        let mut listener = DatapathListener::new(
            ipc,
            install_msgs,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            alg_manager,
            algorithms,
        );

        tokio::spawn(async move {
            match listener.run().await {
                Ok(_infallible) => {}
                Err(e) => {
                    error!(err = %e, "DatapathListener (custom IPC) exited with error");
                }
            }
        })
    }

    // ── 流路由配置（委托给 AlgorithmManager） ─────────────────────────────────

    /// 设置默认算法名称（当无规则匹配时使用）
    pub fn set_default_algorithm(&self, algorithm: String) {
        self.algorithm_manager.set_default_algorithm(algorithm);
    }

    /// 获取当前默认算法名称。
    pub fn default_algorithm(&self) -> String {
        self.algorithm_manager.default_algorithm()
    }

    fn ensure_default_algorithm_registered(
        &self,
        algorithms: &HashMap<String, Box<dyn AsyncCongAlg<()>>>,
    ) {
        let configured_default = self.algorithm_manager.default_algorithm();

        if algorithms.contains_key(&configured_default) {
            info!(
                algorithm = %configured_default,
                "using configured default algorithm for flow routing"
            );
            return;
        }

        if let Some(fallback) = algorithms.keys().min().cloned() {
            warn!(
                configured_default = %configured_default,
                fallback = %fallback,
                "configured default algorithm is not registered; falling back"
            );
            self.algorithm_manager
                .set_default_algorithm(fallback.clone());
            info!(algorithm = %fallback, "set default algorithm for flow routing");
        } else {
            warn!(
                configured_default = %configured_default,
                "no algorithms registered for flow routing"
            );
        }
    }

    /// 添加流路由规则（`AlgorithmRule::PortRange` / `ExactMatch` 等）
    pub fn add_flow_rule(&self, rule: crate::manager::AlgorithmRule) {
        self.algorithm_manager.add_rule(rule);
    }

    /// 为精确 (src_port, dst_port) 组合添加算法映射（兼容旧版 portus add_flow_alg）
    pub fn add_flow_alg_by_port(&self, src_port: u16, dst_port: u16, algorithm: String) {
        self.algorithm_manager
            .add_rule(crate::manager::AlgorithmRule::PortRange {
                start_port: src_port,
                end_port: src_port,
                algorithm: algorithm.clone(),
            });
        self.algorithm_manager
            .add_rule(crate::manager::AlgorithmRule::PortRange {
                start_port: dst_port,
                end_port: dst_port,
                algorithm,
            });
    }

    /// 查询某条流应使用的算法名称
    pub fn get_flow_algorithm(&self, flow_key: &crate::manager::FlowKey) -> String {
        self.algorithm_manager.get_algorithm(flow_key)
    }
}

/// Runtime statistics
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    pub active_flows: usize,
    pub registered_algorithms: usize,
    pub compute_throughput: f64,
    pub compute_success_rate: f64,
    pub total_tasks_submitted: u64,
    pub total_tasks_completed: u64,
    pub router_worker_count: usize,
}

/// Builder for creating and configuring a Lotus runtime
pub struct RuntimeBuilder {
    config: RuntimeConfig,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            config: RuntimeConfig::default(),
        }
    }

    pub fn with_worker_threads(mut self, count: usize) -> Self {
        self.config.lotus_config.worker_threads = count;
        self.config.compute_config.worker_count = count;
        self.config.router_config.worker_count = count;
        self
    }

    pub fn with_algorithm_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.lotus_config.algorithm_timeout_ms = timeout_ms;
        self.config.compute_config.task_timeout_ms = timeout_ms;
        self
    }

    pub fn with_message_buffer_size(mut self, size: usize) -> Self {
        self.config.lotus_config.message_buffer_size = size;
        self.config.router_config.channel_buffer_size = size;
        self
    }

    pub fn enable_work_stealing(mut self, enabled: bool) -> Self {
        self.config.lotus_config.enable_work_stealing = enabled;
        self.config.compute_config.work_stealing_enabled = enabled;
        self
    }

    pub fn with_cleanup_interval(mut self, interval_ms: u64) -> Self {
        self.config.cleanup_interval_ms = interval_ms;
        self
    }

    pub async fn build(self) -> Result<LotusRuntime> {
        LotusRuntime::new(self.config.lotus_config).await
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::{CongAlg, DatapathInfo};
    use crate::flow::FlowContext;
    use std::collections::HashMap;

    struct TestAlgorithm;

    impl CongAlg<()> for TestAlgorithm {
        fn name(&self) -> &'static str {
            "test"
        }

        fn datapath_programs(&self) -> HashMap<&'static str, String> {
            HashMap::new()
        }

        fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn crate::Flow> {
            Box::new(TestFlow)
        }
    }

    struct TestFlow;

    impl crate::Flow for TestFlow {
        fn on_report(&mut self, _sock_id: u32, _report: crate::algorithm::Report) {}
    }

    #[tokio::test]
    async fn test_runtime_creation() {
        let config = LotusConfig::default();
        let runtime = LotusRuntime::new(config).await.unwrap();

        // Runtime should be created successfully
        assert_eq!(runtime.flow_manager.flow_count(), 0);
    }

    #[tokio::test]
    async fn test_runtime_builder() {
        let runtime = RuntimeBuilder::new()
            .with_worker_threads(4)
            .with_algorithm_timeout(200)
            .with_message_buffer_size(4096)
            .enable_work_stealing(true)
            .build()
            .await
            .unwrap();

        assert_eq!(runtime.config.worker_threads, 4);
        assert_eq!(runtime.config.algorithm_timeout_ms, 200);
        assert_eq!(runtime.config.message_buffer_size, 4096);
        assert!(runtime.config.enable_work_stealing);
    }

    // ── Phase 2 测试 ─────────────────────────────────────────────────────

    use crate::algorithm::{AsyncCongAlg, Report};
    use crate::flow::AsyncFlow;
    use crate::serialize::{create, measure, ready, serialize};
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc as StdArc;
    use tokio::sync::Mutex as TokioMutex;

    // ── 复用 MockIpc ──────────────────────────────────────────────────

    struct MockIpc2 {
        inbound: StdArc<TokioMutex<VecDeque<Vec<u8>>>>,
        outbound: StdArc<TokioMutex<Vec<Vec<u8>>>>,
    }

    impl MockIpc2 {
        fn new_with_handles() -> (
            Self,
            StdArc<TokioMutex<VecDeque<Vec<u8>>>>,
            StdArc<TokioMutex<Vec<Vec<u8>>>>,
        ) {
            let inbound = StdArc::new(TokioMutex::new(VecDeque::new()));
            let outbound = StdArc::new(TokioMutex::new(Vec::new()));
            (
                Self {
                    inbound: inbound.clone(),
                    outbound: outbound.clone(),
                },
                inbound,
                outbound,
            )
        }
    }

    #[async_trait]
    impl crate::ipc::AsyncIpc<()> for MockIpc2 {
        async fn send(&self, msg: &[u8], _: &()) -> crate::Result<()> {
            self.outbound.lock().await.push(msg.to_vec());
            Ok(())
        }
        async fn recv(&self, buf: &mut [u8]) -> crate::Result<(usize, ())> {
            if let Some(frame) = self.inbound.lock().await.pop_front() {
                let n = frame.len().min(buf.len());
                buf[..n].copy_from_slice(&frame[..n]);
                return Ok((n, ()));
            }
            Ok((0, ()))
        }
        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "mock2"
        }
    }

    // ── Mock 算法 / 流 ────────────────────────────────────────────────

    #[derive(Default)]
    struct P2Counters {
        report_count: AtomicU32,
        close_count: AtomicU32,
    }

    struct P2Flow {
        counters: StdArc<P2Counters>,
    }

    #[async_trait]
    impl AsyncFlow for P2Flow {
        async fn on_report(&mut self, _: u32, _: Report) -> crate::Result<()> {
            self.counters.report_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn close(&mut self) -> crate::Result<()> {
            self.counters.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct P2Alg {
        counters: StdArc<P2Counters>,
    }

    #[async_trait]
    impl AsyncCongAlg<()> for P2Alg {
        fn name(&self) -> &'static str {
            "p2alg"
        }
        async fn datapath_programs(&self) -> HashMap<&'static str, String> {
            // 合法的最小 fold 程序（与 portus 测试用例一致）
            let prog = "(def (Report (volatile foo 0)))\
                        (when true (bind Report.foo 4))"
                .to_string();
            let mut m = HashMap::new();
            m.insert("test_prog", prog);
            m
        }
        async fn new_flow(
            &self,
            _ctx: crate::flow::FlowContext<()>,
            _info: DatapathInfo,
        ) -> crate::Result<Box<dyn AsyncFlow>> {
            Ok(Box::new(P2Flow {
                counters: self.counters.clone(),
            }))
        }
    }

    fn boxed_p2_alg() -> Box<dyn AsyncCongAlg<()>> {
        Box::new(P2Alg {
            counters: StdArc::new(P2Counters::default()),
        })
    }

    #[tokio::test]
    async fn test_netlink_default_preserves_configured_algorithm() {
        let runtime = LotusRuntime::new(LotusConfig::default()).await.unwrap();
        runtime.set_default_algorithm("dtcc".to_string());

        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert("dtcc".to_string(), boxed_p2_alg());
        algs.insert("orca".to_string(), boxed_p2_alg());

        runtime.ensure_default_algorithm_registered(&algs);

        assert_eq!(runtime.default_algorithm(), "dtcc");
    }

    #[tokio::test]
    async fn test_netlink_default_falls_back_when_configured_algorithm_missing() {
        let runtime = LotusRuntime::new(LotusConfig::default()).await.unwrap();
        runtime.set_default_algorithm("missing".to_string());

        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert("dtcc".to_string(), boxed_p2_alg());
        algs.insert("bbr".to_string(), boxed_p2_alg());

        runtime.ensure_default_algorithm_registered(&algs);

        assert_eq!(runtime.default_algorithm(), "bbr");
    }

    // ── P2-1: compile_install_msgs 编译合法程序 → 至少一帧非空字节 ──────

    #[test]
    fn test_compile_install_msgs_valid_program() {
        let prog = "(def (Report (volatile foo 0)))\
                    (when true (bind Report.foo 4))"
            .to_string();
        let mut programs = HashMap::new();
        programs.insert("prog1", prog);

        let (frames, uid_map, scope_map, report_fields) =
            LotusRuntime::compile_install_msgs(&programs);
        assert_eq!(frames.len(), 1, "should produce exactly 1 INSTALL msg");
        assert!(!frames[0].is_empty(), "INSTALL msg should not be empty");
        // INSTALL 帧 type=2，最小长度 = 8(hdr) + 12(u32s) = 20 字节
        assert!(
            frames[0].len() >= 20,
            "INSTALL msg too short: {} bytes",
            frames[0].len()
        );
        assert!(
            uid_map.contains_key("prog1"),
            "uid_map should contain prog1"
        );
        assert!(
            scope_map.contains_key("prog1"),
            "scope_map should contain prog1"
        );
        // Scope 应包含 Report.foo 寄存器
        let sc = scope_map.get("prog1").unwrap();
        assert!(
            sc.get("Report.foo").is_some(),
            "scope should contain Report.foo"
        );
        // report_fields should contain parsed Report field names
        assert!(
            report_fields.contains_key("prog1"),
            "report_fields should contain prog1"
        );
        assert!(report_fields
            .get("prog1")
            .unwrap()
            .contains(&"foo".to_string()));
    }

    // ── P2-2: compile_install_msgs 遇到非法程序 → 跳过，不 panic ─────────

    #[test]
    fn test_compile_install_msgs_invalid_program_skipped() {
        let mut programs = HashMap::new();
        programs.insert("bad_prog", "this is not valid fold syntax!!!".to_string());
        let (frames, uid_map, scope_map, report_fields) =
            LotusRuntime::compile_install_msgs(&programs);
        // 非法程序被跳过，返回空列表
        assert_eq!(frames.len(), 0, "invalid program should be skipped");
        assert!(
            uid_map.is_empty(),
            "uid_map should be empty for invalid program"
        );
        assert!(
            scope_map.is_empty(),
            "scope_map should be empty for invalid program"
        );
        assert!(
            report_fields.is_empty(),
            "report_fields should be empty for invalid program"
        );
    }

    // ── P2-3: start_with_ipc 启动后，注入 CREATE → 流被创建 ─────────────

    #[tokio::test]
    async fn test_start_with_ipc_dispatches_create() {
        let runtime = LotusRuntime::new(LotusConfig::default()).await.unwrap();
        runtime.set_default_algorithm("p2alg".to_string());

        let counters = StdArc::new(P2Counters::default());
        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert(
            "p2alg".to_string(),
            Box::new(P2Alg {
                counters: counters.clone(),
            }),
        );

        let (ipc, inbound, _outbound) = MockIpc2::new_with_handles();

        // 注入 CREATE(sid=1)
        inbound.lock().await.push_back(
            serialize(&create::Msg {
                sid: 1,
                init_cwnd: 10000,
                mss: 1448,
                src_ip: 0,
                src_port: 5001,
                dst_ip: 0,
                dst_port: 80,
                cong_alg: None,
            })
            .unwrap(),
        );
        // 注入 MEASURE(sid=1, 2 fields) 触发 on_report
        inbound.lock().await.push_back(
            serialize(&measure::Msg {
                sid: 1,
                program_uid: 1,
                num_fields: 2,
                fields: vec![1, 2],
            })
            .unwrap(),
        );
        // 注入 MEASURE(sid=1, num_fields=0) 触发 close
        inbound.lock().await.push_back(
            serialize(&measure::Msg {
                sid: 1,
                program_uid: 1,
                num_fields: 0,
                fields: vec![],
            })
            .unwrap(),
        );

        let handle = runtime.start_with_ipc(ipc, algs, vec![]).await;

        // 等待足够时间让任务处理完所有消息（MockIpc 无延迟）
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        handle.abort();

        assert_eq!(
            counters.report_count.load(Ordering::SeqCst),
            1,
            "on_report should be called once"
        );
        assert_eq!(
            counters.close_count.load(Ordering::SeqCst),
            1,
            "close should be called once"
        );
    }

    // ── P2-4: start_with_ipc + READY → 流表清空，install_msgs 广播 ────────

    #[tokio::test]
    async fn test_start_with_ipc_ready_broadcasts_install() {
        let runtime = LotusRuntime::new(LotusConfig::default()).await.unwrap();
        runtime.set_default_algorithm("p2alg".to_string());

        let counters = StdArc::new(P2Counters::default());
        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert("p2alg".to_string(), Box::new(P2Alg { counters }));

        let (ipc, inbound, outbound) = MockIpc2::new_with_handles();
        let install_payload = vec![0xBBu8; 24]; // 伪 INSTALL 帧

        // 先注入 CREATE，再注入 READY
        inbound.lock().await.push_back(
            serialize(&create::Msg {
                sid: 2,
                init_cwnd: 10000,
                mss: 1448,
                src_ip: 0,
                src_port: 5001,
                dst_ip: 0,
                dst_port: 80,
                cong_alg: None,
            })
            .unwrap(),
        );
        inbound
            .lock()
            .await
            .push_back(serialize(&ready::Msg { id: 0 }).unwrap());

        let handle = runtime
            .start_with_ipc(ipc, algs, vec![install_payload.clone()])
            .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        handle.abort();

        let sent = outbound.lock().await.clone();
        // 首次 CREATE 时发一次 install，READY 后再发一次
        let cnt = sent
            .iter()
            .filter(|f| f.as_slice() == install_payload.as_slice())
            .count();
        assert!(
            cnt >= 1,
            "install_msgs should have been broadcast at least once"
        );
    }
}
