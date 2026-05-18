//! Portus compatibility layer
//!
//! This module provides backward compatibility with Portus interfaces,
//! allowing existing algorithms to work with minimal changes.

use crate::{
    algorithm::{AsyncCongAlg, CongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, Flow, FlowContext},
    portus_datapath,
    runtime::{LotusRuntime, RuntimeBuilder},
    Result,
};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{debug, info};

/// Portus-compatible datapath trait
pub trait Datapath<I> {
    /// Set a datapath program
    fn set_program(&mut self, program_name: &str, fields: Option<&[(&str, u32)]>) -> Result<Scope>;

    /// Update datapath fields
    fn update_field(&mut self, sc: &Scope, updates: &[(&str, u32)]) -> Result<()>;
}

/// Portus-compatible scope for datapath programs
#[derive(Debug, Clone)]
pub struct Scope {
    pub program_uid: u32,
    pub fields: HashMap<String, u32>,
    pub(crate) inner: Option<portus::lang::Scope>,
}

impl Scope {
    pub fn new(program_uid: u32) -> Self {
        Self {
            program_uid,
            fields: HashMap::new(),
            inner: None,
        }
    }

    pub fn get_field(&self, name: &str) -> Option<u32> {
        self.fields.get(name).copied()
    }

    pub fn set_field(&mut self, name: String, value: u32) {
        self.fields.insert(name, value);
    }
}

/// Portus-compatible datapath implementation
pub struct CompatDatapath<I> {
    flow_context: FlowContext<I>,
    programs: HashMap<String, String>,
    compiled_scopes: HashMap<String, Scope>,
    active_scope: Option<Scope>,
}

impl<I> CompatDatapath<I> {
    pub fn new(flow_context: FlowContext<I>, programs: HashMap<String, String>) -> Self {
        Self {
            flow_context,
            programs,
            compiled_scopes: HashMap::new(),
            active_scope: None,
        }
    }
}

impl<I> Datapath<I> for CompatDatapath<I>
where
    I: Default + Send + Sync + 'static,
{
    fn set_program(&mut self, program_name: &str, fields: Option<&[(&str, u32)]>) -> Result<Scope> {
        debug!(program = %program_name, "Setting datapath program");

        let program_source = self.programs.get(program_name).ok_or_else(|| {
            crate::LotusError::Algorithm(format!("Program '{}' not found", program_name))
        })?;

        if !self.compiled_scopes.contains_key(program_name) {
            let (bin, portus_scope) =
                portus_datapath::compile_datapath_program(program_source, &[])?;
            let install_payload = portus_datapath::build_install_message(0, &portus_scope, &bin)?;

            portus_datapath::send_control_message_blocking(
                self.flow_context.sender.as_ref(),
                I::default(),
                install_payload,
            )?;

            self.compiled_scopes.insert(
                program_name.to_string(),
                Scope {
                    program_uid: portus_scope.program_uid,
                    fields: HashMap::new(),
                    inner: Some(portus_scope),
                },
            );
        }

        let mut scope = self
            .compiled_scopes
            .get(program_name)
            .cloned()
            .ok_or_else(|| {
                crate::LotusError::Algorithm(format!(
                    "Compiled program '{}' not found",
                    program_name
                ))
            })?;

        if let Some(field_updates) = fields {
            let inner = scope.inner.as_ref().ok_or_else(|| {
                crate::LotusError::Serialization(
                    "missing portus scope in compatibility mode".to_string(),
                )
            })?;

            let change_payload = portus_datapath::build_change_program_message(
                self.flow_context.sock_id,
                inner,
                field_updates,
            )?;

            portus_datapath::send_control_message_blocking(
                self.flow_context.sender.as_ref(),
                I::default(),
                change_payload,
            )?;

            for (name, value) in field_updates {
                scope.set_field((*name).to_string(), *value);
            }
        } else {
            let inner = scope.inner.as_ref().ok_or_else(|| {
                crate::LotusError::Serialization(
                    "missing portus scope in compatibility mode".to_string(),
                )
            })?;
            let change_payload = portus_datapath::build_change_program_message(
                self.flow_context.sock_id,
                inner,
                &[],
            )?;

            portus_datapath::send_control_message_blocking(
                self.flow_context.sender.as_ref(),
                I::default(),
                change_payload,
            )?;
        }

        self.active_scope = Some(scope.clone());
        Ok(scope)
    }

    fn update_field(&mut self, sc: &Scope, updates: &[(&str, u32)]) -> Result<()> {
        debug!(
            program_uid = sc.program_uid,
            updates = updates.len(),
            "Updating datapath fields"
        );

        let inner = sc.inner.as_ref().ok_or_else(|| {
            crate::LotusError::Serialization(
                "missing portus scope in compatibility mode".to_string(),
            )
        })?;

        let payload =
            portus_datapath::build_update_field_message(self.flow_context.sock_id, inner, updates)?;
        portus_datapath::send_control_message_blocking(
            self.flow_context.sender.as_ref(),
            I::default(),
            payload,
        )?;

        Ok(())
    }
}

/// Wrapper to make Portus algorithms work with Lotus
pub struct PortusAlgorithmWrapper<A, I> {
    inner: A,
    _phantom: std::marker::PhantomData<I>,
}

impl<A, I> PortusAlgorithmWrapper<A, I> {
    pub fn new(algorithm: A) -> Self {
        Self {
            inner: algorithm,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<A, I> AsyncCongAlg<I> for PortusAlgorithmWrapper<A, I>
where
    A: CongAlg<I> + Send + Sync + 'static,
    I: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        self.inner.datapath_programs()
    }

    async fn new_flow(
        &self,
        control: FlowContext<I>,
        info: DatapathInfo,
    ) -> Result<Box<dyn AsyncFlow>> {
        // Create the flow using the original algorithm (returns Box<dyn Flow>)
        let flow = self.inner.new_flow(control, info);

        Ok(Box::new(SyncFlowAsAsync { inner: Some(flow) }))
    }
}

/// Adapter that wraps a Box<dyn Flow> into an AsyncFlow
struct SyncFlowAsAsync {
    inner: Option<Box<dyn Flow>>,
}

#[async_trait]
impl AsyncFlow for SyncFlowAsAsync {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        let mut flow = self
            .inner
            .take()
            .ok_or_else(|| crate::LotusError::Flow("Flow already consumed".to_string()))?;

        let result = tokio::task::spawn_blocking(move || {
            flow.on_report(sock_id, report);
            flow
        })
        .await;

        match result {
            Ok(returned_flow) => {
                self.inner = Some(returned_flow);
                Ok(())
            }
            Err(e) => Err(crate::LotusError::Flow(format!(
                "Flow execution failed: {}",
                e
            ))),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut flow) = self.inner.take() {
            tokio::task::spawn_blocking(move || {
                flow.close();
            })
            .await
            .map_err(|e| crate::LotusError::Flow(format!("Flow close failed: {}", e)))?;
        }
        Ok(())
    }
}

/// Portus-compatible runtime builder
pub struct PortusCompatRuntime {
    runtime: LotusRuntime,
    algorithms: Vec<String>,
}

impl PortusCompatRuntime {
    /// Create a new Portus-compatible runtime
    pub async fn new() -> Result<Self> {
        let runtime = RuntimeBuilder::new()
            .with_worker_threads(1) // Start with single-threaded for compatibility
            .build()
            .await?;

        Ok(Self {
            runtime,
            algorithms: Vec::new(),
        })
    }

    /// Register a Portus algorithm
    pub async fn register_algorithm<A>(&mut self, algorithm: A) -> Result<()>
    where
        A: CongAlg<()> + 'static,
    {
        let name = algorithm.name().to_string();
        let wrapped = PortusAlgorithmWrapper::new(algorithm);

        self.runtime.register_async_algorithm(wrapped).await?;
        self.algorithms.push(name.clone());

        info!(algorithm = %name, "Registered Portus-compatible algorithm");
        Ok(())
    }

    /// Start the runtime (Portus-compatible interface)
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting Portus-compatible runtime");
        let handle = self.runtime.start().await?;

        // In Portus, run() blocks forever
        // Here we simulate that by waiting for a shutdown signal
        tokio::signal::ctrl_c().await.map_err(|e| {
            crate::LotusError::Runtime(format!("Failed to wait for shutdown signal: {}", e))
        })?;

        info!("Received shutdown signal, stopping runtime");
        handle.shutdown().await?;
        Ok(())
    }

    /// 通过真实 Netlink IPC 启动运行时，阻塞直至 Ctrl-C。
    ///
    /// 与 `run()` 的区别：
    /// - `run()` 内部调用 `start()`，其 main_loop 是**占位 sleep 循环**，无任何内核 IPC；
    /// - `run_netlink()` 调用 `start_netlink()`，通过 `NetlinkBlockingBridge` +
    ///   `DatapathListener` 与 CCP 内核模块建立真实通信。
    ///
    /// 传入的 `algorithms` 会被消费（move），每个算法只能注册一次。
    pub async fn run_netlink(
        &self,
        algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>>,
    ) -> Result<()> {
        info!("Starting Portus-compatible runtime via Netlink IPC");

        let netlink_handle = self.runtime.start_netlink(algorithms).await?;

        tokio::signal::ctrl_c()
            .await
            .map_err(|e| crate::LotusError::Runtime(format!("Failed to wait for Ctrl-C: {}", e)))?;

        info!("Received shutdown signal, stopping Netlink listener");
        netlink_handle.abort();
        Ok(())
    }

    /// Get the underlying Lotus runtime
    pub fn lotus_runtime(&self) -> &LotusRuntime {
        &self.runtime
    }

    /// Get the underlying Lotus runtime (mutable)
    pub fn lotus_runtime_mut(&mut self) -> &mut LotusRuntime {
        &mut self.runtime
    }
}

/// Convenience macro for easy migration from Portus
#[macro_export]
macro_rules! portus_main {
    ($alg:expr) => {
        #[tokio::main]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {
            tracing_subscriber::init();

            let mut runtime = $crate::compat::PortusCompatRuntime::new().await?;
            runtime.register_algorithm($alg).await?;
            runtime.run().await?;

            Ok(())
        }
    };
}

/// Helper functions for common Portus patterns
pub mod helpers {
    use super::*;

    /// Create a simple report with basic fields
    pub fn create_report(fields: &[(&str, u64)]) -> Report {
        let mut report = Report {
            fields: HashMap::new(),
            timestamp: std::time::Instant::now(),
        };

        for (name, value) in fields {
            report.set_field(name.to_string(), *value);
        }

        report
    }

    /// Create datapath info from connection parameters
    pub fn create_datapath_info(
        sock_id: u32,
        src_ip: u32,
        src_port: u16,
        dst_ip: u32,
        dst_port: u16,
    ) -> DatapathInfo {
        DatapathInfo {
            sock_id,
            init_cwnd: 10,
            mss: 1460,
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            programs: std::collections::HashMap::new(),
            scopes: std::collections::HashMap::new(),
            report_fields: std::collections::HashMap::new(),
        }
    }

    /// Convert IP address string to u32
    pub fn ipv4_to_u32(ip: &str) -> Result<u32> {
        use std::net::Ipv4Addr;
        ip.parse::<Ipv4Addr>()
            .map(|addr| u32::from(addr))
            .map_err(|e| crate::LotusError::Runtime(format!("Invalid IP address: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct TestPortusAlgorithm;

    impl CongAlg<()> for TestPortusAlgorithm {
        fn name(&self) -> &'static str {
            "test_portus"
        }

        fn datapath_programs(&self) -> HashMap<&'static str, String> {
            let mut programs = HashMap::new();
            programs.insert("test_program", "test program content".to_string());
            programs
        }

        fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn Flow> {
            Box::new(TestPortusFlow {
                reports_received: 0,
            })
        }
    }

    struct TestPortusFlow {
        reports_received: u32,
    }

    impl Flow for TestPortusFlow {
        fn on_report(&mut self, _sock_id: u32, _report: Report) {
            self.reports_received += 1;
        }
    }

    #[tokio::test]
    async fn test_portus_compatibility() {
        let mut runtime = PortusCompatRuntime::new().await.unwrap();
        runtime
            .register_algorithm(TestPortusAlgorithm)
            .await
            .unwrap();

        // Test that the algorithm was registered
        let stats = runtime.lotus_runtime().get_stats().await;
        assert_eq!(stats.registered_algorithms, 1);
    }

    #[test]
    fn test_helper_functions() {
        let report = helpers::create_report(&[("cwnd", 10), ("rtt", 100)]);
        assert_eq!(report.get_field("cwnd"), Some(10));
        assert_eq!(report.get_field("rtt"), Some(100));

        let datapath_info = helpers::create_datapath_info(1, 0x01020304, 8080, 0x05060708, 80);
        assert_eq!(datapath_info.sock_id, 1);
        assert_eq!(datapath_info.src_port, 8080);

        let ip = helpers::ipv4_to_u32("192.168.1.1").unwrap();
        assert_eq!(ip, 0xC0A80101);
    }
}
