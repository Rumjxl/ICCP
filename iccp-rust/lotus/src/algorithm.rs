//! Algorithm trait definitions and execution framework
//!
//! This module provides both synchronous (Portus-compatible) and asynchronous
//! algorithm interfaces, allowing for seamless migration and optimal performance.

use crate::{flow::FlowContext, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Synchronous algorithm trait (Portus-compatible)
pub trait CongAlg<I>: Send + Sync + 'static {
    /// Algorithm name identifier
    fn name(&self) -> &'static str;

    /// Datapath programs for this algorithm
    fn datapath_programs(&self) -> HashMap<&'static str, String>;

    /// Create a new flow instance
    fn new_flow(
        &self,
        control: crate::flow::FlowContext<I>,
        info: DatapathInfo,
    ) -> Box<dyn crate::Flow>;
}

/// Asynchronous algorithm trait (Lotus-native)
#[async_trait]
pub trait AsyncCongAlg<I>: Send + Sync + 'static {
    /// Algorithm name identifier
    fn name(&self) -> &'static str;

    /// Datapath programs for this algorithm
    async fn datapath_programs(&self) -> HashMap<&'static str, String>;

    /// Create a new flow instance asynchronously
    async fn new_flow(
        &self,
        control: FlowContext<I>,
        info: DatapathInfo,
    ) -> Result<Box<dyn crate::AsyncFlow>>;

    /// Optional: Algorithm-specific initialization
    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Optional: Algorithm-specific cleanup
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Unified algorithm trait that supports both sync and async implementations
pub trait LotusAlgorithm: Send + Sync + 'static {
    type SyncImpl: Send + Sync + 'static;
    type AsyncImpl: Send + Sync + 'static;

    fn name(&self) -> &'static str;
}

/// Datapath information passed to algorithms
#[derive(Debug, Clone)]
pub struct DatapathInfo {
    pub sock_id: u32,
    pub init_cwnd: u32,
    pub mss: u32,
    pub src_ip: u32,
    pub src_port: u16,
    pub dst_ip: u32,
    pub dst_port: u16,
    /// 已编译并安装到内核的 datapath 程序映射：程序名 → program_uid
    pub programs: HashMap<String, u32>,
    /// 程序名 → 编译产生的 Scope（包含寄存器名 → Reg 映射）
    ///
    /// 用途：
    /// - `FlowContext::set_program_by_name()` 通过寄存器名查 `(reg_type, reg_index)`
    /// - `FlowContext::update_field_by_name()` 同上
    /// - `Report::get_field("Report.foo")` 通过寄存器名查 Report 字段下标
    pub scopes: HashMap<String, portus::lang::Scope>,
    /// 程序名 → 报告字段名列表（按程序中 `Report` 定义的顺序）
    pub report_fields: HashMap<String, Vec<String>>,
}

/// Report data from datapath
#[derive(Debug, Clone)]
pub struct Report {
    pub fields: HashMap<String, u64>,
    pub timestamp: std::time::Instant,
}

impl Report {
    pub fn get_field(&self, name: &str) -> Option<u64> {
        self.fields.get(name).copied()
    }

    pub fn set_field(&mut self, name: String, value: u64) {
        self.fields.insert(name, value);
    }
}

/// Algorithm execution context with timeout and cancellation support
pub struct AlgorithmContext {
    pub flow_id: crate::flow::FlowId,
    pub algorithm_name: String,
    pub timeout: Duration,
}

/// Algorithm executor that handles both sync and async algorithms
pub struct AlgorithmExecutor {
    timeout_duration: Duration,
}

impl AlgorithmExecutor {
    pub fn new(timeout_duration: Duration) -> Self {
        Self { timeout_duration }
    }

    /// Execute a synchronous algorithm with timeout
    pub async fn execute_sync<F, R>(&self, ctx: AlgorithmContext, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let flow_id = ctx.flow_id;
        let alg_name = ctx.algorithm_name.clone();

        debug!(flow_id = ?flow_id, algorithm = %alg_name, "Executing sync algorithm");

        match timeout(self.timeout_duration, tokio::task::spawn_blocking(f)).await {
            Ok(Ok(result)) => {
                debug!(flow_id = ?flow_id, algorithm = %alg_name, "Sync algorithm completed");
                Ok(result)
            }
            Ok(Err(join_error)) => {
                warn!(flow_id = ?flow_id, algorithm = %alg_name, error = %join_error, "Sync algorithm panicked");
                Err(crate::LotusError::Algorithm(format!(
                    "Algorithm panicked: {}",
                    join_error
                )))
            }
            Err(_timeout) => {
                warn!(flow_id = ?flow_id, algorithm = %alg_name, timeout = ?self.timeout_duration, "Sync algorithm timed out");
                Err(crate::LotusError::Algorithm(format!(
                    "Algorithm timed out after {:?}",
                    self.timeout_duration
                )))
            }
        }
    }

    /// Execute an asynchronous algorithm with timeout
    pub async fn execute_async<F, Fut, R>(&self, ctx: AlgorithmContext, f: F) -> Result<R>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let flow_id = ctx.flow_id;
        let alg_name = ctx.algorithm_name.clone();

        debug!(flow_id = ?flow_id, algorithm = %alg_name, "Executing async algorithm");

        match timeout(self.timeout_duration, f()).await {
            Ok(result) => {
                debug!(flow_id = ?flow_id, algorithm = %alg_name, "Async algorithm completed");
                Ok(result)
            }
            Err(_timeout) => {
                warn!(flow_id = ?flow_id, algorithm = %alg_name, timeout = ?self.timeout_duration, "Async algorithm timed out");
                Err(crate::LotusError::Algorithm(format!(
                    "Algorithm timed out after {:?}",
                    self.timeout_duration
                )))
            }
        }
    }
}

/// Algorithm registry for managing multiple algorithms
pub struct AlgorithmRegistry<I> {
    sync_algorithms: tokio::sync::RwLock<HashMap<String, Box<dyn CongAlg<I>>>>,
    async_algorithms: tokio::sync::RwLock<HashMap<String, Box<dyn AsyncCongAlg<I>>>>,
    executor: AlgorithmExecutor,
}

impl<I: Send + Sync + 'static> AlgorithmRegistry<I> {
    pub fn new(timeout_duration: Duration) -> Self {
        Self {
            sync_algorithms: tokio::sync::RwLock::new(HashMap::new()),
            async_algorithms: tokio::sync::RwLock::new(HashMap::new()),
            executor: AlgorithmExecutor::new(timeout_duration),
        }
    }

    pub async fn register_sync<A: CongAlg<I>>(&self, algorithm: A) {
        let name = algorithm.name().to_string();
        debug!(algorithm = %name, "Registering sync algorithm");
        self.sync_algorithms
            .write()
            .await
            .insert(name, Box::new(algorithm));
    }

    pub async fn register_async<A: AsyncCongAlg<I>>(&self, algorithm: A) {
        let name = algorithm.name().to_string();
        debug!(algorithm = %name, "Registering async algorithm");
        self.async_algorithms
            .write()
            .await
            .insert(name, Box::new(algorithm));
    }

    /// Create a flow using the named async algorithm, if registered.
    pub async fn create_async_flow(
        &self,
        algorithm_name: &str,
        context: crate::flow::FlowContext<I>,
        info: crate::algorithm::DatapathInfo,
    ) -> Option<Result<Box<dyn crate::flow::AsyncFlow>>> {
        let algs = self.async_algorithms.read().await;
        let alg = algs.get(algorithm_name)?;
        Some(alg.new_flow(context, info).await)
    }

    /// Create a flow using the named sync algorithm, if registered.
    pub async fn create_sync_flow(
        &self,
        algorithm_name: &str,
        context: crate::flow::FlowContext<I>,
        info: crate::algorithm::DatapathInfo,
    ) -> Option<Box<dyn crate::flow::Flow>> {
        let algs = self.sync_algorithms.read().await;
        let alg = algs.get(algorithm_name)?;
        Some(alg.new_flow(context, info))
    }

    pub async fn algorithm_count(&self) -> usize {
        let sync_count = self.sync_algorithms.read().await.len();
        let async_count = self.async_algorithms.read().await.len();
        sync_count + async_count
    }

    pub async fn list_algorithms(&self) -> Vec<String> {
        let mut algorithms = Vec::new();
        algorithms.extend(self.sync_algorithms.read().await.keys().cloned());
        algorithms.extend(self.async_algorithms.read().await.keys().cloned());
        algorithms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct TestFlow;

    impl crate::flow::Flow for TestFlow {
        fn on_report(&mut self, _sock_id: u32, _report: Report) {}
    }

    struct TestSyncAlg;

    impl CongAlg<()> for TestSyncAlg {
        fn name(&self) -> &'static str {
            "test_sync"
        }

        fn datapath_programs(&self) -> HashMap<&'static str, String> {
            HashMap::new()
        }

        fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn crate::Flow> {
            Box::new(TestFlow)
        }
    }

    #[tokio::test]
    async fn test_algorithm_registry() {
        let registry = AlgorithmRegistry::<()>::new(Duration::from_millis(100));
        registry.register_sync(TestSyncAlg).await;

        assert_eq!(registry.algorithm_count().await, 1);

        let algorithms = registry.list_algorithms().await;
        assert!(algorithms.contains(&"test_sync".to_string()));
    }
}
