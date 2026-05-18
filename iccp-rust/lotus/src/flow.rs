//! Flow management and state tracking
//!
//! This module provides concurrent flow management with support for both
//! synchronous and asynchronous flow processing.

use crate::{algorithm::Report, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Unique identifier for flows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlowId(Uuid);

impl FlowId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_connection(src_ip: u32, src_port: u16, dst_ip: u32, dst_port: u16) -> Self {
        // Create deterministic UUID from connection 5-tuple
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&src_ip.to_be_bytes());
        bytes[4..6].copy_from_slice(&src_port.to_be_bytes());
        bytes[6..10].copy_from_slice(&dst_ip.to_be_bytes());
        bytes[10..12].copy_from_slice(&dst_port.to_be_bytes());
        Self(Uuid::from_bytes(bytes))
    }
}

impl std::fmt::Display for FlowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Flow state information
#[derive(Debug, Clone)]
pub struct FlowState {
    pub flow_id: FlowId,
    pub sock_id: u32,
    pub algorithm_name: String,
    pub created_at: Instant,
    pub last_report_at: Option<Instant>,
    pub report_count: u64,
    pub src_ip: u32,
    pub src_port: u16,
    pub dst_ip: u32,
    pub dst_port: u16,
}

/// Flow control context for algorithm interaction
pub struct FlowContext<I> {
    pub flow_id: FlowId,
    pub sock_id: u32,
    pub sender: Arc<dyn crate::ipc::AsyncIpc<I>>,
    pub state: Arc<RwLock<FlowState>>,
}

/// `FlowContext` 内部全为 `Arc`，clone 仅做引用计数 +1，成本接近零。
/// fire-and-forget task 需要持有 `FlowContext` 来调用 `update_field`，
/// 而 `tokio::spawn` 要求 `'static + Send`，Clone 是必要条件。
impl<I: Default + Send + Sync + 'static> Clone for FlowContext<I> {
    fn clone(&self) -> Self {
        Self {
            flow_id: self.flow_id,
            sock_id: self.sock_id,
            sender: Arc::clone(&self.sender),
            state: Arc::clone(&self.state),
        }
    }
}

impl<I: Default + Send + Sync + 'static> FlowContext<I> {
    pub async fn send_control_message(&self, msg: &[u8]) -> Result<()> {
        let default_addr = I::default();
        self.sender
            .send(msg, &default_addr)
            .await
            .map_err(|e| crate::LotusError::Ipc(e.to_string()))
    }

    /// 向内核发送 CHANGE_PROG(type=4) 消息，切换当前流的激活 datapath 程序。
    ///
    /// # 参数
    /// - `program_uid`：目标程序的 UID（由 `DatapathInfo::programs` 查得）
    /// - `fields`：可选的寄存器初始值列表，每条为 `(reg_type, reg_index, value)`
    ///   - `reg_type=2, reg_index=4` 对应 `ReportTime`（Implicit register index 4）
    pub async fn set_program(&self, program_uid: u32, fields: &[(u8, u32, u64)]) -> Result<()> {
        use crate::serialize::{changeprog, serialize};
        let msg = changeprog::Msg::new_with_fields(self.sock_id, program_uid, fields.to_vec());
        let buf = serialize(&msg).map_err(|e| crate::LotusError::Ipc(e.to_string()))?;
        self.send_control_message(&buf).await
    }

    /// 向内核发送 UPDATE_FIELD(type=3) 消息，更新已激活程序的寄存器值。
    ///
    /// 主要用于更新 `Cwnd`（Implicit idx=4）和 `Rate`（Implicit idx=5）：
    ///
    /// ```rust,ignore
    /// // 设置 Cwnd = 14480 字节
    /// ctx.update_field(&[(2, 4, 14480)]).await?;
    /// // 设置 Rate = 2_000_000 bps
    /// ctx.update_field(&[(2, 5, 2_000_000)]).await?;
    /// ```
    ///
    /// # reg_type 对应关系（portus Reg tag bytes）
    /// - `0` = Control (non-volatile)
    /// - `2` = Implicit  ← Cwnd(idx=4) / Rate(idx=5) 使用此类型
    /// - `8` = Control (volatile)
    pub async fn update_field(&self, fields: &[(u8, u32, u64)]) -> Result<()> {
        use crate::serialize::{serialize, update_field};
        let msg = update_field::Msg {
            sid: self.sock_id,
            num_fields: fields.len() as u8,
            fields: fields.to_vec(),
        };
        let buf = serialize(&msg).map_err(|e| crate::LotusError::Ipc(e.to_string()))?;
        self.send_control_message(&buf).await
    }

    // ── 命名寄存器辅助方法 ──────────────────────────────────────────────

    /// 将 portus `Reg` 转换为 lotus 线路三元组 `(reg_type, reg_index)`。
    fn reg_to_type_index(reg: &portus::lang::Reg) -> Option<(u8, u32)> {
        use portus::lang::Reg;
        match reg {
            Reg::Control(i, _, volatile) => Some((if *volatile { 8u8 } else { 0u8 }, *i as u32)),
            Reg::Implicit(i, _) => Some((2u8, *i as u32)),
            Reg::Primitive(i, _) => Some((4u8, *i as u32)),
            Reg::Report(i, _, volatile) => Some((if *volatile { 5u8 } else { 6u8 }, *i as u32)),
            _ => None,
        }
    }

    /// 按程序名和字段名切换 datapath 程序（CHANGE_PROG）。
    ///
    /// - `info.programs["prog_name"]` 提供 program_uid
    /// - `info.scopes["prog_name"].get(field_name)` 提供 Reg，转为 `(reg_type, reg_index)`
    /// - `fields`: `&[("field_name", value)]`，value 为 u64
    ///
    /// # 示例
    /// ```rust,ignore
    /// ctx.set_program_by_name(&info, "default_prog",
    ///     &[("Control.report_time_us", 50_000)]).await?;
    /// ```
    pub async fn set_program_by_name(
        &self,
        info: &crate::algorithm::DatapathInfo,
        program_name: &str,
        fields: &[(&str, u64)],
    ) -> Result<()> {
        let uid = info.programs.get(program_name).copied().ok_or_else(|| {
            crate::LotusError::Algorithm(format!(
                "set_program_by_name: program '{}' not found in DatapathInfo.programs",
                program_name
            ))
        })?;

        let scope_opt = info.scopes.get(program_name);

        let mut raw_fields: Vec<(u8, u32, u64)> = Vec::with_capacity(fields.len());
        for &(name, value) in fields {
            let (rt, ri) = scope_opt
                .and_then(|sc| sc.get(name))
                .and_then(Self::reg_to_type_index)
                .ok_or_else(|| {
                    crate::LotusError::Algorithm(format!(
                        "set_program_by_name: field '{}' not found in scope for program '{}'",
                        name, program_name
                    ))
                })?;
            raw_fields.push((rt, ri, value));
        }

        self.set_program(uid, &raw_fields).await
    }

    /// 按字段名更新当前激活程序的寄存器（UPDATE_FIELD）。
    ///
    /// - `info.scopes["program_name"].get(field_name)` 提供 Reg，转为 `(reg_type, reg_index)`
    /// - `program_name` 为当前激活的程序名（通常在流创建时已知）
    ///
    /// # 示例
    /// ```rust,ignore
    /// ctx.update_field_by_name(&info, "default_prog",
    ///     &[("Cwnd", cwnd_bytes), ("Rate", rate_bps)]).await?;
    /// ```
    pub async fn update_field_by_name(
        &self,
        info: &crate::algorithm::DatapathInfo,
        program_name: &str,
        fields: &[(&str, u64)],
    ) -> Result<()> {
        let scope = info.scopes.get(program_name).ok_or_else(|| {
            crate::LotusError::Algorithm(format!(
                "update_field_by_name: scope for program '{}' not found",
                program_name
            ))
        })?;

        let mut raw_fields: Vec<(u8, u32, u64)> = Vec::with_capacity(fields.len());
        for &(name, value) in fields {
            let (rt, ri) = scope
                .get(name)
                .and_then(Self::reg_to_type_index)
                .ok_or_else(|| {
                    crate::LotusError::Algorithm(format!(
                        "update_field_by_name: field '{}' not found in scope for program '{}'",
                        name, program_name
                    ))
                })?;
            raw_fields.push((rt, ri, value));
        }

        self.update_field(&raw_fields).await
    }

    pub async fn update_state<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut FlowState),
    {
        let mut state = self.state.write().await;
        f(&mut *state);
        Ok(())
    }

    pub async fn get_state(&self) -> FlowState {
        self.state.read().await.clone()
    }
}

/// Synchronous flow trait (Portus-compatible)
pub trait Flow: Send + Sync + 'static {
    /// Handle measurement reports from datapath
    fn on_report(&mut self, sock_id: u32, report: Report);

    /// Handle flow closure
    fn close(&mut self) {}
}

/// Asynchronous flow trait (Lotus-native)
#[async_trait]
pub trait AsyncFlow: Send + Sync + 'static {
    /// Handle measurement reports from datapath asynchronously
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()>;

    /// Handle flow closure asynchronously
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Optional: Flow-specific initialization
    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Flow wrapper that can handle both sync and async flows
pub enum FlowWrapper {
    Sync(Box<dyn Flow>),
    Async(Box<dyn AsyncFlow>),
}

impl FlowWrapper {
    pub async fn handle_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        match self {
            FlowWrapper::Sync(flow) => {
                // Execute sync flow in blocking task
                let mut flow_taken = std::mem::replace(flow, Box::new(DummyFlow));
                let result = tokio::task::spawn_blocking(move || {
                    flow_taken.on_report(sock_id, report);
                    flow_taken
                })
                .await;

                match result {
                    Ok(returned_flow) => {
                        *flow = returned_flow;
                        Ok(())
                    }
                    Err(e) => Err(crate::LotusError::Flow(format!(
                        "Sync flow panicked: {}",
                        e
                    ))),
                }
            }
            FlowWrapper::Async(flow) => flow.on_report(sock_id, report).await,
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        match self {
            FlowWrapper::Sync(flow) => {
                let mut flow_taken = std::mem::replace(flow, Box::new(DummyFlow));
                tokio::task::spawn_blocking(move || {
                    flow_taken.close();
                })
                .await
                .map_err(|e| crate::LotusError::Flow(format!("Sync flow close panicked: {}", e)))?;
                Ok(())
            }
            FlowWrapper::Async(flow) => flow.close().await,
        }
    }
}

/// Dummy flow for temporary replacement during async operations
struct DummyFlow;

impl Flow for DummyFlow {
    fn on_report(&mut self, _sock_id: u32, _report: Report) {}
}

/// Concurrent flow manager
pub struct FlowManager<I> {
    flows: DashMap<FlowId, FlowWrapper>,
    flow_states: DashMap<FlowId, Arc<RwLock<FlowState>>>,
    flow_counter: AtomicU64,
    algorithm_registry: Arc<crate::algorithm::AlgorithmRegistry<I>>,
}

impl<I: Send + Sync + 'static> FlowManager<I> {
    pub fn new(algorithm_registry: Arc<crate::algorithm::AlgorithmRegistry<I>>) -> Self {
        Self {
            flows: DashMap::new(),
            flow_states: DashMap::new(),
            flow_counter: AtomicU64::new(0),
            algorithm_registry,
        }
    }

    /// Create a new flow with the specified algorithm
    pub async fn create_flow(
        &self,
        sock_id: u32,
        algorithm_name: &str,
        datapath_info: crate::algorithm::DatapathInfo,
        sender: Arc<dyn crate::ipc::AsyncIpc<I>>,
    ) -> Result<FlowId> {
        let flow_id = FlowId::from_connection(
            datapath_info.src_ip,
            datapath_info.src_port,
            datapath_info.dst_ip,
            datapath_info.dst_port,
        );

        let flow_state = FlowState {
            flow_id,
            sock_id,
            algorithm_name: algorithm_name.to_string(),
            created_at: Instant::now(),
            last_report_at: None,
            report_count: 0,
            src_ip: datapath_info.src_ip,
            src_port: datapath_info.src_port,
            dst_ip: datapath_info.dst_ip,
            dst_port: datapath_info.dst_port,
        };

        let state_arc = Arc::new(RwLock::new(flow_state));

        // Try to create flow with async algorithm first
        {
            let context = FlowContext {
                flow_id,
                sock_id,
                sender: sender.clone(),
                state: state_arc.clone(),
            };
            if let Some(result) = self
                .algorithm_registry
                .create_async_flow(algorithm_name, context, datapath_info.clone())
                .await
            {
                match result {
                    Ok(mut flow) => {
                        flow.initialize().await?;
                        self.flows.insert(flow_id, FlowWrapper::Async(flow));
                        self.flow_states.insert(flow_id, state_arc);
                        info!(flow_id = %flow_id, algorithm = %algorithm_name, "Created async flow");
                        return Ok(flow_id);
                    }
                    Err(e) => {
                        warn!(flow_id = %flow_id, algorithm = %algorithm_name, error = %e, "Failed to create async flow");
                    }
                }
            }
        }

        // Fallback to sync algorithm
        {
            let context = FlowContext {
                flow_id,
                sock_id,
                sender,
                state: state_arc.clone(),
            };
            if let Some(flow) = self
                .algorithm_registry
                .create_sync_flow(algorithm_name, context, datapath_info)
                .await
            {
                self.flows.insert(flow_id, FlowWrapper::Sync(flow));
                self.flow_states.insert(flow_id, state_arc);
                info!(flow_id = %flow_id, algorithm = %algorithm_name, "Created sync flow");
                return Ok(flow_id);
            }
        }

        Err(crate::LotusError::Algorithm(format!(
            "Algorithm '{}' not found",
            algorithm_name
        )))
    }

    /// Handle a report for a specific flow
    pub async fn handle_report(&self, flow_id: FlowId, sock_id: u32, report: Report) -> Result<()> {
        // Update flow state
        if let Some(state) = self.flow_states.get(&flow_id) {
            let mut state = state.write().await;
            state.last_report_at = Some(Instant::now());
            state.report_count += 1;
        }

        // Process report
        if let Some(mut flow_entry) = self.flows.get_mut(&flow_id) {
            debug!(flow_id = %flow_id, sock_id = sock_id, "Processing report");
            flow_entry.handle_report(sock_id, report).await?;
            Ok(())
        } else {
            warn!(flow_id = %flow_id, "Received report for unknown flow");
            Err(crate::LotusError::Flow(format!(
                "Flow {} not found",
                flow_id
            )))
        }
    }

    /// Close a flow
    pub async fn close_flow(&self, flow_id: FlowId) -> Result<()> {
        if let Some((_, mut flow)) = self.flows.remove(&flow_id) {
            flow.close().await?;
            self.flow_states.remove(&flow_id);
            info!(flow_id = %flow_id, "Closed flow");
            Ok(())
        } else {
            warn!(flow_id = %flow_id, "Attempted to close unknown flow");
            Err(crate::LotusError::Flow(format!(
                "Flow {} not found",
                flow_id
            )))
        }
    }

    /// Get flow statistics
    pub async fn get_flow_stats(&self, flow_id: FlowId) -> Option<FlowState> {
        self.flow_states.get(&flow_id)?.read().await.clone().into()
    }

    /// List all active flows
    pub fn list_flows(&self) -> Vec<FlowId> {
        self.flows.iter().map(|entry| *entry.key()).collect()
    }

    /// Get total number of active flows
    pub fn flow_count(&self) -> usize {
        self.flows.len()
    }

    /// Clean up inactive flows (flows that haven't reported in a while)
    pub async fn cleanup_inactive_flows(&self, max_idle_duration: std::time::Duration) -> usize {
        let now = Instant::now();
        let mut cleaned_up = 0;

        let inactive_flows: Vec<FlowId> = self
            .flow_states
            .iter()
            .filter_map(|entry| {
                let state = entry.value().try_read().ok()?;
                let last_activity = state.last_report_at.unwrap_or(state.created_at);
                if now.duration_since(last_activity) > max_idle_duration {
                    Some(state.flow_id)
                } else {
                    None
                }
            })
            .collect();

        for flow_id in inactive_flows {
            if self.close_flow(flow_id).await.is_ok() {
                cleaned_up += 1;
            }
        }

        if cleaned_up > 0 {
            info!(cleaned_up = cleaned_up, "Cleaned up inactive flows");
        }

        cleaned_up
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::{AlgorithmRegistry, CongAlg, DatapathInfo};
    use std::time::Duration;

    struct TestFlow {
        reports_received: u32,
    }

    impl Flow for TestFlow {
        fn on_report(&mut self, _sock_id: u32, _report: Report) {
            self.reports_received += 1;
        }
    }

    struct TestAlgorithm;

    impl CongAlg<()> for TestAlgorithm {
        fn name(&self) -> &'static str {
            "test"
        }

        fn datapath_programs(&self) -> HashMap<&'static str, String> {
            HashMap::new()
        }

        fn new_flow(&self, _control: FlowContext<()>, _info: DatapathInfo) -> Box<dyn Flow> {
            Box::new(TestFlow {
                reports_received: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_flow_creation() {
        let registry = AlgorithmRegistry::new(Duration::from_millis(100));
        registry.register_sync(TestAlgorithm).await;
        let registry = Arc::new(registry);

        let manager = FlowManager::new(registry);

        let datapath_info = DatapathInfo {
            sock_id: 1,
            init_cwnd: 10,
            mss: 1460,
            src_ip: 0x01020304,
            src_port: 8080,
            dst_ip: 0x05060708,
            dst_port: 80,
            programs: std::collections::HashMap::new(),
            scopes: std::collections::HashMap::new(),
            report_fields: std::collections::HashMap::new(),
        };

        // Mock sender
        struct MockSender;
        #[async_trait]
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
        let flow_id = manager
            .create_flow(1, "test", datapath_info, sender)
            .await
            .unwrap();

        assert_eq!(manager.flow_count(), 1);
        assert!(manager.list_flows().contains(&flow_id));
    }
}
