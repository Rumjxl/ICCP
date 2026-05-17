//! DatapathListener：CCP 消息分发主循环
//!
//! 精确移植 portus `run_inner()` 的全部语义到 async Rust。
//! 在独立 tokio task 中运行，通过 `AsyncIpc<()>` 与内核通信。
//!
//! ## 消息分发语义（与 portus run_inner 完全对齐）
//!
//! ```text
//! READY  → 关闭旧流 + 清空流表 → 广播所有 INSTALL 消息
//! CREATE → (首次时补发 INSTALL) → 路由算法 → new_flow → 写入流表
//! MEASURE(num_fields>0)  → on_report
//! MEASURE(num_fields==0) → close → 从流表删除
//! INSTALL                → unreachable（CCP 侧不收）
//! UPDATE_FIELD / Other   → warn + 忽略
//! ```

use crate::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext, FlowId, FlowState},
    ipc::AsyncIpc,
    manager::{AlgorithmManager, FlowKey},
    serialize::{create, measure, ready, Msg, SerializeError},
    LotusError,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ── 内部流记录 ────────────────────────────────────────────────────────

struct FlowEntry {
    #[allow(dead_code)]
    flow_id: FlowId,
    flow: Box<dyn AsyncFlow>,
    /// 当前激活的 datapath 程序名，用于在 MEASURE 时查对应 Scope
    active_program: String,
}

// ── DatapathListener ─────────────────────────────────────────────────

/// CCP 消息分发主循环
///
/// `I` 被包装在 `Arc<I>` 中，以便在 FlowContext.sender 中共享发送句柄。
pub struct DatapathListener<I: AsyncIpc<()>> {
    ipc: Arc<I>,
    flow_map: HashMap<u32, FlowEntry>,
    install_msgs: Vec<Vec<u8>>,
    /// 程序名 → program_uid
    uid_map: HashMap<String, u32>,
    /// 程序名 → 编译 Scope（寄存器名 → Reg，用于 name->Reg 查找）
    scope_map: HashMap<String, portus::lang::Scope>,
    /// 程序名 → 报告字段名列表（按程序中 Report 定义的顺序）
    report_fields: HashMap<String, Vec<String>>,
    alg_manager: Arc<AlgorithmManager>,
    algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>>,
    installed: bool,
}

impl<I: AsyncIpc<()>> DatapathListener<I> {
    pub fn new(
        ipc: I,
        install_msgs: Vec<Vec<u8>>,
        uid_map: HashMap<String, u32>,
        scope_map: HashMap<String, portus::lang::Scope>,
        report_fields: HashMap<String, Vec<String>>,
        alg_manager: Arc<AlgorithmManager>,
        algorithms: HashMap<String, Box<dyn AsyncCongAlg<()>>>,
    ) -> Self {
        Self {
            ipc: Arc::new(ipc),
            flow_map: HashMap::new(),
            install_msgs,
            uid_map,
            scope_map,
            report_fields,
            alg_manager,
            algorithms,
            installed: false,
        }
    }

    /// 主循环，永不正常返回（仅在 I/O 错误时返回 Err）
    pub async fn run(&mut self) -> crate::Result<std::convert::Infallible> {
        let mut buf = [0u8; 1024];
        info!("DatapathListener started");
        loop {
            let (n, _) = self.ipc.recv(&mut buf).await?;
            if n == 0 {
                continue; // SO_RCVTIMEO 超时，继续等待
            }
            let msg = match Msg::from_buf(&buf[..n]) {
                Ok((msg, _)) => msg,
                Err(e) => {
                    warn!(err = %e, "CCP deserialize error, skipping frame");
                    continue;
                }
            };
            if let Err(e) = self.dispatch(msg).await {
                warn!(err = %e, "dispatch error");
            }
        }
    }

    async fn dispatch(&mut self, msg: Msg) -> crate::Result<()> {
        match msg {
            Msg::Rdy(m) => self.handle_rdy(m).await,
            Msg::Cr(m) => self.handle_cr(m).await,
            Msg::Ms(m) => self.handle_ms(m).await,
            Msg::Ins(_) => {
                warn!("received unexpected INSTALL message on CCP side");
                Ok(())
            }
            Msg::Upd(m) => {
                warn!(sid = m.sid, "received UPDATE_FIELD, not yet handled");
                Ok(())
            }
            Msg::ChProg(m) => {
                debug!(
                    sid = m.sid,
                    uid = m.program_uid,
                    "received CHANGE_PROG (echo), ignoring"
                );
                Ok(())
            }
            Msg::Other(t) => {
                debug!(msg_type = t, "unknown CCP message type, ignoring");
                Ok(())
            }
        }
    }

    // ── READY ─────────────────────────────────────────────────────────

    async fn handle_rdy(&mut self, m: ready::Msg) -> crate::Result<()> {
        info!(
            id = m.id,
            old_flows = self.flow_map.len(),
            "READY from datapath"
        );
        self.close_all_flows().await;
        self.installed = false;
        self.broadcast_install().await
    }

    // ── CREATE ────────────────────────────────────────────────────────

    async fn handle_cr(&mut self, m: create::Msg) -> crate::Result<()> {
        if !self.installed {
            info!(
                sid = m.sid,
                "CREATE before READY, installing programs first"
            );
            self.broadcast_install().await?;
        }

        // 清理同 sid 的旧流
        if let Some(mut old) = self.flow_map.remove(&m.sid) {
            let _ = old.flow.close().await;
        }

        let flow_key = FlowKey {
            src_ip: m.src_ip,
            src_port: m.src_port as u16,
            dst_ip: m.dst_ip,
            dst_port: m.dst_port as u16,
        };

        // 算法路由优先级：
        //   1. CREATE 携带的 cong_alg 字段 — 但仅当该名称已在 self.algorithms 注册时才采用
        //      （内核发来的可能是 "cubic" 等系统默认值，并非 CCP 算法名）
        //   2. alg_manager 规则/默认值（由 start_netlink 设置为已注册算法名）
        let alg_name = m
            .cong_alg
            .as_deref()
            .filter(|name| self.algorithms.contains_key(*name))
            .map(str::to_string)
            .unwrap_or_else(|| self.alg_manager.get_algorithm(&flow_key));

        info!(
            sid = m.sid,
            alg = %alg_name,
            "creating new flow"
        );

        let alg = self.algorithms.get(&alg_name).ok_or_else(|| {
            LotusError::Algorithm(format!("algorithm '{}' not registered", alg_name))
        })?;

        let flow_id = FlowId::new();
        let info = DatapathInfo {
            sock_id: m.sid,
            init_cwnd: m.init_cwnd,
            mss: m.mss,
            src_ip: m.src_ip,
            src_port: m.src_port as u16,
            dst_ip: m.dst_ip,
            dst_port: m.dst_port as u16,
            programs: self.uid_map.clone(),
            scopes: self.scope_map.clone(),
            report_fields: self.report_fields.clone(),
        };

        let ctx = self.make_flow_context(flow_id, m.sid, &alg_name, &info);
        let flow = alg.new_flow(ctx, info).await?;
        self.flow_map.insert(
            m.sid,
            FlowEntry {
                flow_id,
                flow,
                active_program: String::new(),
            },
        );
        Ok(())
    }

    // ── MEASURE ───────────────────────────────────────────────────────

    async fn handle_ms(&mut self, m: measure::Msg) -> crate::Result<()> {
        // compute report before taking a mutable borrow of flow_map entry
        if self.flow_map.get(&m.sid).is_none() {
            debug!(sid = m.sid, "MEASURE for unknown flow, ignoring");
            return Ok(());
        }

        if m.num_fields == 0 {
            if let Some(mut entry) = self.flow_map.get_mut(&m.sid) {
                info!(sid = m.sid, "flow closed by datapath");
                let _ = entry.flow.close().await;
            }
            self.flow_map.remove(&m.sid);
            return Ok(());
        }

        let report = self.build_report(&m);
        if let Some(entry) = self.flow_map.get_mut(&m.sid) {
            let _ = entry.flow.on_report(m.sid, report).await;
        }
        Ok(())
    }

    // ── 辅助 ─────────────────────────────────────────────────────────

    async fn broadcast_install(&mut self) -> crate::Result<()> {
        for bytes in self.install_msgs.clone() {
            self.ipc.send(&bytes, &()).await?;
        }
        self.installed = true;
        info!(
            count = self.install_msgs.len(),
            "broadcast INSTALL messages"
        );
        Ok(())
    }

    async fn close_all_flows(&mut self) {
        let entries: Vec<_> = self.flow_map.drain().collect();
        for (sid, mut entry) in entries {
            let _ = entry.flow.close().await;
            debug!(sid, "closed old flow on READY");
        }
    }

    fn make_flow_context(
        &self,
        flow_id: FlowId,
        sock_id: u32,
        alg_name: &str,
        info: &DatapathInfo,
    ) -> FlowContext<()> {
        let state = Arc::new(RwLock::new(FlowState {
            flow_id,
            sock_id,
            algorithm_name: alg_name.to_string(),
            created_at: Instant::now(),
            last_report_at: None,
            report_count: 0,
            src_ip: info.src_ip,
            src_port: info.src_port,
            dst_ip: info.dst_ip,
            dst_port: info.dst_port,
        }));
        FlowContext {
            flow_id,
            sock_id,
            sender: self.ipc.clone() as Arc<dyn AsyncIpc<()>>,
            state,
        }
    }
    fn build_report(&self, m: &measure::Msg) -> Report {
        let mut fields = HashMap::new();

        // include program_uid for compatibility
        fields.insert("program_uid".to_string(), m.program_uid as u64);

        // try to find program name from uid_map
        let program_name_opt = self.uid_map.iter().find_map(|(name, &uid)| {
            if uid == m.program_uid {
                Some(name.clone())
            } else {
                None
            }
        });

        if let Some(program_name) = program_name_opt {
            if let Some(names) = self.report_fields.get(&program_name) {
                for (i, &v) in m.fields.iter().enumerate() {
                    if let Some(fname) = names.get(i) {
                        fields.insert(format!("Report.{}", fname), v);
                    } else if i == names.len() {
                        fields.insert("kernel_report_time_ns".to_string(), v);
                        fields.insert("Report.kernel_report_time_ns".to_string(), v);
                    } else {
                        fields.insert(format!("field_{}", i), v);
                    }
                }
                return Report {
                    fields,
                    timestamp: Instant::now(),
                };
            }
        }

        // fallback: anonymous field names
        for (i, &v) in m.fields.iter().enumerate() {
            fields.insert(format!("field_{}", i), v);
        }
        Report {
            fields,
            timestamp: Instant::now(),
        }
    }
}

// ── MockIpc（仅用于测试）─────────────────────────────────────────────

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::VecDeque;
    use tokio::sync::Mutex as TokioMutex;

    pub struct MockIpc {
        pub inbound: Arc<TokioMutex<VecDeque<Vec<u8>>>>,
        pub outbound: Arc<TokioMutex<Vec<Vec<u8>>>>,
    }

    impl MockIpc {
        pub fn new_with_handles() -> (
            Self,
            Arc<TokioMutex<VecDeque<Vec<u8>>>>,
            Arc<TokioMutex<Vec<Vec<u8>>>>,
        ) {
            let inbound = Arc::new(TokioMutex::new(VecDeque::new()));
            let outbound = Arc::new(TokioMutex::new(Vec::new()));
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
    impl AsyncIpc<()> for MockIpc {
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
            "mock"
        }
    }
}

// ── 单元测试 ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::mock::MockIpc;
    use super::*;
    use crate::serialize::serialize;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Default)]
    struct MockCounters {
        report_count: AtomicU32,
        close_count: AtomicU32,
    }

    struct MockFlow {
        counters: Arc<MockCounters>,
    }

    #[async_trait]
    impl AsyncFlow for MockFlow {
        async fn on_report(&mut self, _sid: u32, _r: Report) -> crate::Result<()> {
            self.counters.report_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn close(&mut self) -> crate::Result<()> {
            self.counters.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockAlg {
        counters: Arc<MockCounters>,
    }

    #[async_trait]
    impl AsyncCongAlg<()> for MockAlg {
        fn name(&self) -> &'static str {
            "mock"
        }
        async fn datapath_programs(&self) -> HashMap<&'static str, String> {
            HashMap::new()
        }
        async fn new_flow(
            &self,
            _ctx: FlowContext<()>,
            _info: DatapathInfo,
        ) -> crate::Result<Box<dyn AsyncFlow>> {
            Ok(Box::new(MockFlow {
                counters: self.counters.clone(),
            }))
        }
    }

    fn make_listener_with_install(
        ipc: MockIpc,
        counters: Arc<MockCounters>,
        install_msgs: Vec<Vec<u8>>,
    ) -> DatapathListener<MockIpc> {
        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert("mock".to_string(), Box::new(MockAlg { counters }));
        let mgr = Arc::new(AlgorithmManager::new());
        mgr.set_default_algorithm("mock".to_string());
        DatapathListener::new(
            ipc,
            install_msgs,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            mgr,
            algs,
        )
    }

    fn make_listener(ipc: MockIpc, counters: Arc<MockCounters>) -> DatapathListener<MockIpc> {
        make_listener_with_install(ipc, counters, vec![])
    }

    fn ready_frame() -> Vec<u8> {
        serialize(&ready::Msg { id: 0 }).unwrap()
    }

    fn create_frame(sid: u32) -> Vec<u8> {
        serialize(&create::Msg {
            sid,
            init_cwnd: 10000,
            mss: 1448,
            src_ip: 0,
            src_port: 5001,
            dst_ip: 0,
            dst_port: 80,
            cong_alg: None,
        })
        .unwrap()
    }

    fn measure_frame(sid: u32, fields: Vec<u64>) -> Vec<u8> {
        let num = fields.len() as u8;
        serialize(&measure::Msg {
            sid,
            program_uid: 1,
            num_fields: num,
            fields,
        })
        .unwrap()
    }

    fn measure_close_frame(sid: u32) -> Vec<u8> {
        serialize(&measure::Msg {
            sid,
            program_uid: 1,
            num_fields: 0,
            fields: vec![],
        })
        .unwrap()
    }

    /// P1-1: READY → 旧流表清空，install_msgs 全部广播
    #[tokio::test]
    async fn test_ready_clears_flowmap_and_broadcasts() {
        let install_bytes = vec![0xABu8; 20];
        let (ipc, _inbound, outbound) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener_with_install(ipc, counters, vec![install_bytes.clone()]);

        // 建一条流（触发首次 install）
        let (msg, _) = Msg::from_buf(&create_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();
        assert_eq!(listener.flow_map.len(), 1);

        // 发 READY
        let (msg, _) = Msg::from_buf(&ready_frame()).unwrap();
        listener.dispatch(msg).await.unwrap();

        assert_eq!(
            listener.flow_map.len(),
            0,
            "flow_map should be empty after READY"
        );
        let sent = outbound.lock().await.clone();
        let count = sent
            .iter()
            .filter(|f| f.as_slice() == install_bytes.as_slice())
            .count();
        assert!(count >= 1, "install_msgs should have been broadcast");
    }

    /// P1-2: CREATE → flow_map 中写入正确的 sid
    #[tokio::test]
    async fn test_create_inserts_flow() {
        let (ipc, _, _) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener(ipc, counters);

        let (msg, _) = Msg::from_buf(&create_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();

        assert_eq!(listener.flow_map.len(), 1);
        assert!(listener.flow_map.contains_key(&1));
    }

    /// P1-3: MEASURE(num_fields>0) → on_report 被调用
    #[tokio::test]
    async fn test_measure_calls_on_report() {
        let (ipc, _, _) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener(ipc, counters.clone());

        let (msg, _) = Msg::from_buf(&create_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();

        let (msg, _) = Msg::from_buf(&measure_frame(1, vec![42, 100])).unwrap();
        listener.dispatch(msg).await.unwrap();

        assert_eq!(counters.report_count.load(Ordering::SeqCst), 1);
    }

    /// P1-4: MEASURE(num_fields==0) → close 被调用，流从 flow_map 删除
    #[tokio::test]
    async fn test_measure_close_removes_flow() {
        let (ipc, _, _) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener(ipc, counters.clone());

        let (msg, _) = Msg::from_buf(&create_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();
        assert_eq!(listener.flow_map.len(), 1);

        let (msg, _) = Msg::from_buf(&measure_close_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();

        assert_eq!(listener.flow_map.len(), 0);
        assert_eq!(counters.close_count.load(Ordering::SeqCst), 1);
    }

    /// P1-5: 损坏帧 → SerializeError::Truncated，不 panic
    #[tokio::test]
    async fn test_corrupt_frame_truncated_error() {
        let corrupt = vec![0xFFu8; 5];
        match Msg::from_buf(&corrupt) {
            Err(SerializeError::Truncated { .. }) => {}
            other => panic!("expected Truncated, got {:?}", other),
        }
    }

    /// P1-6: 对未知 sid 的 MEASURE 静默忽略
    #[tokio::test]
    async fn test_measure_unknown_sid_ignored() {
        let (ipc, _, _) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener(ipc, counters.clone());

        let (msg, _) = Msg::from_buf(&measure_frame(99, vec![1, 2, 3])).unwrap();
        listener.dispatch(msg).await.unwrap();

        assert_eq!(counters.report_count.load(Ordering::SeqCst), 0);
    }

    /// P1-7: READY 前首次 CREATE 会触发 install_msgs 广播
    #[tokio::test]
    async fn test_first_create_broadcasts_install() {
        let install_bytes = vec![0x11u8; 16];
        let (ipc, _, outbound) = MockIpc::new_with_handles();
        let counters = Arc::new(MockCounters::default());
        let mut listener = make_listener_with_install(ipc, counters, vec![install_bytes.clone()]);

        let (msg, _) = Msg::from_buf(&create_frame(1)).unwrap();
        listener.dispatch(msg).await.unwrap();

        let sent = outbound.lock().await.clone();
        assert!(
            sent.iter()
                .any(|f| f.as_slice() == install_bytes.as_slice()),
            "first CREATE should broadcast install_msgs"
        );
    }

    // ── ENOBUFS 复现测试 ─────────────────────────────────────────────

    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Mutex as TokioMutex;

    /// 模拟真实 netlink 场景的 IPC：
    ///
    /// - 前 `normal_count` 次 recv 正常返回队列中的帧
    /// - 之后当队列为空时，若 `enobufs_after_empty` 为 true，则返回 ENOBUFS 错误
    ///   （模拟内核侧接收缓冲区已满、用户侧未能及时消费时的内核行为）
    struct EnobufsIpc {
        /// 消息队列，模拟内核 netlink 接收缓冲区
        inbound: Arc<TokioMutex<VecDeque<Vec<u8>>>>,
        outbound: Arc<TokioMutex<Vec<Vec<u8>>>>,
        /// 成功 recv 计数
        recv_count: Arc<AtomicUsize>,
        /// 当队列耗尽后再 recv，触发 ENOBUFS 而非返回 (0, ())
        enobufs_trigger: Arc<std::sync::atomic::AtomicBool>,
    }

    impl EnobufsIpc {
        fn new() -> (
            Self,
            Arc<TokioMutex<VecDeque<Vec<u8>>>>,
            Arc<TokioMutex<Vec<Vec<u8>>>>,
            Arc<std::sync::atomic::AtomicBool>,
        ) {
            let inbound = Arc::new(TokioMutex::new(VecDeque::new()));
            let outbound = Arc::new(TokioMutex::new(Vec::new()));
            let trigger = Arc::new(std::sync::atomic::AtomicBool::new(false));
            (
                Self {
                    inbound: inbound.clone(),
                    outbound: outbound.clone(),
                    recv_count: Arc::new(AtomicUsize::new(0)),
                    enobufs_trigger: trigger.clone(),
                },
                inbound,
                outbound,
                trigger,
            )
        }
    }

    #[async_trait]
    impl AsyncIpc<()> for EnobufsIpc {
        async fn send(&self, msg: &[u8], _: &()) -> crate::Result<()> {
            self.outbound.lock().await.push(msg.to_vec());
            Ok(())
        }

        async fn recv(&self, buf: &mut [u8]) -> crate::Result<(usize, ())> {
            let mut q = self.inbound.lock().await;
            if let Some(frame) = q.pop_front() {
                self.recv_count.fetch_add(1, Ordering::SeqCst);
                let n = frame.len().min(buf.len());
                buf[..n].copy_from_slice(&frame[..n]);
                return Ok((n, ()));
            }
            // 队列为空——检查是否应触发 ENOBUFS
            if self.enobufs_trigger.load(Ordering::SeqCst) {
                // 模拟 Linux netlink 当接收缓冲区满时 recvmsg 返回 ENOBUFS (errno=105)
                // portus ipc/netlink.rs __recv 将其包装为:
                //   portus err: ENOBUFS: No buffer space available
                return Err(crate::LotusError::Ipc(
                    "portus err: ENOBUFS: No buffer space available".to_string(),
                ));
            }
            Ok((0, ()))
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "enobufs_mock"
        }
    }

    /// `EnobufsIpc` 的"修复版"变体：ENOBUFS 时返回 Ok((0,())) 而非 Err，
    /// 模拟 ipc_netlink 层豁免逻辑修复后的行为，用于 P1-9 验证测试。
    struct FixedEnobufsIpc {
        inbound: Arc<TokioMutex<VecDeque<Vec<u8>>>>,
        outbound: Arc<TokioMutex<Vec<Vec<u8>>>>,
        recv_count: Arc<AtomicUsize>,
        enobufs_trigger: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FixedEnobufsIpc {
        fn new() -> (
            Self,
            Arc<TokioMutex<VecDeque<Vec<u8>>>>,
            Arc<TokioMutex<Vec<Vec<u8>>>>,
            Arc<std::sync::atomic::AtomicBool>,
        ) {
            let inbound = Arc::new(TokioMutex::new(VecDeque::new()));
            let outbound = Arc::new(TokioMutex::new(Vec::new()));
            let trigger = Arc::new(std::sync::atomic::AtomicBool::new(false));
            (
                Self {
                    inbound: inbound.clone(),
                    outbound: outbound.clone(),
                    recv_count: Arc::new(AtomicUsize::new(0)),
                    enobufs_trigger: trigger.clone(),
                },
                inbound,
                outbound,
                trigger,
            )
        }
    }

    #[async_trait]
    impl AsyncIpc<()> for FixedEnobufsIpc {
        async fn send(&self, msg: &[u8], _: &()) -> crate::Result<()> {
            self.outbound.lock().await.push(msg.to_vec());
            Ok(())
        }

        async fn recv(&self, buf: &mut [u8]) -> crate::Result<(usize, ())> {
            let mut q = self.inbound.lock().await;
            if let Some(frame) = q.pop_front() {
                self.recv_count.fetch_add(1, Ordering::SeqCst);
                let n = frame.len().min(buf.len());
                buf[..n].copy_from_slice(&frame[..n]);
                return Ok((n, ()));
            }
            if self.enobufs_trigger.load(Ordering::SeqCst) {
                // ✅ 修复后行为：ENOBUFS 在 ipc_netlink 层被豁免，返回 Ok((0,()))
                return Ok((0, ()));
            }
            Ok((0, ()))
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "fixed_enobufs_mock"
        }
    }

    fn make_listener_enobufs(ipc: EnobufsIpc) -> DatapathListener<EnobufsIpc> {
        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert(
            "mock".to_string(),
            Box::new(MockAlg {
                counters: Arc::new(MockCounters::default()),
            }),
        );
        let mgr = Arc::new(AlgorithmManager::new());
        mgr.set_default_algorithm("mock".to_string());
        DatapathListener::new(
            ipc,
            vec![],
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            mgr,
            algs,
        )
    }

    fn make_listener_fixed_enobufs(ipc: FixedEnobufsIpc) -> DatapathListener<FixedEnobufsIpc> {
        let mut algs: HashMap<String, Box<dyn AsyncCongAlg<()>>> = HashMap::new();
        algs.insert(
            "mock".to_string(),
            Box::new(MockAlg {
                counters: Arc::new(MockCounters::default()),
            }),
        );
        let mgr = Arc::new(AlgorithmManager::new());
        mgr.set_default_algorithm("mock".to_string());
        DatapathListener::new(
            ipc,
            vec![],
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            mgr,
            algs,
        )
    }

    /// P1-8: 复现 ENOBUFS 导致 DatapathListener::run() 异常退出
    ///
    /// 场景：4 条双向流（8 个 CCP 连接），只有发送方频繁上报 MEASURE。
    /// 模拟内核 netlink 接收缓冲区积压满后，recv 返回 ENOBUFS。
    ///
    /// 预期（buggy 行为）：run() 因 ENOBUFS 错误返回 Err，listener 意外退出。
    #[tokio::test]
    async fn test_enobufs_causes_listener_exit_repro() {
        let (ipc, inbound, _outbound, enobufs_trigger) = EnobufsIpc::new();
        let recv_count = ipc.recv_count.clone();

        // 模拟 4 条双向流的 8 个 CREATE 消息（只有发送方活跃，对应 sid 1-4）
        {
            let mut q = inbound.lock().await;
            for sid in 1u32..=8 {
                q.push_back(create_frame(sid));
            }
            // 模拟高频 MEASURE（只有发送方 sid 1-4 频繁上报，共 200 条）
            for _ in 0..50 {
                for sid in 1u32..=4 {
                    q.push_back(measure_frame(sid, vec![1000, 500, 100]));
                }
            }
        }

        // 所有消息入队后，激活 ENOBUFS 触发器
        // 模拟：消息消费速度跟不上内核产生速度，缓冲区已溢出
        enobufs_trigger.store(true, Ordering::SeqCst);

        let mut listener = make_listener_enobufs(ipc);

        // run() 应该因 ENOBUFS 错误退出（复现 bug）
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(500), listener.run()).await;

        match result {
            Ok(Err(crate::LotusError::Ipc(msg))) => {
                // ✅ 复现成功：listener 因 ENOBUFS 退出
                assert!(
                    msg.contains("ENOBUFS") || msg.contains("No buffer space"),
                    "error should mention ENOBUFS, got: {msg}"
                );
                println!(
                    "[REPRO] listener exited with ENOBUFS after {} successful recvs",
                    recv_count.load(Ordering::SeqCst)
                );
            }
            Ok(Err(other)) => {
                panic!("run() exited with unexpected error: {other}");
            }
            Ok(Ok(_)) => {
                panic!("run() returned Ok(Infallible) — impossible");
            }
            Err(_timeout) => {
                panic!(
                    "listener did NOT exit on ENOBUFS (timeout) — \
                     this is the expected behavior AFTER the fix; \
                     run test_enobufs_listener_survives instead"
                );
            }
        }
    }

    /// P1-9: ENOBUFS 后 listener 应继续存活（修复验证测试）
    ///
    /// 使用 `FixedEnobufsIpc`（ENOBUFS 时返回 `Ok((0,()))`，模拟 ipc_netlink 层
    /// 豁免逻辑修复后的行为），验证 DatapathListener 在收到 `(0, ())` 后能正常
    /// 继续处理后续帧，不会退出。
    #[tokio::test]
    async fn test_enobufs_listener_survives() {
        let (ipc, inbound, _outbound, enobufs_trigger) = FixedEnobufsIpc::new();
        let recv_count = ipc.recv_count.clone();

        // 先激活 ENOBUFS（修复层已豁免），再推入 8 个 CREATE + 200 条 MEASURE
        enobufs_trigger.store(true, Ordering::SeqCst);

        {
            let mut q = inbound.lock().await;
            for sid in 1u32..=8 {
                q.push_back(create_frame(sid));
            }
            for _ in 0..50 {
                for sid in 1u32..=4 {
                    q.push_back(measure_frame(sid, vec![1000, 500, 100]));
                }
            }
        }

        let mut listener = make_listener_fixed_enobufs(ipc);

        // 修复后，run() 遇到 ENOBUFS 应继续，直到超时
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(200), listener.run()).await;

        match result {
            Err(_timeout) => {
                // ✅ 修复验证通过：listener 在 ENOBUFS 后继续运行
                println!(
                    "[FIX OK] listener survived ENOBUFS, processed {} frames",
                    recv_count.load(Ordering::SeqCst)
                );
            }
            Ok(Err(e)) => {
                panic!("listener should NOT exit on ENOBUFS after fix, got: {e}");
            }
            Ok(Ok(_)) => {
                panic!("run() returned Ok(Infallible) — impossible");
            }
        }
    }
}
