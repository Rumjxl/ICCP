//! DTCC — Deep/Distributed Traffic Congestion Control
//!
//! 该模块将原先基于 portus 同步框架的 DTCC 实现迁移到
//! **lotus** 异步框架（`AsyncCongAlg` / `AsyncFlow`），
//! 完全消除 `block_on`、`LocalSet`、`Rc<Runtime>` 等同步阻塞原语。
//!
//! ## capnp-rpc 与异步框架的集成方式
//!
//! `capnp_rpc::Client` 底层的 `ClientHook` 不实现 `Send + Sync`，
//! 因此无法直接存入 `Arc<RwLock<…>>` 并跨线程共享。
//! 解决方案：**channel 代理模式（共享 + 并发 + 信号量限流）**——
//!
//! ```text
//!  N 条 DtccFlow（任意线程）
//!    │  try_send AgentRequest  →  共享 mpsc::Sender（容量 C）
//!    │  立即返回，spawn task 等结果
//!    ▼
//!  agent_task（单线程 LocalSet，所有流共享一个 TCP 连接）
//!    │  for each 请求：try_acquire Semaphore(max_inflight) → spawn_local
//!    │  最多 max_inflight 个 RPC 并发 in-flight
//!    │  多个 RPC 同时发往 Python → BatchProcessor 真正聚合
//!    ▼  oneshot::Sender
//!  tokio::spawn task ← AgentResponse(Option<cwnd>)
//!    │  写回 Arc<AtomicU32> cwnd
//!    └► FlowContext::update_field（一次 send 携带 [cwnd, rate] 两字段）
//! ```
//!
//! ### 关键改进（相对旧版）
//!
//! | 维度 | 旧版 | 新版 |
//! |------|------|------|
//! | agent_task 数量 | N 条流 → N 个线程 + N 条 TCP | 1 个线程 + 1 条 TCP（所有流共享） |
//! | RPC 并发度 | 串行（每次 await 一个） | 并发（spawn_local，信号量限流） |
//! | on_report 延迟 | 20ms（等 RPC 响应） | ~0ms（fire-and-forget） |
//! | DatapathListener 吞吐 | N × 20ms/轮 | ~0ms/轮，受限于 netlink recv |
//! | Python batch 聚合 | 难以聚合（8 个独立连接） | 自然聚合（并发 RPC 同时到达） |
//! | channel 容量 | 固定 128 | 参数化：≥ N × ceil(Trpc/Treport) × 1.5 |
//! | capnp 流控 | 无限并发可能打爆 | Semaphore(max_inflight≈N×rounds) 限流 |
//! | netlink UPDATE 次数 | cwnd+rate 各一次（2次/流） | 合并为单次 send（1次/流）|
//!
//! ### channel 容量选取公式
//!
//! ```text
//! C = max(128, N × ceil(T_rpc / T_report) × 1.5)
//!
//! 示例（T_report=10ms）：
//!   N=8,   T_rpc=3ms  → C=max(128, 8×1×1.5)=128
//!   N=50,  T_rpc=5ms  → C=max(128, 50×1×1.5)=128
//!   N=200, T_rpc=10ms → C=max(128, 200×1×1.5)=300
//!   N=200, T_rpc=15ms → C=max(128, 200×2×1.5)=600
//! ```
//!
//! ### max_inflight（Semaphore 许可数）选取
//!
//! max_inflight 跟随 `ceil(T_rpc / T_report)` 放大，保证一个 RPC 周期内
//! 各 flow 的上报不会被固定的小并发门限压住；当许可耗尽时请求立即 fallback，
//! 避免在 `spawn_local` task 中形成隐藏排队。

use async_trait::async_trait;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};
use tokio::net::TcpStream;

// ─── capnp 生成代码 ──────────────────────────────────────────────────────────
mod ccp_capnp {
    include!(concat!(env!("OUT_DIR"), "/ccp_capnp.rs"));
}

// ─── lotus 公共接口 ───────────────────────────────────────────────────────────
use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    LotusError, Result as LotusResult,
};

const USEC_PER_SEC: u64 = 1_000_000;

// ─── channel 代理消息类型 ─────────────────────────────────────────────────────

/// RL 智能体观察值（从 datapath Report 中提取，可安全跨线程传递）
#[derive(Debug, Clone)]
pub struct AgentObservation {
    pub bytes_acked: u64,
    pub loss: u64,
    pub rtt: u64,
    pub rttvar: u64,
    pub castate: u64,
    pub minrtt: u64,
    pub rate_delivery: u64,
    pub packets_unacked: u64,
    pub snd_mss: u64,
    pub packets_delivered: u64,
    pub bytes_sent_delta: u64,
    pub snd_cwnd: u64,
    pub time_delta: u64,
    pub duration: u64,
    pub is_new_flow: bool,
}

/// 传给 agent_task 的请求（含回传 channel）
struct AgentRequest {
    conn_id: u64,
    obs: AgentObservation,
    tx: oneshot::Sender<Option<u64>>,
    is_new_flow: bool,
}

fn pacing_rate_from_cwnd(cwnd_bytes: u64, rtt_us: u64, minrtt_us: u64) -> Option<u64> {
    let current_rtt_rate = if rtt_us > 0 {
        Some(cwnd_bytes.saturating_mul(USEC_PER_SEC) / rtt_us)
    } else {
        None
    };
    let minrtt_rate = if minrtt_us > 0 {
        Some(cwnd_bytes.saturating_mul(2).saturating_mul(USEC_PER_SEC) / minrtt_us)
    } else {
        None
    };

    match (current_rtt_rate, minrtt_rate) {
        (Some(current_rtt_rate), Some(minrtt_rate)) => Some(current_rtt_rate.min(minrtt_rate)),
        (Some(current_rtt_rate), None) => Some(current_rtt_rate),
        (None, Some(minrtt_rate)) => Some(minrtt_rate),
        (None, None) => None,
    }
    .map(|rate| rate.max(1).min(u32::MAX as u64))
}

// ─── 配置结构 ─────────────────────────────────────────────────────────────────

/// 控制 datapath 上报时机
#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
}

/// DTCC 算法配置（lotus `AsyncCongAlg` 工厂）
///
/// `agent_tx` 使用 `OnceLock` 实现懒初始化：第一次 `new_flow` 时连接 Python agent，
/// 此后所有流共享同一 `mpsc::Sender`，对应同一个 `agent_task` 线程和同一条 TCP 连接。
///
/// ### channel_capacity 参数选取（公式）
///
/// ```text
/// C = max(128, N × ceil(T_rpc_ms / T_report_ms) × 1.5)
/// ```
/// 不确定时使用 [`Dtcc::for_flows`] 自动计算。
///
/// ### max_inflight 参数选取
///
/// `max_inflight` 跟随 `ceil(T_rpc / T_report)` 放大，并保留全局上限；
/// 当许可耗尽时 agent_task 立即 fallback，避免额外隐藏排队层。
pub struct Dtcc {
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
    /// mpsc channel 容量（建议 ≥ N × ceil(T_rpc/T_report) × 1.5，最小 128）
    pub channel_capacity: usize,
    /// 并发 in-flight capnp RPC 上限（Semaphore 许可数，建议约 N × ceil(T_rpc/T_report)）
    pub max_inflight: usize,
    /// agent_task 侧单次 capnp RPC 的超时时间。
    pub rpc_timeout: Duration,
    /// fire-and-forget task 侧等待 Python 响应的超时时间。
    pub resp_timeout: Duration,
    /// 所有流共享的 agent channel 发送端（懒初始化，仅创建一次）
    agent_tx: Arc<OnceLock<mpsc::Sender<AgentRequest>>>,
}

impl Default for Dtcc {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4826".to_string(),
            init_cwnd: 10,
            report_option: ConfigReport::Interval(Duration::from_millis(10)),
            channel_capacity: 256,
            max_inflight: 128,
            rpc_timeout: Duration::from_millis(20),
            resp_timeout: Duration::from_millis(30),
            agent_tx: Arc::new(OnceLock::new()),
        }
    }
}

impl Dtcc {
    /// 创建 `Dtcc` 实例（手动指定容量参数）。
    ///
    /// `agent_tx` 由内部懒初始化，无需外部传入。
    pub fn new(server_addr: String, init_cwnd: u32, report_option: ConfigReport) -> Self {
        Self {
            server_addr,
            init_cwnd,
            report_option,
            ..Default::default()
        }
    }

    /// 根据预期流数量和 RPC 时间自动计算合理的 channel 容量。
    ///
    /// # 参数
    /// - `n_flows`：预期并发流数量
    /// - `rpc_ms`：Python agent 单次（批量）推理延迟（毫秒）
    /// - `report_ms`：内核上报间隔（毫秒，通常为 10）
    ///
    /// # 示例
    /// ```rust,ignore
    /// // N=200 条流，Python 推理 ~10ms，上报间隔 10ms
    /// let dtcc = Dtcc::for_flows("127.0.0.1:4826", 10, report, 200, 10, 10);
    /// ```
    pub fn for_flows(
        server_addr: String,
        init_cwnd: u32,
        report_option: ConfigReport,
        n_flows: usize,
        rpc_ms: usize,
        report_ms: usize,
        rpc_timeout_ms: Option<u64>,
        resp_timeout_ms: Option<u64>,
    ) -> Self {
        // ceil(T_rpc / T_report)：RPC 完成期间内核会上报几轮
        let rounds = rpc_ms.div_ceil(report_ms).max(1);
        // 每轮 n_flows 个请求，乘以 1.5 作为安全余量，最小 128
        let channel_capacity = (((n_flows * rounds) as f64 * 1.5) as usize).max(128);
        // 一个 RPC 周期内最多会积累 rounds 轮上报；并发许可跟随 rounds 放大，
        // 避免固定小上限把请求压在 semaphore 前形成长尾等待。
        let max_inflight = (n_flows * rounds).clamp(1, 512);
        // 默认兼容旧逻辑：rpc_timeout = rpc_ms × 2，resp_timeout = rpc_timeout + 10ms。
        // 实验时可通过 CLI 显式覆盖这两个 timeout。
        let rpc_timeout_ms = rpc_timeout_ms.unwrap_or((rpc_ms * 2) as u64);
        let resp_timeout_ms = resp_timeout_ms.unwrap_or(rpc_timeout_ms + 10);
        let rpc_timeout = Duration::from_millis(rpc_timeout_ms);
        let resp_timeout = Duration::from_millis(resp_timeout_ms);
        debug!(
            n_flows,
            rpc_ms,
            report_ms,
            rounds,
            channel_capacity,
            max_inflight,
            rpc_timeout_ms = rpc_timeout.as_millis(),
            resp_timeout_ms = resp_timeout.as_millis(),
            "Dtcc::for_flows: computed capacity parameters"
        );
        Self {
            server_addr,
            init_cwnd,
            report_option,
            channel_capacity,
            max_inflight,
            rpc_timeout,
            resp_timeout,
            agent_tx: Arc::new(OnceLock::new()),
        }
    }
}

// ─── lotus AsyncCongAlg 实现 ──────────────────────────────────────────────────

#[async_trait]
impl AsyncCongAlg<()> for Dtcc {
    fn name(&self) -> &'static str {
        "DTCC"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::new();

        h.insert(
            "DtccDatapathInterval",
            "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_in_flight)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)
                    (fallthrough)
                )
                (when (> Micros ReportTime)
                    (report)
                    (:= Micros 0)
                )
            "
            .to_string(),
        );

        h.insert(
            "DtccDatapathIntervalRTT",
            "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_unacked)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)
                    (fallthrough)
                )
                (when (|| Flow.was_timeout (> Report.loss 0))
                    (report)
                    (:= Micros 0)
                )
                (when (> Micros Flow.rtt_sample_us)
                    (report)
                    (:= Micros 0)
                )
            "
            .to_string(),
        );

        h.insert(
            "DtccDatapathIntervalAck",
            "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile rttvar 0)
                        (volatile castate 0)
                        (volatile minrtt +infinity)
                        (volatile rate_delivery 0)
                        (volatile packets_unacked 0)
                        (volatile snd_mss 0)
                        (volatile packets_delivered 0)
                        (volatile bytes_sent 0)
                    )
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rttvar Flow.rttvar)
                    (:= Report.castate Flow.castate)
                    (:= Report.minrtt Flow.min_rtt)
                    (:= Report.rate_delivery Flow.rate_delivery)
                    (:= Report.packets_unacked Flow.packets_unacked)
                    (:= Report.snd_mss Flow.snd_mss)
                    (:= Report.packets_delivered Flow.packets_delivered)
                    (:= Report.bytes_sent Flow.bytes_sent)
                    (report)
                )
            "
            .to_string(),
        );

        h
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        info: DatapathInfo,
    ) -> LotusResult<Box<dyn AsyncFlow>> {
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };

        debug!(
            sock_id = info.sock_id,
            src_ip = Ipv4Addr::from(info.src_ip.to_be()).to_string(),
            src_port = info.src_port,
            dst_ip = Ipv4Addr::from(info.dst_ip.to_be()).to_string(),
            dst_port = info.dst_port,
            init_cwnd,
            "DTCC: new_flow"
        );

        // 懒初始化：首次调用时启动 agent_task，之后所有流共享同一 Sender
        let channel_capacity = self.channel_capacity;
        let max_inflight = self.max_inflight;
        let rpc_timeout = self.rpc_timeout;
        let resp_timeout = self.resp_timeout;
        let server_addr = self.server_addr.clone();
        let agent_tx = self
            .agent_tx
            .get_or_init(|| {
                info!(
                    %server_addr,
                    channel_capacity,
                    max_inflight,
                    rpc_timeout_ms = rpc_timeout.as_millis(),
                    resp_timeout_ms = resp_timeout.as_millis(),
                    "DTCC: spawning shared agent_task"
                );
                spawn_agent_task(server_addr, rpc_timeout, channel_capacity, max_inflight)
            })
            .clone();

        DtccFlow::init(
            control,
            info,
            agent_tx,
            resp_timeout,
            self.report_option,
            init_cwnd,
        )
        .await
        .map(|f| Box::new(f) as Box<dyn AsyncFlow>)
    }
}

// ─── agent_task：在单线程 LocalSet 中持有 capnp Client ───────────────────────

/// 启动共享 capnp RPC 代理任务，返回请求发送端（所有流共用）。
///
/// ### 并发模型与信号量限流
///
/// 每收到一个请求先尝试获取 `Semaphore` 许可，成功后再 `spawn_local`。
/// 当 in-flight 已满时，请求立即返回 `None`，避免在 `spawn_local` task 中隐藏排队。
///
fn monotonic_raw_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    if rc == 0 {
        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    } else {
        0
    }
}

fn realtime_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            d.as_secs()
                .saturating_mul(1_000_000_000)
                .saturating_add(d.subsec_nanos() as u64)
        })
        .unwrap_or(0)
}

/// capnp `Client` 实现了 `Clone`（内部引用计数），在同一 LocalSet 内并发安全。
///
/// ### 参数
///
/// - `channel_capacity`: mpsc channel 容量（= N × ceil(T_rpc/T_report) × 1.5，最小 128）
/// - `max_inflight`: 并发 in-flight RPC 上限（建议约 N × ceil(T_rpc/T_report)）
fn spawn_agent_task(
    server_addr: String,
    rpc_timeout: Duration,
    channel_capacity: usize,
    max_inflight: usize,
) -> mpsc::Sender<AgentRequest> {
    let (req_tx, mut req_rx) = mpsc::channel::<AgentRequest>(channel_capacity);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("DTCC: failed to build agent_task runtime");

    std::thread::spawn(move || {
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            let client = match connect_capnp(&server_addr).await {
                Ok(c) => {
                    debug!(
                        server_addr,
                        max_inflight, "DTCC agent_task: connected to RL agent"
                    );
                    c
                }
                Err(e) => {
                    error!(error = ?e, server_addr, "DTCC agent_task: connection failed, exiting");
                    return;
                }
            };

            // Semaphore 限制同时 in-flight 的 capnp RPC 数量。
            // 许可耗尽时立即 fallback，避免继续处理已经排队过久的旧请求。
            let sem = Arc::new(Semaphore::new(max_inflight));

            while let Some(AgentRequest {
                conn_id,
                obs,
                tx,
                is_new_flow,
            }) = req_rx.recv().await
            {
                let permit = match Arc::clone(&sem).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        debug!(
                            conn_id,
                            max_inflight,
                            "DTCC agent_task: max_inflight saturated, dropping request"
                        );
                        let _ = tx.send(None);
                        continue;
                    }
                };
                let client = client.clone(); // capnp Client clone 廉价（引用计数）
                tokio::task::spawn_local(async move {
                    // permit 在 spawn 前已获取，避免 task 在 semaphore 上隐藏排队。
                    let _permit = permit;
                    let mut req = client.get_action_request();
                    {
                        let mut o = req.get().init_observation();
                        o.set_bytes_acked(obs.bytes_acked);
                        o.set_loss(obs.loss);
                        o.set_rtt(obs.rtt);
                        o.set_rttvar(obs.rttvar);
                        o.set_castate(obs.castate);
                        o.set_minrtt(obs.minrtt);
                        o.set_delivery_rate(obs.rate_delivery);
                        o.set_unacked(obs.packets_unacked);
                        o.set_snd_mss(obs.snd_mss);
                        o.set_delivered(obs.packets_delivered);
                        o.set_bytes_sent(obs.bytes_sent_delta);
                        o.set_snd_cwnd(obs.snd_cwnd);
                        o.set_time_delta(obs.time_delta);
                        o.set_duration(obs.duration);
                        o.set_connection_id(conn_id); // Python 用此字段查找对应流的 prims
                        o.set_rpc_send_mono_ns(monotonic_raw_ns());
                        o.set_is_new_flow(is_new_flow);
                    }

                    let result = timeout(rpc_timeout, req.send().promise).await;
                    // _permit 在此处 drop，释放 semaphore 许可
                    let cwnd_opt = match result {
                        Ok(Ok(resp)) => resp
                            .get()
                            .ok()
                            .and_then(|r| r.get_action().ok())
                            .map(|a| a.get_cwnd() as u64)
                            .filter(|&c| c > 0),
                        Ok(Err(e)) => {
                            warn!("DTCC agent_task: RPC error: {:?}", e);
                            None
                        }
                        Err(_) => {
                            warn!("DTCC agent_task: RPC timed out");
                            None
                        }
                    };
                    let _ = tx.send(cwnd_opt);
                });
            }

            debug!("DTCC agent_task: channel closed, exiting");
        }));
    });

    req_tx
}

/// 在当前 LocalSet 中建立 capnp TCP 连接并返回客户端句柄。
async fn connect_capnp(server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
    use std::net::ToSocketAddrs;

    let addr = server_addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid addr"))?;

    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    debug!(local = %stream.local_addr()?, peer = %stream.peer_addr()?, "DTCC: TCP connected");

    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();

    let network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));

    let mut rpc_system = RpcSystem::new(network, None);
    let client: ccp_capnp::r_l_agent::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    // RpcSystem 必须在同一 LocalSet 上驱动
    tokio::task::spawn_local(rpc_system.map_err(|e| eprintln!("DTCC RpcSystem error: {:?}", e)));

    Ok(client)
}

// ─── 每条流的状态 ─────────────────────────────────────────────────────────────

/// 每条 TCP 流对应一个 `DtccFlow`（实现 lotus `AsyncFlow`）。
///
/// ### cwnd 并发写安全性
///
/// `cwnd` 改为 `Arc<AtomicU32>`：
/// - fire-and-forget task（tokio::spawn）写回新 cwnd
/// - `on_report` 主路径读 cwnd 计算 `snd_cwnd` 字段
/// - 两者无需同步序，最坏情况是发送了一次旧 cwnd 值，对 RL 训练无害
///
/// ### 与旧版的关键变化
///
/// | 旧版（portus）                          | 新版（lotus）                               |
/// |----------------------------------------|---------------------------------------------|
/// | `Rc<Runtime>` + `Rc<LocalSet>`         | 无（由 lotus 运行时统一管理）                |
/// | 每流独立 agent_task 线程               | 所有流共享一个 agent_task + 一条 TCP 连接   |
/// | `on_report` await RPC（~20ms 阻塞）    | `on_report` try_send + spawn（~0ms）        |
/// | `cwnd: u32`                            | `cwnd: Arc<AtomicU32>`（跨 task 共享写）   |
pub struct DtccFlow {
    control: FlowContext<()>,
    info: DatapathInfo,
    /// 向共享 agent_task 发送请求；`None` 表示 channel 已关闭
    agent_tx: Option<mpsc::Sender<AgentRequest>>,
    report_option: ConfigReport,
    /// 当前 cwnd（packets），由 fire-and-forget task 异步写回
    cwnd: Arc<AtomicU32>,
    init_cwnd: u32,
    /// fire-and-forget task 等待 oneshot 响应的超时时间
    /// = rpc_timeout（agent_task 侧）+ 10ms 余量
    resp_timeout: Duration,
    prev_report_time: Instant,
    start_timestep: Instant,
    flow_started_at: Instant,
    last_report_rx: Option<Instant>,
    last_kernel_report_ns: Option<u64>,
    datapath_to_rust_sum_us: u128,
    datapath_to_rust_count: u64,
    pre_bytes_sent: u64,
    counts: u32,
    /// 成功发送但响应超过 resp_timeout 的请求计数
    resp_timeout_count: Arc<AtomicU32>,
    /// 流创建后首次 on_report 标记（用于通知 Python 重置 prim 状态）
    is_first_report: bool,
}

impl DtccFlow {
    /// 使用已有的共享 `agent_tx` 初始化流（不再自行 spawn_agent_task）。
    async fn init(
        control: FlowContext<()>,
        info: DatapathInfo,
        agent_tx: mpsc::Sender<AgentRequest>,
        resp_timeout: Duration,
        report_option: ConfigReport,
        init_cwnd: u32,
    ) -> LotusResult<Self> {
        let now = Instant::now();
        let mut flow = DtccFlow {
            control,
            info,
            agent_tx: Some(agent_tx),
            report_option,
            cwnd: Arc::new(AtomicU32::new(init_cwnd)),
            init_cwnd,
            resp_timeout,
            prev_report_time: now,
            start_timestep: now,
            flow_started_at: now,
            last_report_rx: None,
            last_kernel_report_ns: None,
            datapath_to_rust_sum_us: 0,
            datapath_to_rust_count: 0,
            pre_bytes_sent: 0,
            counts: 0,
            resp_timeout_count: Arc::new(AtomicU32::new(0)),
            is_first_report: true,
        };

        flow.install_datapath_program().await?;
        Ok(flow)
    }

    async fn install_datapath_program(&mut self) -> LotusResult<()> {
        // reg_type 对应 portus Reg::IntoIterator tag byte：
        //   Control(non-volatile) = 0
        //   Control(volatile)     = 8
        //   Implicit              = 2
        //
        // ReportTime 在 DtccDatapathInterval 的 (def (ReportTime 0)) 中是第一个
        // non-volatile Control register，因此 reg_type=0, reg_index=0。
        let (prog_name, fields): (&'static str, Vec<(u8, u32, u64)>) = match self.report_option {
            ConfigReport::Interval(i) => {
                debug!(us = i.as_micros(), "DTCC: installing interval program");
                (
                    "DtccDatapathInterval",
                    vec![(0u8, 0u32, i.as_micros() as u64)],
                )
            }
            ConfigReport::Rtt => {
                debug!("DTCC: installing RTT program");
                ("DtccDatapathIntervalRTT", vec![])
            }
            ConfigReport::Ack => {
                debug!("DTCC: installing per-ACK program");
                ("DtccDatapathIntervalAck", vec![])
            }
        };

        let uid = self.info.programs.get(prog_name).copied().ok_or_else(|| {
            LotusError::Algorithm(format!(
                "datapath program '{}' uid not found — was INSTALL sent?",
                prog_name
            ))
        })?;

        debug!(
            prog = prog_name,
            uid,
            fields = fields.len(),
            "DTCC: sending CHANGE_PROG"
        );
        self.control.set_program(uid, &fields).await
    }

    async fn apply_fallback(&mut self, sock_id: u32, rtt: u64, loss: u64, minrtt: u64) {
        let cur = self.cwnd.load(Ordering::Relaxed);
        let new_cwnd = if loss > 0 {
            ((cur as f64) * 0.7).max(1.0) as u32
        } else if minrtt > 0 && rtt > minrtt * 2 {
            ((cur as f64) * 0.9).max(1.0) as u32
        } else {
            cur + 1
        };
        self.cwnd.store(new_cwnd, Ordering::Relaxed);
        let cwnd_bytes = (new_cwnd as u64) * (self.info.mss as u64);
        let rate = pacing_rate_from_cwnd(cwnd_bytes, rtt, minrtt);
        debug!(
            sock_id,
            cwnd_packets = new_cwnd,
            cwnd_bytes,
            rate = rate.unwrap_or(0),
            rtt_us = rtt,
            minrtt_us = minrtt,
            "DTCC: fallback"
        );
        // Cwnd = Implicit register index 4 (reg_type=2), Rate = index 5.
        if let Some(rate) = rate {
            let _ = self
                .control
                .update_field(&[(2u8, 4u32, cwnd_bytes), (2u8, 5u32, rate)])
                .await;
        } else {
            let _ = self.control.update_field(&[(2u8, 4u32, cwnd_bytes)]).await;
        }
    }
}

// ─── lotus AsyncFlow 实现 ─────────────────────────────────────────────────────

#[async_trait]
impl AsyncFlow for DtccFlow {
    /// Fire-and-forget 模式：立即返回，不阻塞 DatapathListener 主循环。
    ///
    /// 流程：
    /// 1. 读取 Report 字段（纯内存操作，~0ms）
    /// 2. `try_send` 到共享 channel（非阻塞，满则 fallback）
    /// 3. `tokio::spawn` 一个 task 等待 oneshot 响应并写回 cwnd
    /// 4. 立即返回 `Ok(())`
    ///
    /// N 条流并发调用时，N 个 `try_send` 几乎同时将请求投入共享 channel，
    /// `agent_task` 并发 `spawn_local` 发起 N 个 capnp RPC，
    /// Python `BatchProcessor` 在 timeout(3ms) 内聚合全部 N 个请求一起推理。
    async fn on_report(&mut self, sock_id: u32, m: Report) -> LotusResult<()> {
        let now = Instant::now();
        let report_rx = m.timestamp;
        let user_report_time_ns = realtime_ns();
        let kernel_report_time_ns = m
            .get_field("kernel_report_time_ns")
            .or_else(|| m.get_field("Report.kernel_report_time_ns"));
        let datapath_to_rust_us = kernel_report_time_ns.and_then(|kernel_ns| {
            // Absolute CLOCK_REALTIME nanoseconds are currently ~1e18. Older
            // kernels reported time relative to module load, which cannot be
            // compared with user-space realtime and would produce nonsense.
            if kernel_ns < 1_000_000_000_000_000_000 {
                return None;
            }

            user_report_time_ns
                .checked_sub(kernel_ns)
                .map(|delta_ns| delta_ns / 1_000)
        });
        let user_report_gap_us = self
            .last_report_rx
            .map(|last_rx| report_rx.saturating_duration_since(last_rx).as_micros() as u64);
        let kernel_report_gap_us = match (self.last_kernel_report_ns, kernel_report_time_ns) {
            (Some(last_kernel_ns), Some(current_kernel_ns)) => current_kernel_ns
                .checked_sub(last_kernel_ns)
                .map(|gap_ns| gap_ns / 1_000),
            _ => None,
        };
        let user_minus_kernel_gap_us = match (user_report_gap_us, kernel_report_gap_us) {
            (Some(user_gap), Some(kernel_gap)) => Some(user_gap as i64 - kernel_gap as i64),
            _ => None,
        };
        let report_rx_elapsed_us = report_rx
            .saturating_duration_since(self.flow_started_at)
            .as_micros() as u64;
        let on_report_queue_us = now.saturating_duration_since(report_rx).as_micros() as u64;

        if let Some(datapath_to_rust_us) = datapath_to_rust_us {
            self.datapath_to_rust_sum_us = self
                .datapath_to_rust_sum_us
                .saturating_add(datapath_to_rust_us as u128);
            self.datapath_to_rust_count = self.datapath_to_rust_count.saturating_add(1);
        }
        let datapath_to_rust_avg_us = if self.datapath_to_rust_count > 0 {
            (self.datapath_to_rust_sum_us / self.datapath_to_rust_count as u128) as u64
        } else {
            0
        };

        self.last_report_rx = Some(report_rx);
        if let Some(kernel_report_time_ns) = kernel_report_time_ns {
            self.last_kernel_report_ns = Some(kernel_report_time_ns);
        }

        self.counts += 1;

        let bytes_acked = m.get_field("Report.bytes_acked").unwrap_or(0);
        let loss = m.get_field("Report.loss").unwrap_or(0);
        let rtt = m.get_field("Report.rtt").unwrap_or(0);
        let rttvar = m.get_field("Report.rttvar").unwrap_or(0);
        let castate = m.get_field("Report.castate").unwrap_or(0);
        let minrtt = m.get_field("Report.minrtt").unwrap_or(0);
        let rate_delivery = m.get_field("Report.rate_delivery").unwrap_or(0);
        let packets_unacked = m.get_field("Report.packets_unacked").unwrap_or(0);
        let snd_mss = m.get_field("Report.snd_mss").unwrap_or(1460);
        let packets_delivered = m.get_field("Report.packets_delivered").unwrap_or(0);
        let bytes_sent = m.get_field("Report.bytes_sent").unwrap_or(0);

        let bytes_sent_delta = bytes_sent.saturating_sub(self.pre_bytes_sent);
        let time_delta = if rtt > 0 {
            self.start_timestep.elapsed().as_micros() as u64
        } else {
            m.get_field("ReportTime").unwrap_or(0) * 1000
        };

        debug!(
            sock_id = sock_id,
            total_reports = self.counts,
            resp_timeout_actions = self.resp_timeout_count.load(Ordering::Relaxed),
            duration_ms = time_delta as f64 / 1000.0,
            kernel_report_time_ns = kernel_report_time_ns.unwrap_or(0),
            kernel_report_gap_us = kernel_report_gap_us.unwrap_or(0),
            user_report_gap_us = user_report_gap_us.unwrap_or(0),
            user_minus_kernel_gap_us = user_minus_kernel_gap_us.unwrap_or(0),
            on_report_queue_us,
            report_rx_elapsed_us,
            datapath_to_rust_us = datapath_to_rust_us.unwrap_or(0),
            datapath_to_rust_avg_us,
            datapath_to_rust_samples = self.datapath_to_rust_count,
            "DTCC on_report"
        );

        let duration = {
            let d = self.prev_report_time.elapsed();
            if d > Duration::ZERO {
                d.as_micros() as u64
            } else {
                1_000_000
            }
        };

        // time_delta == 0 是异常值（Ack/Rtt 模式下 ReportTime 字段不存在，或两次
        // on_report 间隔 < 1µs），此时观察值不可信，跳过本轮，不发给 agent，
        // 不改变 cwnd，仅更新时间戳基准。
        if time_delta == 0 {
            warn!(
                sock_id,
                counts = self.counts,
                "DTCC: time_delta == 0, skipping report"
            );
            self.pre_bytes_sent = bytes_sent;
            self.prev_report_time = Instant::now();
            self.start_timestep = Instant::now();
            return Ok(());
        }

        debug!(sock_id, counts = self.counts, "DTCC: on_report");
        debug!(
            sock_id,
            rtt,
            delivery_rate = rate_delivery,
            bytes_sent,
            "DTCC: report fields"
        );

        // ── Fire-and-forget：try_send + spawn task 等响应 ────────────────────
        if let Some(agent_tx) = &self.agent_tx {
            let (resp_tx, resp_rx) = oneshot::channel();
            let obs = AgentObservation {
                bytes_acked,
                loss,
                rtt,
                rttvar,
                castate,
                minrtt,
                rate_delivery,
                packets_unacked,
                snd_mss,
                packets_delivered,
                bytes_sent_delta,
                snd_cwnd: self.cwnd.load(Ordering::Relaxed) as u64,
                time_delta,
                duration,
                is_new_flow: self.is_first_report,
            };

            let is_new_flow = self.is_first_report;
            self.is_first_report = false;

            match agent_tx.try_send(AgentRequest {
                conn_id: sock_id as u64,
                obs,
                tx: resp_tx,
                is_new_flow,
            }) {
                Ok(_) => {
                    // 将等待响应的工作交给独立 task，on_report 立即返回
                    let control = self.control.clone();
                    let cwnd_arc = Arc::clone(&self.cwnd);
                    let mss = self.info.mss;
                    let resp_timeout = self.resp_timeout;
                    let timeout_counter = Arc::clone(&self.resp_timeout_count);
                    tokio::spawn(async move {
                        // 等待 Python agent 响应，超时 = rpc_timeout + 10ms
                        match timeout(resp_timeout, resp_rx).await {
                            Ok(Ok(Some(new_cwnd))) => {
                                let new_cwnd = new_cwnd as u32;
                                cwnd_arc.store(new_cwnd, Ordering::Relaxed);
                                let cwnd_bytes = (new_cwnd as u64) * (mss as u64);
                                debug!(
                                    sock_id,
                                    cwnd_packets = new_cwnd,
                                    cwnd_bytes,
                                    rtt_us = rtt,
                                    minrtt_us = minrtt,
                                    "DTCC: cwnd updated (async)"
                                );
                                // 合并 Cwnd（idx=4）和 Rate（idx=5）为单次 UPDATE_FIELD send，
                                // 将 netlink 写次数从 2次/流 降为 1次/流，
                                // 避免 N 很大时 N×2 次并发 send 打爆内核写缓冲区。
                                if let Some(rate) = pacing_rate_from_cwnd(cwnd_bytes, rtt, minrtt) {
                                    debug!(
                                        sock_id,
                                        rate,
                                        current_rtt_rate = if rtt > 0 {
                                            cwnd_bytes.saturating_mul(USEC_PER_SEC) / rtt
                                        } else {
                                            0
                                        },
                                        minrtt_rate = if minrtt > 0 {
                                            cwnd_bytes
                                                .saturating_mul(2)
                                                .saturating_mul(USEC_PER_SEC)
                                                / minrtt
                                        } else {
                                            0
                                        },
                                        "DTCC: pacing rate limited by cwnd/rtt"
                                    );
                                    let _ = control
                                        .update_field(&[(2u8, 4u32, cwnd_bytes), (2u8, 5u32, rate)])
                                        .await;
                                } else {
                                    let _ = control.update_field(&[(2u8, 4u32, cwnd_bytes)]).await;
                                }
                            }
                            Ok(Ok(None)) => {
                                // agent 返回 None（RPC 超时或 fallback）：保持当前 cwnd
                                debug!(sock_id, "DTCC: agent returned None, keeping cwnd");
                            }
                            Ok(Err(_)) => {
                                warn!(sock_id, "DTCC: agent_task response channel dropped");
                            }
                            Err(_) => {
                                // resp_timeout 到期（= rpc_timeout + 10ms）：Python 推理过慢，保持当前 cwnd
                                warn!(
                                    sock_id,
                                    resp_timeout_ms = resp_timeout.as_millis(),
                                    "DTCC: agent response timed out"
                                );
                                timeout_counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // channel 满（Python 推理速度跟不上），同步 fallback
                    warn!(sock_id, "DTCC: agent channel full, applying fallback");
                    self.apply_fallback(sock_id, rtt, loss, minrtt).await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!(sock_id, "DTCC: agent_tx closed, disabling RL agent");
                    self.agent_tx = None;
                    self.apply_fallback(sock_id, rtt, loss, minrtt).await;
                }
            }
        } else {
            // agent 连接已关闭，纯 fallback 模式
            self.apply_fallback(sock_id, rtt, loss, minrtt).await;
        }

        // 更新时间戳和 bytes_sent 基准（不依赖 RPC 响应，立即更新）
        self.pre_bytes_sent = bytes_sent;
        self.prev_report_time = Instant::now();
        self.start_timestep = Instant::now();

        Ok(())
    }

    async fn close(&mut self) -> LotusResult<()> {
        let resp_timeout_count = self.resp_timeout_count.load(Ordering::Relaxed);
        let datapath_to_rust_avg_us = if self.datapath_to_rust_count > 0 {
            (self.datapath_to_rust_sum_us / self.datapath_to_rust_count as u128) as u64
        } else {
            0
        };
        info!(
            sock_id = self.info.sock_id,
            total_reports = self.counts,
            final_cwnd = self.cwnd.load(Ordering::Relaxed),
            resp_timeout_actions = resp_timeout_count,
            resp_timeout_ms = self.resp_timeout.as_millis(),
            datapath_to_rust_avg_us,
            datapath_to_rust_samples = self.datapath_to_rust_count,
            "DTCC: flow closed"
        );
        // 关闭本流对 channel 的引用；若这是最后一个 Sender，agent_task 自动退出
        self.agent_tx = None;
        Ok(())
    }

    async fn initialize(&mut self) -> LotusResult<()> {
        debug!(
            sock_id       = self.info.sock_id,
            init_cwnd     = self.init_cwnd,
            report_option = ?self.report_option,
            "DTCC: flow initialized"
        );
        Ok(())
    }
}
