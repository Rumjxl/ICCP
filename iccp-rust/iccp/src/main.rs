//! ICCP — Intelligent Concurrent Congestion Protocol
//!
//! 基于 **lotus** 异步框架的多算法并发拥塞控制调度器。
//!
//! ## 与旧版（portus）的对比
//!
//! | 旧版（portus）                         | 新版（lotus）                                               |
//! |---------------------------------------|-------------------------------------------------------------|
//! | `portus::RunBuilder`                  | `lotus::runtime::RuntimeBuilder`                            |
//! | `.default_alg(Dtcc::default())`       | `algorithms.insert("dtcc", Box::new(Dtcc::new(...)))`       |
//! | `.additional_alg(BbrConfig)`          | `algorithms.insert("bbr", Box::new(BbrAlg))`                |
//! | `.add_flow_alg(src, dst, "bbr")`      | `runtime.add_flow_alg_by_port(src, dst, "bbr".into())`      |
//! | `.default_alg_name("DTCC")`           | `runtime.set_default_algorithm("dtcc".into())`              |
//! | `rb.run()` (同步阻塞)                  | `runtime.start_netlink(algorithms).await` + Ctrl-C 信号     |
//!
//! ## 算法注册策略
//!
//! 所有算法通过 `HashMap<String, Box<dyn AsyncCongAlg<()>>>` 传入
//! `runtime.start_netlink()`，框架内部负责编译 datapath 程序、发送 INSTALL 帧，
//! 以及根据流路由规则将每条新流分发给对应算法。
//!
//! - **DTCC**：已完整迁移为 `lotus::AsyncCongAlg`，直接使用。
//! - **BBR / Orca**：原库深度依赖 portus，尚未迁移。
//!   本版本提供同名的 **lotus 占位适配器**（`BbrAlg` / `OrcaAlg`），
//!   内部使用简化的 AIMD/CUBIC 逻辑，保持框架完整可运行；
//!   待 bbr/orca 完成 lotus 迁移后，替换实现即可，接口不变。
//!
//! ## 流路由
//!
//! lotus 的 `AlgorithmManager` 支持三种路由规则（优先级从高到低）：
//! 1. `ExactMatch`  — 精确 (src_ip+src_port, dst_ip+dst_port) 匹配
//! 2. `PortRange`   — 目标/源端口范围匹配
//! 3. `Default`     — 兜底默认算法

use async_trait::async_trait;
use clap::{App, Arg};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::info;
use tracing_subscriber::EnvFilter;

use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    runtime::RuntimeBuilder,
    Result as LotusResult,
};

use ccp_bbr::BbrConfig;
use dtcc::{ConfigReport, Dtcc};

// BBR 实现已迁移到 ccp_bbr 库，使用真实的 AsyncCongAlg 实现

// ─────────────────────────────────────────────────────────────────────────────
// Orca/Cubic 占位适配器
// 待 orca 完成 lotus 迁移后，将此模块替换为真正的 Orca 实现。
// ─────────────────────────────────────────────────────────────────────────────

/// Orca/Cubic lotus 占位算法（标准 CUBIC 公式）
pub struct OrcaAlg;

#[async_trait]
impl AsyncCongAlg<()> for OrcaAlg {
    fn name(&self) -> &'static str {
        "orca"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::new();
        h.insert(
            "OrcaDatapath",
            "
                (def
                    (Report
                        (volatile bytes_acked 0)
                        (volatile loss 0)
                        (volatile rtt 0)
                        (volatile minrtt +infinity)
                    )
                    (ReportTime 0)
                )
                (when true
                    (:= Report.bytes_acked (+ Report.bytes_acked Ack.bytes_acked))
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.minrtt Flow.min_rtt)
                    (fallthrough)
                )
                (when (> Micros ReportTime)
                    (report)
                    (:= Micros 0)
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
        info!(sock_id = info.sock_id, "Orca/Cubic: new flow");
        let init_cwnd = info.init_cwnd;
        Ok(Box::new(OrcaFlow {
            control,
            info,
            cwnd: init_cwnd,
            ss_thresh: 0x7fff_ffff_u32,
            w_last_max: 0.0,
            epoch_start: None,
            counts: 0,
        }))
    }
}

const CUBIC_C: f64 = 0.4;
const CUBIC_BETA: f64 = 0.7;

const DTCC_ROUTE_SRC_PORT: u16 = 35603;
const DTCC_ROUTE_DST_PORT: u16 = 5003;
const BBR_ROUTE_SRC_PORT: u16 = 35859;
const BBR_ROUTE_DST_PORT: u16 = 5004;
const ORCA_ROUTE_SRC_PORT: u16 = 36011;
const ORCA_ROUTE_DST_PORT: u16 = 5004;

pub struct OrcaFlow {
    control: FlowContext<()>,
    info: DatapathInfo,
    cwnd: u32,
    ss_thresh: u32,
    w_last_max: f64,
    epoch_start: Option<Instant>,
    counts: u32,
}

#[async_trait]
impl AsyncFlow for OrcaFlow {
    async fn on_report(&mut self, sock_id: u32, m: Report) -> LotusResult<()> {
        self.counts += 1;

        let loss = m.get_field("Report.loss").unwrap_or(0);
        let _rtt_us = m.get_field("Report.rtt").unwrap_or(100_000);

        if loss > 0 {
            // 拥塞：CUBIC 窗口缩减
            self.w_last_max = self.cwnd as f64;
            self.ss_thresh = ((self.cwnd as f64 * CUBIC_BETA) as u32).max(4);
            self.cwnd = self.ss_thresh;
            self.epoch_start = None;
        } else if self.cwnd < self.ss_thresh {
            // 慢启动
            self.cwnd += 1;
        } else {
            // CUBIC 稳态增长
            let now = Instant::now();
            let t = match self.epoch_start {
                Some(start) => now.duration_since(start).as_secs_f64(),
                None => {
                    self.epoch_start = Some(now);
                    0.0
                }
            };
            // K = cbrt(w_last_max * (1 - beta) / C)
            let k = (self.w_last_max * (1.0 - CUBIC_BETA) / CUBIC_C).cbrt();
            // W_cubic(t) = C*(t-K)^3 + w_last_max
            let w_cubic = CUBIC_C * (t - k).powi(3) + self.w_last_max;
            self.cwnd = (w_cubic as u32).max(self.cwnd).max(4);
        }

        let cwnd_bytes = (self.cwnd as u64) * (self.info.mss as u64);
        info!(
            sock_id,
            cwnd_packets = self.cwnd,
            cwnd_bytes,
            loss,
            "Orca/Cubic: updated cwnd"
        );

        // 更新内核 Cwnd（Implicit reg index=4, type=2）
        self.control
            .update_field(&[(2u8, 4u32, cwnd_bytes)])
            .await?;

        Ok(())
    }

    async fn close(&mut self) -> LotusResult<()> {
        info!(
            sock_id = self.info.sock_id,
            counts = self.counts,
            "Orca/Cubic: flow closed"
        );
        Ok(())
    }

    async fn initialize(&mut self) -> LotusResult<()> {
        info!(sock_id = self.info.sock_id, "Orca/Cubic: flow initialized");
        // 切换到 OrcaDatapath 程序，并将 ReportTime 设为 10ms（10_000µs）。
        // ReportTime 是 OrcaDatapath 中的第一个 non-volatile Control 寄存器，
        // 因此 reg_type=0（Control non-volatile），reg_index=0。
        self.control
            .set_program_by_name(&self.info, "OrcaDatapath", &[("ReportTime", 10_000)])
            .await?;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 命令行参数
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct IccpConfig {
    /// 默认算法名称
    default_alg: String,
    /// DTCC RL 智能体地址
    dtcc_server_addr: String,
    /// DTCC 初始拥塞窗口
    dtcc_init_cwnd: u32,
}

fn make_args() -> IccpConfig {
    let matches = App::new("ICCP (lotus)")
        .version("0.1.0")
        .author("Xiaolan Ji")
        .about("Intelligent Concurrent Congestion Protocol — lotus async edition")
        .arg(
            Arg::with_name("default_alg")
                .long("default")
                .help("Default algorithm: dtcc | bbr | orca")
                .default_value("dtcc"),
        )
        .arg(
            Arg::with_name("dtcc_addr")
                .long("dtcc-addr")
                .help("DTCC RL agent address")
                .default_value("127.0.0.1:4826"),
        )
        .arg(
            Arg::with_name("dtcc_init_cwnd")
                .long("dtcc-init-cwnd")
                .help("DTCC initial cwnd in packets")
                .default_value("0"),
        )
        .get_matches();

    let default_alg = matches
        .value_of("default_alg")
        .unwrap()
        .to_ascii_lowercase();
    if !matches!(default_alg.as_str(), "dtcc" | "bbr" | "orca") {
        eprintln!("invalid --default {default_alg:?}; expected one of: dtcc, bbr, orca");
        std::process::exit(2);
    }

    IccpConfig {
        default_alg,
        dtcc_server_addr: matches.value_of("dtcc_addr").unwrap().to_string(),
        dtcc_init_cwnd: matches
            .value_of("dtcc_init_cwnd")
            .unwrap()
            .parse()
            .unwrap_or(0),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 主函数
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let cfg = make_args();

    info!(
        default_alg = cfg.default_alg,
        dtcc_server_addr = cfg.dtcc_server_addr,
        dtcc_init_cwnd = cfg.dtcc_init_cwnd,
        "Starting ICCP (lotus async framework)"
    );

    // ── 构建 lotus 运行时 ─────────────────────────────────────────────────────
    let runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .with_algorithm_timeout(200)
        .enable_work_stealing(true)
        .build()
        .await
        .expect("Failed to create lotus runtime");

    // ── 配置流路由规则（AlgorithmManager） ───────────────────────────────────
    //
    // 规则优先级（高→低）：ExactMatch > PortRange > Default
    //
    // 对应旧版 portus 的 add_flow_alg / default_alg_name：
    //   .add_flow_alg(src_port, dst_port, "alg") → 任一端口命中即路由到该算法
    //   .default_alg_name("DTCC")            → Default → "dtcc"
    //
    // 注意：旧版 add_flow_alg 参数为裸整数（非 IP），根据原代码语义
    // 理解为 (src_port, dst_port) 组合，在 lotus 中映射为两个 PortRange 规则。
    runtime.add_flow_alg_by_port(BBR_ROUTE_SRC_PORT, BBR_ROUTE_DST_PORT, "bbr".to_string());
    runtime.add_flow_alg_by_port(DTCC_ROUTE_SRC_PORT, DTCC_ROUTE_DST_PORT, "dtcc".to_string());
    runtime.add_flow_alg_by_port(ORCA_ROUTE_SRC_PORT, ORCA_ROUTE_DST_PORT, "orca".to_string());

    // 设置默认算法
    runtime.set_default_algorithm(cfg.default_alg.clone());

    let rule_dtcc = format!(
        "any port {}/{} -> dtcc",
        DTCC_ROUTE_SRC_PORT, DTCC_ROUTE_DST_PORT
    );
    let rule_bbr = format!(
        "any port {}/{} -> bbr",
        BBR_ROUTE_SRC_PORT, BBR_ROUTE_DST_PORT
    );
    let rule_orca = format!(
        "any port {}/{} -> orca",
        ORCA_ROUTE_SRC_PORT, ORCA_ROUTE_DST_PORT
    );

    info!(
        default = cfg.default_alg,
        rule_dtcc = %rule_dtcc,
        rule_bbr = %rule_bbr,
        rule_orca = %rule_orca,
        "Flow routing configured"
    );

    // ── 构建算法表并启动 netlink IPC ─────────────────────────────────────────
    //
    // 所有算法以 `Box<dyn AsyncCongAlg<()>>` 形式传入 `start_netlink()`，
    // 框架内部负责：
    //   1. 调用每个算法的 `datapath_programs()` 并编译成 INSTALL 帧
    //   2. 创建 netlink socket，发送所有 INSTALL 帧
    //   3. 监听内核 CREATE/MEASURE/CLOSE 消息并路由到对应算法
    let mut algorithms: HashMap<String, Box<dyn lotus::algorithm::AsyncCongAlg<()>>> =
        HashMap::new();

    // 1. DTCC — 完整 lotus AsyncCongAlg 实现，带 capnp RPC 智能体通信
    //    使用 Dtcc::new() 公共构造函数，避免访问私有字段 agent_tx
    algorithms.insert(
        "dtcc".to_string(),
        Box::new(Dtcc::new(
            cfg.dtcc_server_addr.clone(),
            cfg.dtcc_init_cwnd,
            ConfigReport::Interval(Duration::from_millis(10)),
        )),
    );

    // 2. BBR — lotus AsyncCongAlg 完整实现，从 ccp_bbr 库导入
    algorithms.insert("bbr".to_string(), Box::new(BbrConfig::default()));

    // 3. Orca/Cubic — lotus 占位适配器（待 orca 迁移后替换）
    algorithms.insert("orca".to_string(), Box::new(OrcaAlg));

    info!(
        algorithms = algorithms.keys().cloned().collect::<Vec<_>>().join(", "),
        "All algorithms prepared"
    );

    // ── 启动 netlink IPC 主循环（阻塞，直到内核连接断开或进程退出） ────────────
    //
    // `start_netlink` 内部：
    //   - 编译并安装所有 datapath 程序到内核
    //   - 监听内核消息（CREATE / MEASURE / CLOSE / READY）
    //   - 将每条新流路由到 AlgorithmManager 选定的算法
    //
    // 返回 `JoinHandle`，通过 abort() 停止（配合 Ctrl-C 信号）。
    let netlink_handle = runtime
        .start_netlink(algorithms)
        .await
        .expect("Failed to start netlink IPC");

    info!("ICCP started (netlink IPC active), press Ctrl-C to stop");

    // 阻塞等待 Ctrl-C
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to wait for Ctrl-C");

    info!("Received shutdown signal, stopping ICCP");

    // 中止 netlink 监听任务
    netlink_handle.abort();
    let _ = netlink_handle.await;

    info!("ICCP stopped");
}
