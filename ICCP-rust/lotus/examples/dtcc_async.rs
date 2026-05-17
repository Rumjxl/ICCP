//! DTCC算法的Lotus异步实现
//!
//! 这个示例展示了如何在Lotus中实现与外部智能体的异步通信

use async_trait::async_trait;
use lotus::{
    algorithm::{AsyncCongAlg, DatapathInfo, Report},
    flow::{AsyncFlow, FlowContext},
    runtime::RuntimeBuilder,
    Result,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// 模拟capnp_rpc相关的结构
mod ccp_capnp {
    use std::time::Duration;
    // 这里应该包含实际的capnp生成代码
    pub struct RLAgent {
        // 模拟的智能体客户端
    }

    impl RLAgent {
        pub async fn get_action(
            &self,
            observation: Observation,
        ) -> Result<Action, Box<dyn std::error::Error + Send + Sync>> {
            // 模拟网络请求延迟
            tokio::time::sleep(Duration::from_millis(5)).await;

            // 简单的控制逻辑作为示例
            let cwnd = if observation.loss > 0 {
                (observation.snd_cwnd as f64 * 0.7).max(1.0) as u64
            } else if observation.rtt > observation.minrtt * 2 {
                (observation.snd_cwnd as f64 * 0.9).max(1.0) as u64
            } else {
                observation.snd_cwnd + 1
            };

            Ok(Action { cwnd })
        }
    }

    #[derive(Debug, Clone)]
    pub struct Observation {
        pub bytes_acked: u64,
        pub loss: u64,
        pub rtt: u64,
        pub rttvar: u64,
        pub castate: u64,
        pub minrtt: u64,
        pub delivery_rate: u64,
        pub unacked: u64,
        pub snd_mss: u64,
        pub delivered: u64,
        pub bytes_sent: u64,
        pub snd_cwnd: u64,
        pub time_delta: u64,
        pub duration: u64,
    }

    #[derive(Debug, Clone)]
    pub struct Action {
        pub cwnd: u64,
    }
}

/// 配置报告选项
#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
}

/// DTCC异步算法实现
#[derive(Clone)]
pub struct DtccAsync {
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
}

impl Default for DtccAsync {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4826".to_string(),
            init_cwnd: 10,
            report_option: ConfigReport::Interval(Duration::from_millis(10)),
        }
    }
}

#[async_trait]
impl AsyncCongAlg<()> for DtccAsync {
    fn name(&self) -> &'static str {
        "DTCC_Async"
    }

    async fn datapath_programs(&self) -> HashMap<&'static str, String> {
        let mut h = HashMap::new();

        h.insert(
            "DtccDatapathInterval",
            r#"
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
        "#
            .to_string(),
        );

        h
    }

    async fn new_flow(
        &self,
        control: FlowContext<()>,
        info: DatapathInfo,
    ) -> Result<Box<dyn AsyncFlow>> {
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };

        info!(
            sock_id = info.sock_id,
            src_ip = info.src_ip,
            src_port = info.src_port,
            dst_ip = info.dst_ip,
            dst_port = info.dst_port,
            init_cwnd = init_cwnd,
            "Creating DTCC async flow"
        );

        DtccAsyncFlow::new(
            control,
            info,
            &self.server_addr,
            self.report_option,
            init_cwnd,
        )
        .await
        .map(|f| Box::new(f) as Box<dyn AsyncFlow>)
    }
}

/// DTCC异步流实现
pub struct DtccAsyncFlow {
    control: FlowContext<()>,
    info: DatapathInfo,
    rl_client: Arc<RwLock<Option<ccp_capnp::RLAgent>>>,
    report_option: ConfigReport,
    cwnd: u32,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_bytes_sent: u64,
    counts: u32,
}

impl DtccAsyncFlow {
    async fn new(
        control: FlowContext<()>,
        info: DatapathInfo,
        server_addr: &str,
        report_option: ConfigReport,
        init_cwnd: u32,
    ) -> Result<Self> {
        let flow = Self {
            control,
            info,
            rl_client: Arc::new(RwLock::new(None)),
            report_option,
            cwnd: init_cwnd,
            init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: Instant::now(),
            pre_bytes_sent: 0,
            counts: 0,
        };

        // 异步连接到智能体服务器
        if let Err(e) = flow.connect_to_agent(server_addr).await {
            warn!("Failed to connect to RL agent: {}, using fallback", e);
        }

        Ok(flow)
    }

    /// 异步连接到智能体服务器
    async fn connect_to_agent(&self, server_addr: &str) -> Result<()> {
        debug!("Connecting to RL agent at {}", server_addr);

        // 在实际实现中，这里会建立capnp_rpc连接
        // 现在我们模拟连接过程
        tokio::time::sleep(Duration::from_millis(10)).await;

        let client = ccp_capnp::RLAgent {};
        let mut rl_client = self.rl_client.write().await;
        *rl_client = Some(client);

        info!("Successfully connected to RL agent");
        Ok(())
    }

    /// 异步获取智能体动作
    async fn get_agent_action(
        &self,
        observation: ccp_capnp::Observation,
    ) -> Result<ccp_capnp::Action> {
        let client_guard = self.rl_client.read().await;

        if let Some(client) = client_guard.as_ref() {
            // 设置超时以避免阻塞
            match tokio::time::timeout(Duration::from_millis(50), client.get_action(observation))
                .await
            {
                Ok(Ok(action)) => {
                    debug!("Received action from RL agent: cwnd={}", action.cwnd);
                    Ok(action)
                }
                Ok(Err(e)) => {
                    warn!("RL agent returned error: {}", e);
                    // 返回默认动作
                    Ok(ccp_capnp::Action {
                        cwnd: self.cwnd as u64,
                    })
                }
                Err(_) => {
                    warn!("RL agent request timed out, using default action");
                    Ok(ccp_capnp::Action {
                        cwnd: self.cwnd as u64,
                    })
                }
            }
        } else {
            warn!("RL agent not connected, using default action");
            Ok(ccp_capnp::Action {
                cwnd: self.cwnd as u64,
            })
        }
    }

    /// 从报告中提取观察数据
    fn extract_observation(&self, report: &Report) -> ccp_capnp::Observation {
        let bytes_acked = report.get_field("Report.bytes_acked").unwrap_or(0);
        let loss = report.get_field("Report.loss").unwrap_or(0);
        let rtt = report.get_field("Report.rtt").unwrap_or(100000);
        let rttvar = report.get_field("Report.rttvar").unwrap_or(0);
        let castate = report.get_field("Report.castate").unwrap_or(0);
        let minrtt = report.get_field("Report.minrtt").unwrap_or(100000);
        let delivery_rate = report.get_field("Report.rate_delivery").unwrap_or(0);
        let unacked = report.get_field("Report.packets_unacked").unwrap_or(0);
        let snd_mss = report.get_field("Report.snd_mss").unwrap_or(1460);
        let delivered = report.get_field("Report.packets_delivered").unwrap_or(0);
        let bytes_sent = report.get_field("Report.bytes_sent").unwrap_or(0);

        let bytes_sent_delta = bytes_sent.saturating_sub(self.pre_bytes_sent);
        let time_delta = self.start_timestep.elapsed().as_micros() as u64;
        let duration = self.prev_report_time.elapsed().as_micros() as u64;

        ccp_capnp::Observation {
            bytes_acked,
            loss,
            rtt,
            rttvar,
            castate,
            minrtt,
            delivery_rate,
            unacked,
            snd_mss,
            delivered,
            bytes_sent: bytes_sent_delta,
            snd_cwnd: self.cwnd as u64,
            time_delta,
            duration,
        }
    }
}

#[async_trait]
impl AsyncFlow for DtccAsyncFlow {
    async fn on_report(&mut self, sock_id: u32, report: Report) -> Result<()> {
        self.counts += 1;

        debug!(
            sock_id = sock_id,
            counts = self.counts,
            "Processing report asynchronously"
        );

        // 提取观察数据
        let observation = self.extract_observation(&report);

        // 异步获取智能体动作
        let action = self.get_agent_action(observation.clone()).await?;

        // 更新拥塞窗口
        if action.cwnd > 0 {
            self.cwnd = action.cwnd as u32;
            let cwnd_bytes = self.cwnd * self.info.mss;

            info!(
                sock_id = sock_id,
                cwnd_packets = self.cwnd,
                cwnd_bytes = cwnd_bytes,
                rtt = observation.rtt,
                loss = observation.loss,
                "Updated congestion window from RL agent"
            );

            // 发送控制消息到数据路径
            let control_msg = format!("SET_CWND {}", cwnd_bytes);
            self.control
                .send_control_message(control_msg.as_bytes())
                .await?;

            // 计算并设置发送速率
            if observation.minrtt > 0 {
                let rate_bps = (cwnd_bytes as u64 * 8 * 1_000_000) / observation.minrtt;
                let rate_msg = format!("SET_RATE {}", rate_bps);
                self.control
                    .send_control_message(rate_msg.as_bytes())
                    .await?;

                debug!(
                    sock_id = sock_id,
                    rate_bps = rate_bps,
                    "Updated sending rate"
                );
            }
        }

        // 更新状态
        self.pre_bytes_sent = report.get_field("Report.bytes_sent").unwrap_or(0);
        self.prev_report_time = Instant::now();
        self.start_timestep = Instant::now();

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        info!(
            total_reports = self.counts,
            final_cwnd = self.cwnd,
            "Closing DTCC async flow"
        );

        // 清理资源
        let mut client = self.rl_client.write().await;
        *client = None;

        Ok(())
    }

    async fn initialize(&mut self) -> Result<()> {
        info!(
            init_cwnd = self.init_cwnd,
            report_option = ?self.report_option,
            "Initializing DTCC async flow"
        );

        // 根据报告选项设置数据路径程序
        match self.report_option {
            ConfigReport::Interval(interval) => {
                let program_msg = format!(
                    "SET_PROGRAM DtccDatapathInterval ReportTime={}",
                    interval.as_micros()
                );
                self.control
                    .send_control_message(program_msg.as_bytes())
                    .await?;
            }
            ConfigReport::Rtt => {
                let program_msg = "SET_PROGRAM DtccDatapathIntervalRTT";
                self.control
                    .send_control_message(program_msg.as_bytes())
                    .await?;
            }
            ConfigReport::Ack => {
                let program_msg = "SET_PROGRAM DtccDatapathIntervalAck";
                self.control
                    .send_control_message(program_msg.as_bytes())
                    .await?;
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting DTCC async algorithm example");

    // 创建运行时
    let mut runtime = RuntimeBuilder::new()
        .with_worker_threads(4)
        .with_algorithm_timeout(1000) // 1秒超时
        .enable_work_stealing(true)
        .build()
        .await?;

    // 注册DTCC异步算法
    let dtcc_algorithm = DtccAsync::default();
    runtime.register_async_algorithm(dtcc_algorithm).await?;

    // 启动运行时
    let _handle = runtime.start().await?;

    // 模拟创建流和处理报告
    let datapath_info = DatapathInfo {
        sock_id: 1,
        init_cwnd: 10,
        mss: 1460,
        src_ip: 0x7F000001, // 127.0.0.1
        src_port: 8080,
        dst_ip: 0x7F000001, // 127.0.0.1
        dst_port: 80,
        programs: std::collections::HashMap::new(),
        scopes: std::collections::HashMap::new(),
        report_fields: std::collections::HashMap::new(),
    };

    match runtime.create_flow(1, "DTCC_Async", datapath_info).await {
        Ok(flow_id) => {
            info!(flow_id = %flow_id, "Created DTCC flow");

            // 模拟一系列报告
            for i in 0..20u64 {
                let mut report = Report {
                    fields: HashMap::new(),
                    timestamp: Instant::now(),
                };

                // 模拟网络状况变化
                let base_rtt: u64 = 50000; // 50ms
                let rtt_variation = (i % 5) * 10000; // 变化的RTT
                let loss: u64 = if i == 10 { 1 } else { 0 }; // 在第10个报告时模拟丢包

                report.set_field("Report.bytes_acked".to_string(), 1460 * (i + 1));
                report.set_field("Report.loss".to_string(), loss);
                report.set_field("Report.rtt".to_string(), base_rtt + rtt_variation);
                report.set_field("Report.rttvar".to_string(), 5000);
                report.set_field("Report.castate".to_string(), 0);
                report.set_field("Report.minrtt".to_string(), base_rtt);
                report.set_field("Report.rate_delivery".to_string(), 1000000); // 1Mbps
                report.set_field("Report.packets_unacked".to_string(), 5);
                report.set_field("Report.snd_mss".to_string(), 1460);
                report.set_field("Report.packets_delivered".to_string(), i + 1);
                report.set_field("Report.bytes_sent".to_string(), 1460 * (i + 1) + 100);

                if let Err(e) = runtime.handle_report(flow_id, 1, report).await {
                    warn!(error = %e, "Failed to handle report");
                }

                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // 关闭流
            if let Err(e) = runtime.close_flow(flow_id).await {
                warn!(error = %e, "Failed to close flow");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to create DTCC flow");
        }
    }

    // 打印统计信息
    let stats = runtime.get_stats().await;
    info!(
        active_flows = stats.active_flows,
        compute_throughput = stats.compute_throughput,
        "Final statistics"
    );

    info!("DTCC async example completed successfully");
    Ok(())
}
