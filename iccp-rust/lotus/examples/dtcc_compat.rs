//! DTCC算法的Lotus兼容性实现
//!
//! 这个示例展示了如何将现有的Portus DTCC算法迁移到Lotus，
//! 同时保持与外部智能体的capnp_rpc通信能力

use lotus::{
    algorithm::{CongAlg, DatapathInfo, Report},
    compat::PortusCompatRuntime,
    flow::{Flow, FlowContext},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

// 重用您现有的capnp模块结构
mod ccp_capnp {
    // 这里应该是您现有的capnp生成代码
    // 为了演示，我们创建一个简化版本

    pub mod r_l_agent {
        use std::time::Duration;

        #[derive(Clone)]
        pub struct Client {
            // 模拟客户端
        }

        impl Client {
            pub fn get_action_request(&self) -> ActionRequest {
                ActionRequest::new()
            }
        }

        pub struct ActionRequest {
            observation: Observation,
        }

        impl ActionRequest {
            fn new() -> Self {
                Self {
                    observation: Observation::default(),
                }
            }

            pub fn get(&mut self) -> &mut Self {
                self
            }

            pub fn init_observation(&mut self) -> &mut Observation {
                &mut self.observation
            }

            pub async fn send(
                self,
            ) -> Result<ActionResponse, Box<dyn std::error::Error + Send + Sync>> {
                // 模拟网络延迟
                tokio::time::sleep(Duration::from_millis(5)).await;

                // 简单的控制逻辑
                let cwnd = if self.observation.loss > 0 {
                    (self.observation.snd_cwnd as f64 * 0.7).max(1.0) as u64
                } else if self.observation.rtt > self.observation.minrtt * 2 {
                    (self.observation.snd_cwnd as f64 * 0.9).max(1.0) as u64
                } else {
                    self.observation.snd_cwnd + 1
                };

                Ok(ActionResponse {
                    action: Action { cwnd },
                })
            }
        }

        pub struct ActionResponse {
            action: Action,
        }

        impl ActionResponse {
            pub fn get(&self) -> Result<&ActionResponse, Box<dyn std::error::Error>> {
                Ok(self)
            }

            pub fn get_action(&self) -> Result<&Action, Box<dyn std::error::Error>> {
                Ok(&self.action)
            }
        }

        #[derive(Default)]
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

        impl Observation {
            pub fn set_bytes_acked(&mut self, val: u64) {
                self.bytes_acked = val;
            }
            pub fn set_loss(&mut self, val: u64) {
                self.loss = val;
            }
            pub fn set_rtt(&mut self, val: u64) {
                self.rtt = val;
            }
            pub fn set_rttvar(&mut self, val: u64) {
                self.rttvar = val;
            }
            pub fn set_castate(&mut self, val: u64) {
                self.castate = val;
            }
            pub fn set_minrtt(&mut self, val: u64) {
                self.minrtt = val;
            }
            pub fn set_delivery_rate(&mut self, val: u64) {
                self.delivery_rate = val;
            }
            pub fn set_unacked(&mut self, val: u64) {
                self.unacked = val;
            }
            pub fn set_snd_mss(&mut self, val: u64) {
                self.snd_mss = val;
            }
            pub fn set_delivered(&mut self, val: u64) {
                self.delivered = val;
            }
            pub fn set_bytes_sent(&mut self, val: u64) {
                self.bytes_sent = val;
            }
            pub fn set_snd_cwnd(&mut self, val: u64) {
                self.snd_cwnd = val;
            }
            pub fn set_time_delta(&mut self, val: u64) {
                self.time_delta = val;
            }
            pub fn set_duration(&mut self, val: u64) {
                self.duration = val;
            }
        }

        pub struct Action {
            pub cwnd: u64,
        }

        impl Action {
            pub fn get_cwnd(&self) -> u64 {
                self.cwnd
            }
        }
    }
}

/// 配置报告选项
#[derive(Debug, Clone, Copy)]
pub enum ConfigReport {
    Ack,
    Rtt,
    Interval(Duration),
}

/// DTCC算法（兼容Portus接口）
pub struct Dtcc {
    pub server_addr: String,
    pub init_cwnd: u32,
    pub report_option: ConfigReport,
}

impl Default for Dtcc {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4826".to_string(),
            init_cwnd: 10,
            report_option: ConfigReport::Interval(Duration::from_millis(10)),
        }
    }
}

impl CongAlg<()> for Dtcc {
    fn name(&self) -> &'static str {
        "DTCC"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
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

    fn new_flow(&self, _control: FlowContext<()>, info: DatapathInfo) -> Box<dyn Flow> {
        let init_cwnd = if self.init_cwnd != 0 {
            self.init_cwnd
        } else {
            info.init_cwnd
        };

        debug!(
            sock_id = info.sock_id,
            src_ip = info.src_ip,
            src_port = info.src_port,
            dst_ip = info.dst_ip,
            dst_port = info.dst_port,
            init_cwnd = init_cwnd,
            "Creating DTCC flow"
        );

        Box::new(DtccFlow::new(
            info,
            &self.server_addr,
            self.report_option,
            init_cwnd,
        ))
    }
}

/// DTCC流实现
pub struct DtccFlow {
    info: DatapathInfo,
    client: Option<Arc<ccp_capnp::r_l_agent::Client>>,
    report_option: ConfigReport,
    runtime: Arc<tokio::runtime::Runtime>,
    cwnd: u32,
    init_cwnd: u32,
    prev_report_time: Instant,
    start_timestep: Instant,
    pre_bytes_sent: u64,
    counts: u32,
}

impl DtccFlow {
    /// 异步连接到智能体服务器
    async fn connect_async(_server_addr: &str) -> std::io::Result<ccp_capnp::r_l_agent::Client> {
        debug!("Connecting to RL agent (simulated)");
        // 在实际实现中，这里会建立真正的capnp_rpc连接
        // 现在我们模拟连接过程
        info!("Successfully connected to RL agent (simulated)");
        Ok(ccp_capnp::r_l_agent::Client {})
    }

    fn new(
        info: DatapathInfo,
        rl_server_addr: &str,
        report_option: ConfigReport,
        init_cwnd: u32,
    ) -> DtccFlow {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        let mut flow = DtccFlow {
            info,
            client: None,
            report_option,
            runtime: Arc::new(runtime),
            cwnd: init_cwnd,
            init_cwnd,
            prev_report_time: Instant::now(),
            start_timestep: Instant::now(),
            pre_bytes_sent: 0,
            counts: 0,
        };

        // 异步连接到智能体
        if let Ok(client) = flow
            .runtime
            .clone()
            .block_on(Self::connect_async(rl_server_addr))
        {
            flow.client = Some(Arc::new(client));
        } else {
            warn!("Failed to connect to RL agent, will use fallback logic");
        }

        flow
    }
}

impl Flow for DtccFlow {
    fn on_report(&mut self, sock_id: u32, m: Report) {
        self.counts += 1;

        debug!(sock_id = sock_id, counts = self.counts, "Processing report");

        if let Some(client) = &self.client {
            let client_clone = client.clone();

            // 准备观察数据
            let bytes_acked = m.get_field("Report.bytes_acked").unwrap_or(0);
            let loss = m.get_field("Report.loss").unwrap_or(0);
            let rtt = m.get_field("Report.rtt").unwrap_or(100000);
            let rttvar = m.get_field("Report.rttvar").unwrap_or(0);
            let castate = m.get_field("Report.castate").unwrap_or(0);
            let minrtt = m.get_field("Report.minrtt").unwrap_or(100000);
            let rate_delivery = m.get_field("Report.rate_delivery").unwrap_or(0);
            let packets_unacked = m.get_field("Report.packets_unacked").unwrap_or(0);
            let snd_mss = m.get_field("Report.snd_mss").unwrap_or(1460);
            let packets_delivered = m.get_field("Report.packets_delivered").unwrap_or(0);
            let bytes_sent = m.get_field("Report.bytes_sent").unwrap_or(0);

            // 计算增量数据
            let bytes_sent_delta = bytes_sent.saturating_sub(self.pre_bytes_sent);
            let time_delta = self.start_timestep.elapsed().as_micros() as u64;
            let duration = self.prev_report_time.elapsed().as_micros() as u64;

            // 异步调用智能体
            let runtime = self.runtime.clone();
            let cwnd = self.cwnd;

            let result = runtime.block_on(async move {
                // 创建请求
                let mut req = client_clone.get_action_request();
                let obs = req.get().init_observation();

                // 设置观察数据
                obs.set_bytes_acked(bytes_acked);
                obs.set_loss(loss);
                obs.set_rtt(rtt);
                obs.set_rttvar(rttvar);
                obs.set_castate(castate);
                obs.set_minrtt(minrtt);
                obs.set_delivery_rate(rate_delivery);
                obs.set_unacked(packets_unacked);
                obs.set_snd_mss(snd_mss);
                obs.set_delivered(packets_delivered);
                obs.set_bytes_sent(bytes_sent_delta);
                obs.set_snd_cwnd(cwnd as u64);
                obs.set_time_delta(time_delta);
                obs.set_duration(duration);

                // 发送请求并等待响应（带超时）
                tokio::time::timeout(Duration::from_millis(50), req.send()).await
            });

            // 处理响应
            match result {
                Ok(Ok(resp)) => {
                    if let Ok(action) = resp.get().and_then(|r| r.get_action()) {
                        let new_cwnd = action.get_cwnd();
                        if new_cwnd > 0 {
                            self.cwnd = new_cwnd as u32;
                            let cwnd_bytes = self.cwnd * self.info.mss;

                            info!(
                                sock_id = sock_id,
                                cwnd_packets = self.cwnd,
                                cwnd_bytes = cwnd_bytes,
                                rtt = rtt,
                                loss = loss,
                                "Updated congestion window from RL agent"
                            );

                            debug!(
                                sock_id = sock_id,
                                cwnd_bytes = cwnd_bytes,
                                "Would update datapath cwnd"
                            );

                            // 计算发送速率
                            if minrtt > 0 {
                                let rate = (cwnd_bytes as u64 * 2 * 1_000_000) / minrtt;
                                debug!(sock_id = sock_id, rate = rate, "Would update sending rate");
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("RL agent returned error: {}", e);
                    self.apply_fallback_logic(sock_id, rtt, loss, minrtt);
                }
                Err(_) => {
                    warn!("RL agent request timed out");
                    self.apply_fallback_logic(sock_id, rtt, loss, minrtt);
                }
            }

            // 更新状态
            self.pre_bytes_sent = bytes_sent;
            self.prev_report_time = Instant::now();
            self.start_timestep = Instant::now();
        } else {
            // 没有连接到智能体，使用回退逻辑
            let rtt = m.get_field("Report.rtt").unwrap_or(100000);
            let loss = m.get_field("Report.loss").unwrap_or(0);
            let minrtt = m.get_field("Report.minrtt").unwrap_or(100000);

            self.apply_fallback_logic(sock_id, rtt, loss, minrtt);
        }
    }
}

impl DtccFlow {
    /// 应用回退控制逻辑（当智能体不可用时）
    fn apply_fallback_logic(&mut self, sock_id: u32, rtt: u64, loss: u64, minrtt: u64) {
        // 简单的AIMD逻辑
        if loss > 0 {
            self.cwnd = ((self.cwnd as f64) * 0.7).max(1.0) as u32;
        } else if rtt > minrtt * 2 {
            self.cwnd = ((self.cwnd as f64) * 0.9).max(1.0) as u32;
        } else {
            self.cwnd += 1;
        }

        let cwnd_bytes = self.cwnd * self.info.mss;

        debug!(
            sock_id = sock_id,
            cwnd_packets = self.cwnd,
            cwnd_bytes = cwnd_bytes,
            "Applied fallback control logic"
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting DTCC compatibility example");

    // 使用兼容性运行时
    let mut runtime = PortusCompatRuntime::new().await?;

    // 注册DTCC算法
    let dtcc_algorithm = Dtcc::default();
    runtime.register_algorithm(dtcc_algorithm).await?;

    // Get access to the underlying Lotus runtime
    let lotus_runtime = runtime.lotus_runtime();

    let datapath_info = lotus::algorithm::DatapathInfo {
        sock_id: 1,
        init_cwnd: 10,
        mss: 1460,
        src_ip: 0x7F000001,
        src_port: 8080,
        dst_ip: 0x7F000001,
        dst_port: 80,
        programs: std::collections::HashMap::new(),
        scopes: std::collections::HashMap::new(),
        report_fields: std::collections::HashMap::new(),
    };

    match lotus_runtime.create_flow(1, "DTCC", datapath_info).await {
        Ok(flow_id) => {
            info!(flow_id = %flow_id, "Created DTCC flow");

            // 模拟报告处理
            for i in 0..15u64 {
                let mut report = lotus::algorithm::Report {
                    fields: HashMap::new(),
                    timestamp: Instant::now(),
                };

                // 模拟网络状况
                let rtt = 50000 + (i % 3) * 20000;
                let loss: u64 = if i == 8 { 1 } else { 0 };

                report.set_field("Report.bytes_acked".to_string(), 1460 * (i + 1));
                report.set_field("Report.loss".to_string(), loss);
                report.set_field("Report.rtt".to_string(), rtt);
                report.set_field("Report.rttvar".to_string(), 5000);
                report.set_field("Report.castate".to_string(), 0);
                report.set_field("Report.minrtt".to_string(), 50000);
                report.set_field("Report.rate_delivery".to_string(), 1000000);
                report.set_field("Report.packets_unacked".to_string(), 5);
                report.set_field("Report.snd_mss".to_string(), 1460);
                report.set_field("Report.packets_delivered".to_string(), i + 1);
                report.set_field("Report.bytes_sent".to_string(), 1460 * (i + 1) + 50);

                if let Err(e) = lotus_runtime.handle_report(flow_id, 1, report).await {
                    warn!(error = %e, "Failed to handle report");
                }

                tokio::time::sleep(Duration::from_millis(30)).await;
            }

            // 关闭流
            if let Err(e) = lotus_runtime.close_flow(flow_id).await {
                warn!(error = %e, "Failed to close flow");
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to create DTCC flow");
        }
    }

    info!("DTCC compatibility example completed");
    Ok(())
}
